const express = require('express');
const axios = require('axios');
const { S3Client, HeadObjectCommand, GetObjectCommand } = require('@aws-sdk/client-s3');
const { Upload } = require('@aws-sdk/lib-storage');
const { getSignedUrl } = require('@aws-sdk/s3-request-presigner');
const stream = require('stream');

const app = express();
const PORT = process.env.PORT || 3000;

const s3Client = new S3Client({
    region: "auto",
    endpoint: `https://${process.env.R2_ACCOUNT_ID}.r2.cloudflarestorage.com`,
    credentials: {
        accessKeyId: process.env.R2_ACCESS_KEY,
        secretAccessKey: process.env.R2_SECRET_KEY,
    },
});

// --- কনকারেন্সি এবং ট্র্যাকিং ভেরিয়েবল ---
const MAX_CONCURRENT_UPLOADS = 5; 
let runningUploads = 0;           
const uploadQueue = [];           
const activeUploads = new Set();  
const failedFiles = new Set();    // চিরস্থায়ীভাবে ব্যর্থ (যেমন 404) ফাইল ট্র্যাকার

app.get('/favicon.ico', (req, res) => res.status(204).end());

// --- ১ ঘণ্টা পর পর ট্র্যাকার পরিষ্কার করার ফাংশন ---
setInterval(() => {
    console.log("[Cleaner] Clearing failed and active trackers to free memory...");
    failedFiles.clear();
    activeUploads.clear();
}, 60 * 60 * 1000); 

// --- প্রিসাইন ইউআরএল তৈরি করার ফাংশন ---
const generatePresignedUrl = async (bucketName, key, expiresIn = 3600) => {
    try {
        const command = new GetObjectCommand({
            Bucket: bucketName,
            Key: key
        });
        
        const url = await getSignedUrl(s3Client, command, { expiresIn });
        return url;
    } catch (error) {
        console.error(`[Presigned URL Error] Failed to generate for ${key}:`, error.message);
        return null;
    }
};

// --- কিউ প্রসেস করার ফাংশন ---
const processQueue = async () => {
    if (runningUploads >= MAX_CONCURRENT_UPLOADS || uploadQueue.length === 0) {
        return;
    }

    const task = uploadQueue.shift(); 
    runningUploads++;
    
    const { fileId, fileName, r2Key, gDriveUrl } = task;
    console.log(`[Queue] Starting upload (${runningUploads}/${MAX_CONCURRENT_UPLOADS}): ${fileName}`);

    try {
        const response = await axios({ 
            method: 'get', 
            url: gDriveUrl, 
            responseType: 'stream', 
            timeout: 0 
        });

        const upload = new Upload({
            client: s3Client,
            params: {
                Bucket: process.env.R2_BUCKET_NAME,
                Key: r2Key,
                Body: response.data.pipe(new stream.PassThrough()),
                ContentType: response.headers['content-type'] || 'video/x-matroska',
                ContentDisposition: `attachment; filename="${fileName}"`
            },
            queueSize: 3, 
            partSize: 1024 * 1024 * 10 
        });

        await upload.done();
        console.log(`[Success] Finished: ${fileName}`);
    } catch (err) {
        console.error(`[Error] Upload failed for ${fileName}:`, err.message);
        
        // যদি ফাইলটি ড্রাইভে না থাকে (404), তবে লুপ ঠেকাতে ব্ল্যাকলিস্ট করা হবে
        if (err.response && err.response.status === 404) {
            console.log(`[Blacklist] Adding ${fileId} to failedFiles.`);
            failedFiles.add(fileId);
        } else {
            // অন্য কোনো সাময়িক এরর হলে কিউ থেকে সরালাম যেন পরে আবার ট্রাই করা যায়
            activeUploads.delete(fileId);
        }
    } finally {
        runningUploads--;
        setTimeout(() => {
            processQueue();
        }, 300);
    }
};

app.get('/:fileId', async (req, res) => {
    const fileId = req.params.fileId;
    if (!fileId || fileId.length < 15) return res.status(400).json({ status: "error", message: "Invalid ID" });

    // আগে ব্যর্থ হয়েছে এমন ফাইল হলে সরাসরি এরর
    if (failedFiles.has(fileId)) {
        return res.status(404).json({ status: "error", message: "File previously failed (404/Invalid)" });
    }

    try {
        let fileName = "";
        let fileSize = 0;

        // ১. নাম উদ্ধার (API এবং Scraping Method) 
        try {
            const metaUrl = `https://www.googleapis.com/drive/v3/files/${fileId}?fields=name,size&key=${process.env.GDRIVE_API_KEY}`;
            const metaResponse = await axios.get(metaUrl);
            fileName = metaResponse.data.name;
            fileSize = parseInt(metaResponse.data.size) || 0;
        } catch (e) {
            console.log(`Meta API failed for ${fileId}, trying Scraping...`);
            try {
                const previewUrl = `https://drive.google.com/file/d/${fileId}/view`;
                const response = await axios.get(previewUrl, { 
                    headers: { 'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)' } 
                });
                const match = response.data.match(/<title>(.*?) - Google Drive<\/title>/);
                if (match && match[1]) {
                    fileName = match[1].trim();
                }
            } catch (scrapErr) {
                console.error(`Scraping failed for ${fileId}`);
            }
        }

        const gDriveUrl = `https://www.googleapis.com/drive/v3/files/${fileId}?alt=media&key=${process.env.GDRIVE_API_KEY}`;

        // ২. বিকল্প Header চেক
        if (!fileName) {
            try {
                const headRes = await axios.head(gDriveUrl);
                const cd = headRes.headers['content-disposition'];
                if (cd && cd.includes('filename=')) {
                    fileName = cd.split('filename=')[1].replace(/['"]/g, '');
                }
            } catch (hErr) {}
        }

        if (!fileName) fileName = `Movie_${fileId}.mkv`;

        const r2Key = fileName;
        const r2PublicUrl = `${process.env.R2_PUBLIC_DOMAIN}/${encodeURIComponent(r2Key)}`;

        // ৩. R2 চেক 
        try {
            const headData = await s3Client.send(new HeadObjectCommand({
                Bucket: process.env.R2_BUCKET_NAME,
                Key: r2Key
            }));
            
            // প্রিসাইন ইউআরএল তৈরি
            const presignedUrl = await generatePresignedUrl(process.env.R2_BUCKET_NAME, r2Key);
            
            const responseObj = {
                status: "success",
                filename: fileName,
                size: headData.ContentLength || fileSize,
                url: r2PublicUrl
            };
            
            // যদি প্রিসাইন ইউআরএল তৈরি হয় তাহলে যুক্ত করবে
            if (presignedUrl) {
                responseObj.presigned_url = presignedUrl;
            }
            
            return res.json(responseObj);
        } catch (e) { /* ফাইল নেই */ }

        // ৪. কিউতে যুক্ত করা (ডুপ্লিকেট চেক)
        if (!activeUploads.has(fileId)) {
            activeUploads.add(fileId);
            uploadQueue.push({ fileId, fileName, r2Key, gDriveUrl });
            console.log(`[Queue] Added: ${fileName}`);
            processQueue(); 
        }

        const queuePos = uploadQueue.findIndex(f => f.fileId === fileId) + 1;
        
        res.json({
            status: "processing",
            filename: fileName,
            message: queuePos > 0 ? `In queue (Position: ${queuePos})` : "Uploading...",
            active_slots: `${runningUploads}/${MAX_CONCURRENT_UPLOADS}`,
            queue_length: uploadQueue.length
        });

    } catch (error) {
        res.json({ status: "error", message: "Server encountered an error." });
    }
});

app.listen(PORT, () => console.log(`Server running on port ${PORT}`));
