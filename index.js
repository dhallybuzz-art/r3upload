const express = require('express');
const axios = require('axios');
const { S3Client, HeadObjectCommand, GetObjectCommand } = require('@aws-sdk/client-s3');
const { Upload } = require('@aws-sdk/lib-storage');
const { getSignedUrl } = require('@aws-sdk/s3-request-presigner');
const stream = require('stream');

const app = express();
const PORT = process.env.PORT || 3000;

// --- ক্রেডেনশিয়ালস (নিরাপত্তার জন্য এগুলো Heroku Config Vars এ রাখা ভালো) ---
const GD_CONFIG = {
    clientId: "328071675996-ac8efr6hk3ijrhovedkqvdaugo7pk2p2.apps.googleusercontent.com",
    clientSecret: "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",
    refreshToken: "1//04riw9Rs5hg0bCgYIARAAGAQSNwF-L9IrLTHOAYXCdvtUiTLzJI3KkpAwmlrwycOB_YlkPJIdnG_AEe7Gw0ZAAwLa3xyp7zZ1RPk"
};

const s3Client = new S3Client({
    region: "auto",
    endpoint: `https://${process.env.R2_ACCOUNT_ID}.r2.cloudflarestorage.com`,
    credentials: {
        accessKeyId: process.env.R2_ACCESS_KEY,
        secretAccessKey: process.env.R2_SECRET_KEY,
    },
});

const MAX_CONCURRENT_UPLOADS = 5; 
let runningUploads = 0;           
const uploadQueue = [];           
const activeUploads = new Set();  
const failedFiles = new Set();    
let cachedAccessToken = null;

// --- ১ ঘণ্টা পর পর ট্র্যাকার পরিষ্কার করা ---
setInterval(() => {
    failedFiles.clear();
    activeUploads.clear();
    console.log("[System] Tracker cleared.");
}, 60 * 60 * 1000); 

// --- Google Access Token পাওয়ার ফাংশন ---
const getAccessToken = async () => {
    try {
        const res = await axios.post('https://oauth2.googleapis.com/token', {
            client_id: GD_CONFIG.clientId,
            client_secret: GD_CONFIG.clientSecret,
            refresh_token: GD_CONFIG.refreshToken,
            grant_type: 'refresh_token'
        });
        cachedAccessToken = res.data.access_token;
        return cachedAccessToken;
    } catch (error) {
        console.error("[Auth Error] Refresh token failed:", error.response?.data || error.message);
        return null;
    }
};

const generatePresignedUrl = async (bucketName, key) => {
    try {
        const command = new GetObjectCommand({ Bucket: bucketName, Key: key });
        return await getSignedUrl(s3Client, command, { expiresIn: 3600 });
    } catch (error) { 
        return null; 
    }
};

const processQueue = async () => {
    if (runningUploads >= MAX_CONCURRENT_UPLOADS || uploadQueue.length === 0) return;

    const task = uploadQueue.shift(); 
    runningUploads++;
    
    const { fileId, fileName, r2Key } = task;
    console.log(`[Upload] Starting: ${fileName}`);
    
    try {
        const token = cachedAccessToken || await getAccessToken();
        const response = await axios({ 
            method: 'get', 
            url: `https://www.googleapis.com/drive/v3/files/${fileId}?alt=media`, 
            headers: { Authorization: `Bearer ${token}` },
            responseType: 'stream', 
            timeout: 0 
        });

        const upload = new Upload({
            client: s3Client,
            params: {
                Bucket: process.env.R2_BUCKET_NAME,
                Key: r2Key,
                Body: response.data.pipe(new stream.PassThrough()),
                ContentType: response.headers['content-type'] || 'application/octet-stream',
                ContentDisposition: `attachment; filename="${fileName}"`
            },
            queueSize: 3, 
            partSize: 10 * 1024 * 1024 // 10MB chunks
        });

        await upload.done();
        console.log(`[Success] ${fileName}`);
        activeUploads.delete(fileId);
    } catch (err) {
        console.error(`[Upload Failed] ID: ${fileId} - Error:`, err.message);
        failedFiles.add(fileId);
        activeUploads.delete(fileId);
    } finally {
        runningUploads--;
        setTimeout(processQueue, 500);
    }
};

// --- Favicon handling (To avoid 400 errors) ---
app.get('/favicon.ico', (req, res) => res.status(204).end());

// --- Root check ---
app.get('/', (req, res) => res.send("R2 Bridge API is running..."));

// --- Main File Handler ---
app.get('/:fileId', async (req, res) => {
    const fileId = req.params.fileId;

    if (!fileId || fileId.length < 15) {
        return res.status(400).json({ status: "error", message: "Invalid File ID" });
    }

    if (failedFiles.has(fileId)) {
        return res.status(410).json({ status: "error", message: "File download failed on previous attempt." });
    }

    try {
        const token = cachedAccessToken || await getAccessToken();
        if (!token) throw new Error("Could not retrieve Google Access Token");
        
        // ১. মেটাডাটা উদ্ধার (ফাইল আছে কি না চেক)
        let metaRes;
        try {
            metaRes = await axios.get(`https://www.googleapis.com/drive/v3/files/${fileId}?fields=name,size,mimeType`, {
                headers: { Authorization: `Bearer ${token}` }
            });
        } catch (gErr) {
            console.error(`[Google API Error] ID: ${fileId}`, gErr.response?.data || gErr.message);
            return res.status(404).json({ status: "error", message: "File not found on Google Drive or no permission." });
        }

        const fileName = metaRes.data.name;
        const r2Key = fileName;

        // ২. R2 চেক
        try {
            const headData = await s3Client.send(new HeadObjectCommand({
                Bucket: process.env.R2_BUCKET_NAME,
                Key: r2Key
            }));
            
            const presignedUrl = await generatePresignedUrl(process.env.R2_BUCKET_NAME, r2Key);
            const publicUrl = `${process.env.R2_PUBLIC_DOMAIN}/${encodeURIComponent(r2Key)}`;
            
            return res.json({
                status: "success",
                filename: fileName,
                size: headData.ContentLength,
                url: publicUrl,
                presigned_url: presignedUrl
            });
        } catch (e) { 
            // ফাইল R2 তে নেই, আপলোড শুরু হবে
            console.log(`[Queue] File not in R2, adding to queue: ${fileName}`);
        }

        // ৩. কিউতে যুক্ত করা
        if (!activeUploads.has(fileId)) {
            activeUploads.add(fileId);
            uploadQueue.push({ fileId, fileName, r2Key });
            processQueue(); 
        }

        res.json({
            status: "processing",
            filename: fileName,
            message: "File is being uploaded to R2. Please check back in a few minutes.",
            queue_length: uploadQueue.length
        });

    } catch (error) {
        console.error("[System Error]", error.message);
        res.status(500).json({ status: "error", message: "Internal Server Error" });
    }
});

app.listen(PORT, () => console.log(`Server running on port ${PORT}`));
