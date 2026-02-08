const express = require('express');
const axios = require('axios');
const { S3Client, HeadObjectCommand, GetObjectCommand } = require('@aws-sdk/client-s3');
const { Upload } = require('@aws-sdk/lib-storage');
const { getSignedUrl } = require('@aws-sdk/s3-request-presigner');
const stream = require('stream');

const app = express();
const PORT = process.env.PORT || 3000;

// --- ক্রেডেনশিয়ালস ---
const GD_CONFIG = {
    clientId: "328071675996-ac8efr6hk3ijrhovedkqvdaugo7pk2p2.apps.googleusercontent.com",
    clientSecret: "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",
    refreshToken: "1//043JlgFIn7kngCgYIARAAGAQSNwF-L9IrJgqUS2zMSD82DBfBUJNEEvBIJcq8ZA9dGaKLFMP-xO4079ausTkUmZUQBRxuZRHZafQ"
};

const s3Client = new S3Client({
    region: "auto",
    endpoint: `https://${process.env.R2_ACCOUNT_ID}.r2.cloudflarestorage.com`,
    credentials: {
        accessKeyId: process.env.R2_ACCESS_KEY,
        secretAccessKey: process.env.R2_SECRET_KEY,
    },
});

const MAX_CONCURRENT_UPLOADS = 2; // মেমরি এবং কানেকশন স্ট্যাবিলিটির জন্য ২ করা হলো
let runningUploads = 0;           
const uploadQueue = [];           
const activeUploads = new Set();  
const failedFiles = new Set();    
let cachedAccessToken = null;

// --- ট্র্যাকার পরিষ্কার করা ---
setInterval(() => {
    failedFiles.clear();
    activeUploads.clear();
    console.log("[System] Stats tracker reset.");
}, 60 * 60 * 1000); 

// --- Google Access Token পাওয়ার ফাংশন ---
const getAccessToken = async (force = false) => {
    if (cachedAccessToken && !force) return cachedAccessToken;
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
        console.error("[Auth Error] Token failed:", error.response?.data || error.message);
        return null;
    }
};

const generatePresignedUrl = async (bucketName, key) => {
    try {
        const command = new GetObjectCommand({ Bucket: bucketName, Key: key });
        return await getSignedUrl(s3Client, command, { expiresIn: 3600 });
    } catch (error) { return null; }
};

const processQueue = async () => {
    if (runningUploads >= MAX_CONCURRENT_UPLOADS || uploadQueue.length === 0) return;

    const task = uploadQueue.shift(); 
    runningUploads++;
    
    const { fileId, fileName, r2Key } = task;
    console.log(`[Upload] Processing: ${fileName}`);
    
    try {
        const token = await getAccessToken();
        
        // ১. ডাউনলোড রিকোয়েস্ট (acknowledgeAbuse এবং নির্দিষ্ট হেডারসহ)
        const response = await axios({ 
            method: 'get', 
            url: `https://www.googleapis.com/drive/v3/files/${fileId}?alt=media&acknowledgeAbuse=true`, 
            headers: { 
                'Authorization': `Bearer ${token}`,
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36' 
            },
            responseType: 'stream', 
            timeout: 0 
        });

        // ২. R2 আপলোড স্ট্রিম
        const upload = new Upload({
            client: s3Client,
            params: {
                Bucket: process.env.R2_BUCKET_NAME,
                Key: r2Key,
                Body: response.data.pipe(new stream.PassThrough()),
                ContentType: 'application/octet-stream' // ভিডিও/জিপ সবকিছুর জন্য সেফ
            },
            queueSize: 4, 
            partSize: 15 * 1024 * 1024 // ১৫ মেগাবাইট চাঙ্ক
        });

        await upload.done();
        console.log(`[Success] Finished: ${fileName}`);
        activeUploads.delete(fileId);
    } catch (err) {
        // বিস্তারিত এরর ডিবাগিং
        const errorData = err.response?.data;
        console.error(`[Upload Failed] ID: ${fileId}`);
        if (err.response?.status === 403) {
            console.error("Reason: 403 Forbidden. Possible Quota Exceeded or Restricted File.");
        } else {
            console.error("Reason:", err.message);
        }
        failedFiles.add(fileId);
        activeUploads.delete(fileId);
    } finally {
        runningUploads--;
        setTimeout(processQueue, 2000); // ২ সেকেন্ড বিরতি
    }
};

// --- Routes ---
app.get('/favicon.ico', (req, res) => res.status(204).end());
app.get('/', (req, res) => res.send("R2 Bridge is Active."));

app.get('/:fileId', async (req, res) => {
    const fileId = req.params.fileId;

    if (!fileId || fileId.length < 15) {
        return res.status(400).json({ status: "error", message: "Invalid ID" });
    }

    try {
        const token = await getAccessToken();
        
        // ১. মেটাডাটা উদ্ধার
        const metaRes = await axios.get(`https://www.googleapis.com/drive/v3/files/${fileId}?fields=name,size`, {
            headers: { Authorization: `Bearer ${token}` }
        });

        const fileName = metaRes.data.name;
        const r2Key = fileName;

        // ২. R2 চেক
        try {
            const headData = await s3Client.send(new HeadObjectCommand({
                Bucket: process.env.R2_BUCKET_NAME,
                Key: r2Key
            }));
            
            const presignedUrl = await generatePresignedUrl(process.env.R2_BUCKET_NAME, r2Key);
            
            return res.json({
                status: "success",
                filename: fileName,
                size: headData.ContentLength,
                url: `${process.env.R2_PUBLIC_DOMAIN}/${encodeURIComponent(r2Key)}`,
                presigned_url: presignedUrl
            });
        } catch (e) {
            console.log(`[System] Not in R2: ${fileName}`);
        }

        // ৩. কিউ ম্যানেজমেন্ট
        if (failedFiles.has(fileId)) {
            return res.status(410).json({ status: "error", message: "File failed previously. Check Google Drive permissions." });
        }

        if (!activeUploads.has(fileId)) {
            activeUploads.add(fileId);
            uploadQueue.push({ fileId, fileName, r2Key });
            processQueue(); 
        }

        res.json({
            status: "processing",
            filename: fileName,
            message: "Upload started. Check back soon.",
            queue_pos: uploadQueue.length
        });

    } catch (error) {
        console.error(`[API Error]`, error.message);
        res.status(500).json({ status: "error", message: "File not accessible or Google API issue." });
    }
});

app.listen(PORT, () => console.log(`Active on port ${PORT}`));
