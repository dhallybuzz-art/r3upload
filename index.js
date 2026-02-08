const express = require('express');
const axios = require('axios');
const { S3Client, HeadObjectCommand, GetObjectCommand } = require('@aws-sdk/client-s3');
const { Upload } = require('@aws-sdk/lib-storage'); 
const { getSignedUrl } = require('@aws-sdk/s3-request-presigner');

const app = express();
const PORT = process.env.PORT || 3000;

// --- ক্রেডেনশিয়ালস (Google Drive API) ---
const GD_CONFIG = {
    clientId: "328071675996-ac8efr6hk3ijrhovedkqvdaugo7pk2p2.apps.googleusercontent.com",
    clientSecret: "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",
    refreshToken: "1//043JlgFIn7kngCgYIARAAGAQSNwF-L9IrJgqUS2zMSD82DBfBUJNEEvBIJcq8ZA9dGaKLFMP-xO4079ausTkUmZUQBRxuZRHZafQ"
};

// --- Cloudflare R2 Client কনফিগারেশন ---
const s3Client = new S3Client({
    region: "auto",
    endpoint: `https://${process.env.R2_ACCOUNT_ID}.r2.cloudflarestorage.com`,
    credentials: {
        accessKeyId: process.env.R2_ACCESS_KEY,
        secretAccessKey: process.env.R2_SECRET_KEY,
    },
});

const activeUploads = new Set();

// Google Access Token পাওয়ার ফাংশন
const getAccessToken = async () => {
    try {
        const res = await axios.post('https://oauth2.googleapis.com/token', {
            client_id: GD_CONFIG.clientId,
            client_secret: GD_CONFIG.clientSecret,
            refresh_token: GD_CONFIG.refreshToken,
            grant_type: 'refresh_token'
        });
        return res.data.access_token;
    } catch (error) {
        console.error("[Auth Error] Failed to refresh token.");
        return null;
    }
};

// সরাসরি গুগল থেকে ডাটা নিয়ে R2-তে স্ট্রিমিং করার ফাংশন
const startDirectTransfer = async (fileId, fileName) => {
    if (activeUploads.has(fileId)) return;
    activeUploads.add(fileId);

    try {
        const token = await getAccessToken();
        if (!token) throw new Error("Auth Token not available");

        // ১. গুগল ড্রাইভ থেকে ডাটা স্ট্রিম আনা
        const response = await axios({
            method: 'get',
            url: `https://www.googleapis.com/drive/v3/files/${fileId}?alt=media&acknowledgeAbuse=true`,
            headers: { Authorization: `Bearer ${token}` },
            responseType: 'stream'
        });

        // ২. AWS SDK দিয়ে সরাসরি R2 তে মাল্টিপার্ট আপলোড
        const parallelUploads3 = new Upload({
            client: s3Client,
            params: {
                Bucket: process.env.R2_BUCKET_NAME,
                Key: fileName,
                Body: response.data,
            },
            queueSize: 4, 
            partSize: 10 * 1024 * 1024, // 10MB চাঙ্ক
            leavePartsOnError: false,
        });

        console.log(`[Stream Start] Transferring: ${fileName}`);
        await parallelUploads3.done();
        console.log(`[Stream Success] Uploaded to R2: ${fileName}`);
    } catch (err) {
        console.error(`[Stream Error] ${fileName}:`, err.message);
    } finally {
        activeUploads.delete(fileId);
    }
};

app.get('/favicon.ico', (req, res) => res.status(204).end());
app.get('/', (req, res) => res.send("Direct R2 Bridge Engine is running."));

app.get('/:fileId', async (req, res) => {
    const fileId = req.params.fileId.trim();
    if (!fileId || fileId.length < 15) return res.status(400).json({ status: "error", message: "Invalid ID" });

    try {
        const token = await getAccessToken();
        if (!token) return res.status(500).json({ status: "error", message: "Google Auth Failed" });

        // ফাইল মেটাডাটা (নাম এবং সাইজ) উদ্ধার
        const meta = await axios.get(`https://www.googleapis.com/drive/v3/files/${fileId}?fields=name,size`, {
            headers: { Authorization: `Bearer ${token}` }
        });
        
        const fileName = meta.data.name;
        const fileSize = meta.data.size;

        try {
            // ১. বাকেটে ফাইল আছে কি না চেক করা
            const headData = await s3Client.send(new HeadObjectCommand({ 
                Bucket: process.env.R2_BUCKET_NAME, 
                Key: fileName 
            }));
            
            // ২. Presigned URL তৈরি করা (১ ঘণ্টা মেয়াদী)
            const presignedUrl = await getSignedUrl(
                s3Client, 
                new GetObjectCommand({ Bucket: process.env.R2_BUCKET_NAME, Key: fileName }), 
                { expiresIn: 3600 }
            );
            
            // ৩. পাবলিক ইউআরএল কনস্ট্রাকশন
            const publicDomain = (process.env.R2_PUBLIC_DOMAIN || '').replace(/\/$/, '');
            const publicUrl = `${publicDomain}/${encodeURIComponent(fileName)}`;
            
            return res.json({ 
                status: "success", 
                filename: fileName,
                size: headData.ContentLength || fileSize,
                url: publicUrl, 
                presigned_url: presignedUrl 
            });

        } catch (e) {
            // ফাইল R2-তে না থাকলে ট্রান্সফার শুরু করা
            startDirectTransfer(fileId, fileName);
            
            res.json({ 
                status: "processing", 
                filename: fileName,
                size: fileSize,
                message: "Streaming data directly to R2. Please wait and refresh." 
            });
        }
    } catch (err) {
        res.status(err.response?.status || 500).json({ 
            status: "error", 
            message: err.response?.data?.error?.message || err.message 
        });
    }
});

app.listen(PORT, () => console.log(`Engine active on port ${PORT}`));
