const express = require('express');
const axios = require('axios');
const { exec } = require('child_process');
const { S3Client, HeadObjectCommand, GetObjectCommand } = require('@aws-sdk/client-s3');
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

// --- স্টেট ম্যানেজমেন্ট ও কিউ ---
const MAX_CONCURRENT_UPLOADS = 2; 
let runningUploads = 0;           
const uploadQueue = [];           
const activeUploads = new Set();  
const failedFiles = new Set();    
let cachedAccessToken = null;

// --- অটোমেটিক ক্লিনার ---
setInterval(() => {
    failedFiles.clear();
    activeUploads.clear();
    console.log("[System] Stats reset to maintain performance.");
}, 60 * 60 * 1000); 

// --- Google Access Token ফাংশন ---
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
        console.error("[Auth Error] Token refresh failed.");
        return null;
    }
};

const generatePresignedUrl = async (bucketName, key) => {
    try {
        const command = new GetObjectCommand({ Bucket: bucketName, Key: key });
        return await getSignedUrl(s3Client, command, { expiresIn: 3600 });
    } catch (error) { return null; }
};

// --- Rclone Engine (The Powerhouse) ---
const processQueue = async () => {
    if (runningUploads >= MAX_CONCURRENT_UPLOADS || uploadQueue.length === 0) return;

    const task = uploadQueue.shift(); 
    runningUploads++;
    
    const { fileId, fileName, r2Key } = task;
    console.log(`[Queue] Starting Rclone for: ${fileName}`);
    
    try {
        const token = cachedAccessToken || await getAccessToken();
        
        // rclone কমান্ড যা সরাসরি Google থেকে R2 তে ডাটা ট্রান্সফার করবে
        // --drive-acknowledge-abuse মুভি ফাইলের ৪MD এরর ঠেকাবে
        const rcloneCmd = `rclone copyurl "https://www.googleapis.com/drive/v3/files/${fileId}?alt=media&acknowledgeAbuse=true" \
        :s3:"${process.env.R2_BUCKET_NAME}/${r2Key}" \
        --header "Authorization: Bearer ${token}" \
        --s3-endpoint "https://${process.env.R2_ACCOUNT_ID}.r2.cloudflarestorage.com" \
        --s3-access-key-id "${process.env.R2_ACCESS_KEY}" \
        --s3-secret-access-key "${process.env.R2_SECRET_KEY}" \
        --s3-no-check-bucket \
        --ignore-errors \
        -v`;

        exec(rcloneCmd, (error, stdout, stderr) => {
            activeUploads.delete(fileId);
            runningUploads--;
            
            if (error) {
                console.error(`[Rclone Error] ${fileName}:`, stderr);
                failedFiles.add(fileId);
            } else {
                console.log(`[Success] Finished: ${fileName}`);
            }
            
            // পরবর্তী টাস্ক প্রসেস করা
            setTimeout(processQueue, 1500);
        });

    } catch (err) {
        console.error(`[Internal Error] ID: ${fileId}`, err.message);
        activeUploads.delete(fileId);
        runningUploads--;
        setTimeout(processQueue, 1500);
    }
};

// --- Routes ---
app.get('/favicon.ico', (req, res) => res.status(204).end());
app.get('/', (req, res) => res.send("R2 Bridge v2.0 (Rclone Powered) is running."));

app.get('/:fileId', async (req, res) => {
    const fileId = req.params.fileId;
    if (!fileId || fileId.length < 15) return res.status(400).json({ status: "error", message: "Invalid ID" });

    if (failedFiles.has(fileId)) {
        return res.status(410).json({ status: "error", message: "Download failed previously. Possible GDrive Quota Exceeded." });
    }

    try {
        const token = cachedAccessToken || await getAccessToken();
        
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
            // ফাইল R2 তে নেই
            console.log(`[Check] Not in R2: ${fileName}`);
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
            message: "File is being transferred to R2 via Rclone Engine.",
            queue_position: uploadQueue.length,
            active_threads: runningUploads
        });

    } catch (error) {
        console.error(`[API Error]`, error.message);
        res.status(500).json({ status: "error", message: "Google API Error or Restricted File" });
    }
});

app.listen(PORT, () => console.log(`Server running on port ${PORT}`));
