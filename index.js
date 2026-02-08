const express = require('express');
const axios = require('axios');
const { exec } = require('child_process');
const { S3Client, HeadObjectCommand, GetObjectCommand } = require('@aws-sdk/client-s3');
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

// --- স্টেট ম্যানেজমেন্ট এবং কিউ ---
const MAX_CONCURRENT_UPLOADS = 2; 
let runningUploads = 0;           
const uploadQueue = [];           
const activeUploads = new Set();  
const failedFiles = new Set();    
let cachedAccessToken = null;

setInterval(() => {
    failedFiles.clear();
    activeUploads.clear();
    console.log("[System] Stats reset to maintain performance.");
}, 60 * 60 * 1000); 

const getAccessToken = async (res = null) => {
    try {
        const response = await axios.post('https://oauth2.googleapis.com/token', {
            client_id: GD_CONFIG.clientId,
            client_secret: GD_CONFIG.clientSecret,
            refresh_token: GD_CONFIG.refreshToken,
            grant_type: 'refresh_token'
        });
        cachedAccessToken = response.data.access_token;
        return cachedAccessToken;
    } catch (error) {
        console.error(`[Google Auth Error]`);
        if (res) res.status(500).json({ status: "error", message: "Auth Failed" });
        return null;
    }
};

const processQueue = async () => {
    if (runningUploads >= MAX_CONCURRENT_UPLOADS || uploadQueue.length === 0) return;

    const task = uploadQueue.shift(); 
    runningUploads++;
    const { fileId, fileName, r2Key } = task;
    console.log(`[System] Final Attempt Initiated for: ${fileName}`);
    
    try {
        const token = cachedAccessToken || await getAccessToken();
        
        // এন্ডপয়েন্ট থেকে https:// সরিয়ে দেওয়া হয়েছে যাতে ডাবল না হয়
        const r2Endpoint = `${process.env.R2_ACCOUNT_ID}.r2.cloudflarestorage.com`;

        const rcloneCmd = `rclone copyurl "https://www.googleapis.com/drive/v3/files/${fileId}?alt=media&acknowledgeAbuse=true" \
        :s3,provider=Cloudflare,endpoint="${r2Endpoint}",access_key_id="${process.env.R2_ACCESS_KEY}",secret_access_key="${process.env.R2_SECRET_KEY}",region=auto,v2_auth=false:"${process.env.R2_BUCKET_NAME}/${r2Key}" \
        --header "Authorization: Bearer ${token}" \
        --s3-no-check-bucket \
        --ignore-errors \
        -v`;

        exec(rcloneCmd, (error, stdout, stderr) => {
            activeUploads.delete(fileId);
            runningUploads--;
            
            if (error) {
                console.error(`[Fatal Error] Detailed Log:`, stderr);
                failedFiles.add(fileId);
            } else {
                console.log(`[Success] Transfer Completed: ${fileName}`);
            }
            setTimeout(processQueue, 1500);
        });

    } catch (err) {
        console.error(`[Critical Error] ${err.message}`);
        activeUploads.delete(fileId);
        runningUploads--;
        setTimeout(processQueue, 1500);
    }
};

app.get('/favicon.ico', (req, res) => res.status(204).end());
app.get('/', (req, res) => res.send("R2 Bridge API v2.0 (Powered by Rclone) is running."));

app.get('/:fileId', async (req, res) => {
    const fileId = req.params.fileId.trim();
    if (!fileId || fileId.length < 15) return res.status(400).json({ status: "error", message: "Invalid ID format" });

    if (failedFiles.has(fileId)) return res.status(410).json({ status: "error", message: "Download failed previously." });

    try {
        const token = await getAccessToken(res);
        if (!token) return; 

        const metaRes = await axios.get(`https://www.googleapis.com/drive/v3/files/${fileId}?fields=name,size`, {
            headers: { Authorization: `Bearer ${token}` }
        });
        const fileName = metaRes.data.name;

        try {
            await s3Client.send(new HeadObjectCommand({ Bucket: process.env.R2_BUCKET_NAME, Key: fileName }));
            const command = new GetObjectCommand({ Bucket: process.env.R2_BUCKET_NAME, Key: fileName });
            const presignedUrl = await getSignedUrl(s3Client, command, { expiresIn: 3600 });
            return res.json({
                status: "success",
                filename: fileName,
                size: metaRes.data.size,
                url: `${process.env.R2_PUBLIC_DOMAIN}/${encodeURIComponent(fileName)}`,
                presigned_url: presignedUrl
            });
        } catch (e) {
            console.log(`[Check] Missing in R2, adding to queue: ${fileName}`);
        }

        if (!activeUploads.has(fileId)) {
            activeUploads.add(fileId);
            uploadQueue.push({ fileId, fileName, r2Key: fileName });
            processQueue(); 
        }

        res.json({
            status: "processing",
            filename: fileName,
            message: "File is being transferred to R2 via Rclone AWS4 engine.",
            queue_position: uploadQueue.length,
            active_threads: runningUploads
        });

    } catch (error) {
        res.status(500).json({ status: "error", message: "Google API Error" });
    }
});

app.listen(PORT, () => console.log(`Worker active on port ${PORT}. Rclone engine is ready.`));
