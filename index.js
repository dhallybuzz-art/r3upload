const express = require('express');
const axios = require('axios');
const { exec } = require('child_process');
const { S3Client, HeadObjectCommand, GetObjectCommand } = require('@aws-sdk/client-s3');
const { getSignedUrl } = require('@aws-sdk/s3-request-presigner');

const app = express();
const PORT = process.env.PORT || 3000;

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

const MAX_CONCURRENT_UPLOADS = 2; 
let runningUploads = 0;           
const uploadQueue = [];           
const activeUploads = new Set();  
const failedFiles = new Set();    
let cachedAccessToken = null;

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
        if (res) res.status(500).json({ status: "error", message: "Auth Failed" });
        return null;
    }
};

const processQueue = async () => {
    if (runningUploads >= MAX_CONCURRENT_UPLOADS || uploadQueue.length === 0) return;

    const task = uploadQueue.shift(); 
    runningUploads++;
    const { fileId, fileName, r2Key } = task;
    console.log(`[Queue] Transferring: ${fileName}`);
    
    try {
        const token = cachedAccessToken || await getAccessToken();
        
        // এন্ডপয়েন্ট থেকে https:// এবং ডোমেইন অংশ পরিষ্কার করা (Error Fix)
        const pureAccountId = process.env.R2_ACCOUNT_ID.replace(/^https?:\/\//, '').split('.')[0];
        const r2Endpoint = `${pureAccountId}.r2.cloudflarestorage.com`;

        // Connection String এ সরাসরি প্যারামিটার পাস করা (No rclone.conf needed)
        const rcloneRemote = `:s3,provider=Cloudflare,endpoint="${r2Endpoint}",access_key_id="${process.env.R2_ACCESS_KEY}",secret_access_key="${process.env.R2_SECRET_KEY}",region=auto,v2_auth=false`;

        const rcloneCmd = `rclone copyurl "https://www.googleapis.com/drive/v3/files/${fileId}?alt=media&acknowledgeAbuse=true" \
        "${rcloneRemote}:${process.env.R2_BUCKET_NAME}/${r2Key}" \
        --header "Authorization: Bearer ${token}" \
        --s3-no-check-bucket \
        --ignore-errors \
        -v`;

        exec(rcloneCmd, (error, stdout, stderr) => {
            activeUploads.delete(fileId);
            runningUploads--;
            if (error) {
                console.error(`[Rclone Error]:`, stderr);
                failedFiles.add(fileId);
            } else {
                console.log(`[Success]: ${fileName}`);
            }
            setTimeout(processQueue, 1500);
        });

    } catch (err) {
        activeUploads.delete(fileId);
        runningUploads--;
        setTimeout(processQueue, 1500);
    }
};

app.get('/favicon.ico', (req, res) => res.status(204).end());
app.get('/', (req, res) => res.send("Worker is active."));

app.get('/:fileId', async (req, res) => {
    const fileId = req.params.fileId.trim();
    if (!fileId || fileId.length < 15) return res.status(400).json({ status: "error", message: "Invalid ID" });
    if (failedFiles.has(fileId)) return res.status(410).json({ status: "error", message: "Download failed." });

    try {
        const token = await getAccessToken(res);
        const metaRes = await axios.get(`https://www.googleapis.com/drive/v3/files/${fileId}?fields=name`, {
            headers: { Authorization: `Bearer ${token}` }
        });
        const fileName = metaRes.data.name;

        try {
            await s3Client.send(new HeadObjectCommand({ Bucket: process.env.R2_BUCKET_NAME, Key: fileName }));
            return res.json({ status: "success", url: `${process.env.R2_PUBLIC_DOMAIN}/${encodeURIComponent(fileName)}` });
        } catch (e) {
            if (!activeUploads.has(fileId)) {
                activeUploads.add(fileId);
                uploadQueue.push({ fileId, fileName, r2Key: fileName });
                processQueue(); 
            }
            res.json({ status: "processing", filename: fileName });
        }
    } catch (error) {
        res.status(500).json({ status: "error", message: "API Error" });
    }
});

app.listen(PORT, () => console.log(`Worker active on ${PORT}`));
