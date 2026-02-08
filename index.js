const express = require('express');
const axios = require('axios');
const { exec } = require('child_process');
const { S3Client, HeadObjectCommand, GetObjectCommand } = require('@aws-sdk/client-s3');
const { getSignedUrl } = require('@aws-sdk/s3-request-presigner');

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

const MAX_CONCURRENT_UPLOADS = 2; 
let runningUploads = 0;           
const uploadQueue = [];           
const activeUploads = new Set();  
const failedFiles = new Set();    
let cachedAccessToken = null;

setInterval(() => {
    failedFiles.clear();
    activeUploads.clear();
    console.log("[System] Stats reset.");
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
    
    try {
        const token = cachedAccessToken || await getAccessToken();
        
        // AWS4 Signature ফোর্স করার জন্য এনভায়রনমেন্ট সেটআপ
        const rcloneEnv = {
            ...process.env,
            RCLONE_S3_V2_AUTH: "false",
            RCLONE_S3_REGION: "auto",
            RCLONE_S3_PROVIDER: "Cloudflare",
            RCLONE_S3_ENV_AUTH: "false"
        };

        const rcloneCmd = `rclone copyurl "https://www.googleapis.com/drive/v3/files/${fileId}?alt=media&acknowledgeAbuse=true" \
        :s3:"${process.env.R2_BUCKET_NAME}/${r2Key}" \
        --header "Authorization: Bearer ${token}" \
        --s3-endpoint "https://${process.env.R2_ACCOUNT_ID}.r2.cloudflarestorage.com" \
        --s3-access-key-id "${process.env.R2_ACCESS_KEY}" \
        --s3-secret-access-key "${process.env.R2_SECRET_KEY}" \
        --s3-no-check-bucket -v`;

        exec(rcloneCmd, { env: rcloneEnv }, (error, stdout, stderr) => {
            activeUploads.delete(fileId);
            runningUploads--;
            console.log(error ? `[Rclone Error] ${fileName}` : `[Success] Finished: ${fileName}`);
            setTimeout(processQueue, 1500);
        });
    } catch (err) {
        activeUploads.delete(fileId);
        runningUploads--;
        setTimeout(processQueue, 1500);
    }
};

app.get('/favicon.ico', (req, res) => res.status(204).end());
app.get('/', (req, res) => res.send("R2 Bridge is ready."));

app.get('/:fileId', async (req, res) => {
    const fileId = req.params.fileId.trim();
    if (!fileId || fileId.length < 15) return res.status(400).json({ status: "error", message: "Invalid ID" });

    try {
        const token = await getAccessToken(res);
        const metaRes = await axios.get(`https://www.googleapis.com/drive/v3/files/${fileId}?fields=name`, {
            headers: { Authorization: `Bearer ${token}` }
        });
        const fileName = metaRes.data.name;

        try {
            await s3Client.send(new HeadObjectCommand({ Bucket: process.env.R2_BUCKET_NAME, Key: fileName }));
            const command = new GetObjectCommand({ Bucket: process.env.R2_BUCKET_NAME, Key: fileName });
            const presignedUrl = await getSignedUrl(s3Client, command, { expiresIn: 3600 });
            return res.json({ status: "success", filename: fileName, url: `${process.env.R2_PUBLIC_DOMAIN}/${encodeURIComponent(fileName)}`, presigned_url: presignedUrl });
        } catch (e) {
            if (!activeUploads.has(fileId)) {
                activeUploads.add(fileId);
                uploadQueue.push({ fileId, fileName, r2Key: fileName });
                processQueue();
            }
        }
        res.json({ status: "processing", filename: fileName, message: "Transfer started via Rclone AWS4 Engine." });
    } catch (error) {
        res.status(500).json({ status: "error", message: "Google API Error" });
    }
});

app.listen(PORT, () => console.log(`Worker active on ${PORT}`));
