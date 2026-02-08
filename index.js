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

// --- ট্র্যাকার এবং কিউ ---
const activeUploads = new Set();  
const failedFiles = new Set();    
let cachedAccessToken = null;

// ১ ঘণ্টা পর পর ট্র্যাকার পরিষ্কার
setInterval(() => {
    failedFiles.clear();
    activeUploads.clear();
    console.log("[System] Stats reset.");
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
        console.error("[Auth Error] Token failed");
        return null;
    }
};

const generatePresignedUrl = async (bucketName, key) => {
    try {
        const command = new GetObjectCommand({ Bucket: bucketName, Key: key });
        return await getSignedUrl(s3Client, command, { expiresIn: 3600 });
    } catch (error) { return null; }
};

// --- rclone আপলোড ফাংশন (যেটা কিউ এবং স্ট্রিম হ্যান্ডেল করবে) ---
const runRcloneUpload = async (fileId, fileName, r2Key, token) => {
    if (activeUploads.has(fileId)) return;
    activeUploads.add(fileId);

    console.log(`[rclone] Started: ${fileName}`);

    // rclone সরাসরি কপি করবে, যা আপনার কোডের Manual Upload-এর বিকল্প
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
        if (error) {
            console.error(`[rclone Error] ${fileName}:`, stderr);
            failedFiles.add(fileId);
            return;
        }
        console.log(`[rclone Success] ${fileName}`);
    });
};

// --- Routes ---
app.get('/favicon.ico', (req, res) => res.status(204).end());
app.get('/', (req, res) => res.send("R2 Bridge with rclone is Running."));

app.get('/:fileId', async (req, res) => {
    const fileId = req.params.fileId;
    if (!fileId || fileId.length < 15) return res.status(400).json({ status: "error", message: "Invalid ID" });

    // আগে ফেইল হয়েছে কিনা চেক
    if (failedFiles.has(fileId)) {
        return res.status(410).json({ status: "error", message: "Download failed previously." });
    }

    try {
        const token = await getAccessToken();
        
        // ১. মেটাডাটা উদ্ধার
        const metaRes = await axios.get(`https://www.googleapis.com/drive/v3/files/${fileId}?fields=name,size`, {
            headers: { Authorization: `Bearer ${token}` }
        });

        const fileName = metaRes.data.name;
        const r2Key = fileName;

        // ২. R2-তে ফাইল আছে কিনা চেক
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
            // ফাইল নেই, ব্যাকগ্রাউন্ডে rclone শুরু হবে
            if (!activeUploads.has(fileId)) {
                runRcloneUpload(fileId, fileName, r2Key, token);
            }
        }

        res.json({
            status: "processing",
            filename: fileName,
            message: "Transfer started via rclone. Check back in a few minutes.",
            is_active: activeUploads.has(fileId)
        });

    } catch (error) {
        console.error(`[API Error]`, error.message);
        res.status(500).json({ status: "error", message: "Google API Error" });
    }
});

app.listen(PORT, () => console.log(`Server running on port ${PORT}`));
