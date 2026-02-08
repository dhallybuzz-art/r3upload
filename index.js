const express = require('express');
const axios = require('axios');
const { exec } = require('child_process');
const { S3Client, HeadObjectCommand, GetObjectCommand } = require('@aws-sdk/client-s3');
const { getSignedUrl } = require('@aws-sdk/s3-request-presigner');
const stream = require('stream');

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

// --- স্টেট ম্যানেজমেন্ট, ট্র্যাকিং এবং কিউ ---
const MAX_CONCURRENT_UPLOADS = 2; // Heroku-র স্ট্যাবিলিটির জন্য ২ টি থ্রেড রাখা হয়েছে
let runningUploads = 0;           
const uploadQueue = [];           
const activeUploads = new Set();  
const failedFiles = new Set();    
let cachedAccessToken = null;

// --- ১ ঘণ্টা পর পর ট্র্যাকার পরিষ্কার করা (মেমোরি সেফ রাখতে) ---
setInterval(() => {
    failedFiles.clear();
    activeUploads.clear();
    console.log("[System] Stats reset to maintain performance.");
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
    // আগের Console.error এর বদলে এটি দিন
    const errorData = error.response ? error.response.data : error.message;
    console.error(`[Google API Error Detailed]:`, JSON.stringify(errorData));
    
    res.status(500).json({ 
        status: "error", 
        message: "Google API Error",
        detail: error.response?.data?.error?.message || error.message 
    });
}
};

// --- R2 Presigned URL জেনারেটর ---
const generatePresignedUrl = async (bucketName, key) => {
    try {
        const command = new GetObjectCommand({ Bucket: bucketName, Key: key });
        return await getSignedUrl(s3Client, command, { expiresIn: 3600 });
    } catch (error) { return null; }
};

// --- Main Engine: rclone Queue Processor ---
const processQueue = async () => {
    // যদি কিউ খালি থাকে বা রানিং লিমিট ফুল থাকে তবে থামুন
    if (runningUploads >= MAX_CONCURRENT_UPLOADS || uploadQueue.length === 0) return;

    const task = uploadQueue.shift(); 
    runningUploads++;
    
    const { fileId, fileName, r2Key } = task;
    console.log(`[Queue] Starting Rclone Transfer for: ${fileName}`);
    
    try {
        const token = cachedAccessToken || await getAccessToken();
        
        // rclone কমান্ড: সরাসরি Google Drive থেকে R2 তে ডাটা কপি করবে
        [cite_start]// --drive-acknowledge-abuse ফ্ল্যাগটি ৪MD (Forbidden) এরর আটকাবে [cite: 1]
        const rcloneCmd = `rclone copyurl "https://www.googleapis.com/drive/v3/files/${fileId}?alt=media&acknowledgeAbuse=true" \
        :s3:"${process.env.R2_BUCKET_NAME}/${r2Key}" \
        --header "Authorization: Bearer ${token}" \
        --s3-endpoint "https://${process.env.R2_ACCOUNT_ID}.r2.cloudflarestorage.com" \
        --s3-access-key-id "${process.env.R2_ACCESS_KEY}" \
        --s3-secret-access-key "${process.env.R2_SECRET_KEY}" \
        --s3-no-check-bucket \
        --ignore-errors \
        -v`;

        // কমান্ড এক্সিকিউট করা
        exec(rcloneCmd, (error, stdout, stderr) => {
            activeUploads.delete(fileId);
            runningUploads--;
            
            if (error) {
                console.error(`[Rclone Error] ${fileName}:`, stderr);
                failedFiles.add(fileId);
            } else {
                console.log(`[Success] Finished Transfer: ${fileName}`);
            }
            
            // পরবর্তী টাস্ক প্রসেস করার জন্য কিউ চেক করুন
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

// ১. Favicon হ্যান্ডলার (Status 204: No Content)
app.get('/favicon.ico', (req, res) => res.status(204).end());

// ২. হোম রুট
app.get('/', (req, res) => res.send("R2 Bridge API v2.0 (Powered by Rclone) is running."));

// ৩. মেইন এপিআই রুট
app.get('/:fileId', async (req, res) => {
    const fileId = req.params.fileId;
    
    // ফাইল আইডি ভ্যালিডেশন
    if (!fileId || fileId.length < 15) {
        return res.status(400).json({ status: "error", message: "Invalid ID format" });
    }

    // যদি ফাইলটি আগে ফেইল হয়ে থাকে
    if (failedFiles.has(fileId)) {
        return res.status(410).json({ status: "error", message: "Download failed previously. Check Google Drive permissions." });
    }

    try {
        const token = cachedAccessToken || await getAccessToken();
        
        // ১. গুগল ড্রাইভ থেকে মেটাডাটা উদ্ধার
        const metaRes = await axios.get(`https://www.googleapis.com/drive/v3/files/${fileId}?fields=name,size`, {
            headers: { Authorization: `Bearer ${token}` }
        });

        const fileName = metaRes.data.name;
        const r2Key = fileName;

        // ২. চেক করা হচ্ছে ফাইলটি অলরেডি R2 তে আছে কিনা
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
            // ফাইল R2 তে নেই, আপলোড শুরু করতে হবে
            console.log(`[Check] File missing in R2, initiating rclone: ${fileName}`);
        }

        // ৩. কিউতে যুক্ত করা এবং প্রসেস করা
        if (!activeUploads.has(fileId)) {
            activeUploads.add(fileId);
            uploadQueue.push({ fileId, fileName, r2Key });
            processQueue(); 
        }

        res.json({
            status: "processing",
            filename: fileName,
            message: "File is being transferred from Google Drive to R2 via Rclone.",
            queue_position: uploadQueue.length,
            active_threads: runningUploads
        });

    } catch (error) {
    // আগের Console.error এর বদলে এটি দিন
    const errorData = error.response ? error.response.data : error.message;
    console.error(`[Google API Error Detailed]:`, JSON.stringify(errorData));
    
    res.status(500).json({ 
        status: "error", 
        message: "Google API Error",
        detail: error.response?.data?.error?.message || error.message 
    });
}
});

app.listen(PORT, () => console.log(`Worker active on port ${PORT}. Rclone engine is ready.`));
