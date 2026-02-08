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
    const res = await axios.post('https://oauth2.googleapis.com/token', {
        client_id: GD_CONFIG.clientId,
        client_secret: GD_CONFIG.clientSecret,
        refresh_token: GD_CONFIG.refreshToken,
        grant_type: 'refresh_token'
    });
    return res.data.access_token;
};

// এই ফাংশনটি সরাসরি গুগল থেকে ডাটা নিয়ে আর২-তে স্ট্রিমিং করবে
const startDirectTransfer = async (fileId, fileName) => {
    if (activeUploads.has(fileId)) return;
    activeUploads.add(fileId);

    try {
        const token = await getAccessToken();
        
        // ১. গুগল ড্রাইভ থেকে ডাটা স্ট্রিম আনা
        const response = await axios({
            method: 'get',
            url: `https://www.googleapis.com/drive/v3/files/${fileId}?alt=media&acknowledgeAbuse=true`,
            headers: { Authorization: `Bearer ${token}` },
            responseType: 'stream'
        });

        // ২. AWS SDK দিয়ে সরাসরি R2 তে আপলোড (এটি গ্যারান্টিড AWS4 Signature ব্যবহার করে)
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

        console.log(`[Stream] Transferring: ${fileName}`);
        await parallelUploads3.done();
        console.log(`[Success] Uploaded to R2: ${fileName}`);
    } catch (err) {
        console.error(`[Error] Direct Transfer Failed:`, err.message);
    } finally {
        activeUploads.delete(fileId);
    }
};

app.get('/favicon.ico', (req, res) => res.status(204).end());
app.get('/', (req, res) => res.send("Direct R2 Bridge is active."));

app.get('/:fileId', async (req, res) => {
    const fileId = req.params.fileId.trim();
    if (!fileId || fileId.length < 15) return res.status(400).send("Invalid ID");

    try {
        const token = await getAccessToken();
        const meta = await axios.get(`https://www.googleapis.com/drive/v3/files/${fileId}?fields=name`, {
            headers: { Authorization: `Bearer ${token}` }
        });
        const fileName = meta.data.name;

        try {
            // ১. বাকেটে ফাইল আছে কি না চেক
            await s3Client.send(new HeadObjectCommand({ Bucket: process.env.R2_BUCKET_NAME, Key: fileName }));
            
            // ২. presigned_url তৈরি করা
            const presignedUrl = await getSignedUrl(s3Client, new GetObjectCommand({ Bucket: process.env.R2_BUCKET_NAME, Key: fileName }), { expiresIn: 3600 });
            
            // ৩. পাবলিক ডোমেইন ইউআরএল তৈরি (স্ল্যাশ ম্যানেজমেন্টসহ)
            const publicDomain = process.env.R2_PUBLIC_DOMAIN.replace(/\/$/, '');
            const publicUrl = `${publicDomain}/${encodeURIComponent(fileName)}`;
            
            // আপনার চাহিত ফরম্যাটে রেসপন্স
            return res.json({ 
                status: "success", 
                filename: fileName, 
                url: publicUrl, 
                presigned_url: presignedUrl 
            });
        } catch (e) {
            // ফাইল নেই, তাই সরাসরি ট্রান্সফার শুরু
            startDirectTransfer(fileId, fileName);
            res.json({ 
                status: "processing", 
                filename: fileName, 
                message: "Streaming data directly to R2 using AWS SDK." 
            });
        }
    } catch (err) {
        res.status(500).send("API Error: " + err.message);
    }
});

app.listen(PORT, () => console.log(`Direct Worker active on ${PORT}`));
