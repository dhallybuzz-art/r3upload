const express = require('express');
const axios = require('axios');
const { S3Client, HeadObjectCommand, GetObjectCommand } = require('@aws-sdk/client-s3');
const { Upload } = require('@aws-sdk/lib-storage'); 
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

const activeUploads = new Set();

const getAccessToken = async () => {
    const res = await axios.post('https://oauth2.googleapis.com/token', {
        client_id: GD_CONFIG.clientId,
        client_secret: GD_CONFIG.clientSecret,
        refresh_token: GD_CONFIG.refreshToken,
        grant_type: 'refresh_token'
    });
    return res.data.access_token;
};

// এই ফাংশনটি গুগল ড্রাইভের বড় ফাইলের সিকিউরিটি ওয়ার্নিং বাইপাস করবে
const startDirectTransfer = async (fileId, fileName) => {
    if (activeUploads.has(fileId)) return;
    activeUploads.add(fileId);

    try {
        const token = await getAccessToken();
        
        // ১. বড় ফাইলের জন্য ডাউনলোড ইউআরএল (acknowledgeAbuse=true ব্যবহার করে)
        const downloadUrl = `https://www.googleapis.com/drive/v3/files/${fileId}?alt=media&acknowledgeAbuse=true`;

        const response = await axios({
            method: 'get',
            url: downloadUrl,
            headers: { Authorization: `Bearer ${token}` },
            responseType: 'stream'
        });

        const parallelUploads3 = new Upload({
            client: s3Client,
            params: {
                Bucket: process.env.R2_BUCKET_NAME,
                Key: fileName,
                Body: response.data,
                // সরাসরি ডাউনলোডের জন্য হেডার
                ContentDisposition: `attachment; filename="${encodeURIComponent(fileName)}"`
            },
            queueSize: 4, 
            partSize: 10 * 1024 * 1024,
            leavePartsOnError: false,
        });

        console.log(`[Transfer Start] ${fileName}`);
        await parallelUploads3.done();
        console.log(`[Transfer Success] ${fileName}`);
    } catch (err) {
        console.error(`[Fatal Error] ${fileName}: ${err.message}`);
    } finally {
        activeUploads.delete(fileId);
    }
};

app.get('/favicon.ico', (req, res) => res.status(204).end());
app.get('/', (req, res) => res.send("Engine Ready."));

app.get('/:fileId', async (req, res) => {
    const fileId = req.params.fileId.trim();
    if (!fileId || fileId.length < 15) return res.status(400).send("Invalid ID");

    try {
        const token = await getAccessToken();
        const meta = await axios.get(`https://www.googleapis.com/drive/v3/files/${fileId}?fields=name,size`, {
            headers: { Authorization: `Bearer ${token}` }
        });
        const fileName = meta.data.name;

        try {
            const head = await s3Client.send(new HeadObjectCommand({ Bucket: process.env.R2_BUCKET_NAME, Key: fileName }));
            
            const presignedUrl = await getSignedUrl(s3Client, new GetObjectCommand({ 
                Bucket: process.env.R2_BUCKET_NAME, 
                Key: fileName,
                ResponseContentDisposition: `attachment; filename="${fileName}"`
            }), { expiresIn: 3600 });
            
            return res.json({ 
                status: "success", 
                filename: fileName, 
                url: `${process.env.R2_PUBLIC_DOMAIN}/${encodeURIComponent(fileName)}`, 
                presigned_url: presignedUrl 
            });
        } catch (e) {
            startDirectTransfer(fileId, fileName);
            res.json({ status: "processing", filename: fileName, message: "Bypassing Google 403... Transfer started." });
        }
    } catch (err) {
        res.status(500).send("API Error: " + err.message);
    }
});

app.listen(PORT, () => console.log(`Worker active on ${PORT}`));
