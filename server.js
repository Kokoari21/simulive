const express = require('express');
const multer = require('multer');
const { Queue } = require('bullmq');
const cors = require('cors');
const path = require('path');
const fs = require('fs');

// --- Setup ---
const app = express();
app.use(cors());
app.use(express.json());

// Redis Connection for Queue
const redisConnection = {
    host: process.env.REDIS_HOST || 'localhost',
    port: 6379
};

// Create Queues
const transcodeQueue = new Queue('transcode-queue', { connection: redisConnection });
const streamQueue = new Queue('stream-queue', { connection: redisConnection });

// File Upload Storage
const storage = multer.diskStorage({
    destination: './uploads/',
    filename: (req, file, cb) => {
        cb(null, Date.now() + '-' + file.originalname);
    }
});
const upload = multer({ storage: storage });

// Mock Database (In-memory for demo, use Postgres in prod)
const db = {
    users: [],
    videos: [],
    schedules: []
};

// --- Routes ---

// 1. Login/Register (Mocked)
app.post('/api/auth/login', (req, res) => {
    // In real app: Validate password hash
    const user = { id: 1, name: 'Demo User', email: req.body.email };
    res.json({ token: 'mock-jwt-token', user });
});

// 2. Upload Video
app.post('/api/videos/upload', upload.single('file'), async (req, res) => {
    if (!req.file) return res.status(400).send('No file uploaded');

    const video = {
        id: Date.now(),
        filename: req.file.filename,
        path: req.file.path,
        title: req.body.title || 'Untitled',
        status: 'ready', // In real app: 'processing' then triggers transcode job
        streamKey: `user_${Date.now()}` // Unique key for HLS
    };
    
    db.videos.push(video);
    res.json(video);
});

// 3. Get Videos
app.get('/api/videos', (req, res) => {
    res.json(db.videos);
});

// 4. Schedule Simulive
app.post('/api/videos/:videoId/schedule', async (req, res) => {
    const videoId = parseInt(req.params.videoId);
    const video = db.videos.find(v => v.id === videoId);
    
    if (!video) return res.status(404).send('Video not found');

    const { startTime, targets } = req.body; 
    // targets example: [{ platform: 'youtube', rtmpUrl: '...', key: '...' }]

    const scheduleId = Date.now();
    const delay = new Date(startTime).getTime() - Date.now();

    if (delay < 0) return res.status(400).send('Start time must be in the future');

    // Add to BullMQ with delay
    await streamQueue.add('start-stream', {
        videoId: video.id,
        filePath: video.path,
        streamKey: video.streamKey, // For local HLS
        targets: targets
    }, {
        delay: delay,
        jobId: `sched_${scheduleId}`
    });

    const schedule = { id: scheduleId, videoId, startTime, targets, status: 'scheduled' };
    db.schedules.push(schedule);

    console.log(`Scheduled video ${video.title} to start in ${delay/1000} seconds`);
    res.json(schedule);
});

// Start Server
app.listen(4000, () => {
    if (!fs.existsSync('./uploads')){
        fs.mkdirSync('./uploads');
    }
    console.log('API Server running on port 4000');
});
