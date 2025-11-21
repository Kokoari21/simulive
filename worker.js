const { Worker } = require('bullmq');
const { spawn } = require('child_process');
const path = require('path');

const redisConnection = {
    host: process.env.REDIS_HOST || 'localhost',
    port: 6379
};

const NGINX_RTMP_URL = process.env.NGINX_RTMP_URL || 'rtmp://localhost:1935/live';

console.log("Worker started, waiting for jobs...");

const worker = new Worker('stream-queue', async (job) => {
    console.log(`Processing job ${job.id}: Starting Simulive Stream`);
    const { filePath, streamKey, targets } = job.data;

    // Resolve absolute path
    const inputPath = path.resolve(filePath);

    // --- Construct FFmpeg Command ---
    // Basic input: Read file at native framerate (-re)
    const args = [
        '-re', 
        '-i', inputPath,
    ];

    // 1. Output to Local Nginx (for Website HLS Player)
    // We map input stream 0 to this output
    args.push(
        '-map', '0',
        '-c:v', 'libx264', '-preset', 'veryfast', '-b:v', '3000k', '-maxrate', '3000k', '-bufsize', '6000k',
        '-c:a', 'aac', '-b:a', '128k', '-ar', '44100',
        '-f', 'flv',
        `${NGINX_RTMP_URL}/${streamKey}`
    );

    // 2. Output to External Targets (Restream)
    // Loop through targets (YouTube, TikTok, Facebook, etc.)
    targets.forEach(target => {
        const rtmpUrl = target.rtmpUrl.endsWith('/') 
            ? target.rtmpUrl + target.key 
            : target.rtmpUrl + '/' + target.key;
            
        console.log(`-> Adding target: ${target.platform} (${rtmpUrl})`);

        args.push(
            '-map', '0',
            '-c', 'copy', // Use copy for efficiency if source is already good, or re-encode if needed
            '-f', 'flv',
            rtmpUrl
        );
    });

    // --- Execute FFmpeg ---
    return new Promise((resolve, reject) => {
        const ffmpeg = spawn('ffmpeg', args);

        ffmpeg.stderr.on('data', (data) => {
            // FFmpeg logs to stderr
            console.log(`[FFmpeg ${job.id}]: ${data.toString().substring(0, 100)}...`);
        });

        ffmpeg.on('close', (code) => {
            if (code === 0) {
                console.log(`Stream ${job.id} finished successfully.`);
                resolve();
            } else {
                console.error(`Stream ${job.id} failed with code ${code}`);
                reject(new Error(`FFmpeg exited with code ${code}`));
            }
        });

        // Safety: Kill stream if job is cancelled (logic needs implementation in queue events)
    });

}, { connection: redisConnection });

worker.on('completed', job => {
    console.log(`${job.id} has completed!`);
});

worker.on('failed', (job, err) => {
    console.log(`${job.id} has failed with ${err.message}`);
});
