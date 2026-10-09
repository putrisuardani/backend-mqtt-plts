require('dotenv').config();

const express = require('express');
const http = require('http');
const { WebSocket, WebSocketServer } = require('ws');
const { createClient } = require('@supabase/supabase-js');
const mqtt = require('mqtt');

const app = express();
app.use(express.json());

// =====================================
// Konfigurasi Supabase
// =====================================
if (
    !process.env.SUPABASE_URL ||
    !process.env.SUPABASE_SECRET_KEY
) {
    throw new Error('Konfigurasi Supabase di .env belum lengkap');
}

const db = createClient(
    process.env.SUPABASE_URL,
    process.env.SUPABASE_SECRET_KEY
);

const num = x =>
    typeof x === 'number' &&
    Number.isFinite(x) &&
    x >= 0;

// =====================================
// HTTP Server + WebSocket Server
// =====================================
const PORT = process.env.PORT || 3000;
const server = http.createServer(app);

const wss = new WebSocketServer({ server });

// Menyimpan data terbaru di memori backend.
let latestTelemetry = null;

wss.on('connection', (ws) => {
    console.log('Flutter terhubung melalui WebSocket');

    // Kirim data terakhir saat Flutter baru terhubung.
    if (latestTelemetry) {
        ws.send(JSON.stringify(latestTelemetry));
    }

    ws.on('error', (err) => {
        console.error('WebSocket client error:', err.message);
    });

    ws.on('close', () => {
        console.log('Flutter terputus dari WebSocket');
    });
});

// =====================================
// Fungsi mengirim data ke Flutter
// =====================================
function broadcastTelemetry(payload) {
    const message = JSON.stringify(payload);

    wss.clients.forEach((client) => {
        if (client.readyState === WebSocket.OPEN) {
            client.send(message, (err) => {
                if (err) {
                    console.error(
                        'Gagal mengirim data ke Flutter:',
                        err.message
                    );
                }
            });
        }
    });
}

// =====================================
// Endpoint HTTP untuk menerima telemetry
// (opsional, tetap dipertahankan)
// =====================================
app.post('/api/telemetry', async (req, res) => {
    const { voltage, current, power, energy } = req.body || {};

    const valid = [
        voltage,
        current,
        power,
        energy
    ].every(num);

    if (!valid) {
        return res.status(422).json({
            message: 'Payload tidak valid'
        });
    }

    const { data, error } = await db
        .from('plts_telemetry')
        .insert({
            voltage,
            current,
            power,
            energy
        })
        .select('id, received_at')
        .single();

    if (error) {
        console.error(
            'Gagal menyimpan telemetry HTTP:',
            error.message
        );

        return res.status(500).json({
            message: 'Gagal menyimpan telemetry'
        });
    }

    res.status(201).json(data);
});

// =====================================
// Health check
// =====================================
app.get('/health', (_, res) => {
    res.json({
        status: 'ok',
        mqtt: mqttClient.connected,
        websocketClients: wss.clients.size
    });
});

// =====================================
// Konfigurasi MQTT HiveMQ Cloud
// =====================================
if (
    !process.env.MQTT_HOST ||
    !process.env.MQTT_PORT ||
    !process.env.MQTT_USERNAME ||
    !process.env.MQTT_PASSWORD
) {
    throw new Error('Konfigurasi MQTT di .env belum lengkap');
}

const mqttUrl =
    `mqtts://${process.env.MQTT_HOST}:${process.env.MQTT_PORT}`;

const mqttClient = mqtt.connect(mqttUrl, {
    username: process.env.MQTT_USERNAME,
    password: process.env.MQTT_PASSWORD,
    clientId: `backend-plts-${process.pid}`
});

// =====================================
// Pengaturan penyimpanan Supabase
// =====================================
const SAVE_INTERVAL_MS = 60 * 1000;

let lastSavedAt = 0;
let saving = false;

// =====================================
// Koneksi dan subscription MQTT
// =====================================
mqttClient.on('connect', () => {
    console.log('Terhubung ke HiveMQ Cloud');

    mqttClient.subscribe('iot/pzem', (err) => {
        if (err) {
            console.error('Gagal subscribe:', err.message);
            return;
        }

        console.log('Subscribe ke topik iot/pzem berhasil');
    });
});

mqttClient.on('error', (err) => {
    console.error('MQTT error:', err.message);
});

// =====================================
// Menerima pesan MQTT
// =====================================
mqttClient.on('message', async (topic, message) => {
    try {
        const payload = JSON.parse(message.toString());

        const { voltage, current, power, energy } = payload;

        const valid = [
            voltage,
            current,
            power,
            energy
        ].every(num);

        if (!valid) {
            console.log('Payload MQTT tidak valid:', payload);
            return;
        }

        // 1. Perbarui data terbaru di backend.
        const telemetry = {
            voltage,
            current,
            power,
            energy
        };

        latestTelemetry = telemetry;

        // 2. Teruskan SETIAP pesan valid ke Flutter.
        broadcastTelemetry(telemetry);

        // 3. Batasi penyimpanan Supabase ke satu kali per menit.
        const now = Date.now();

        if (
            saving ||
            now - lastSavedAt < SAVE_INTERVAL_MS
        ) {
            return;
        }

        saving = true;

        try {
            const { data, error } = await db
                .from('plts_telemetry')
                .insert({
                    voltage,
                    current,
                    power,
                    energy
                })
                .select('id, received_at')
                .single();

            if (error) {
                console.error(
                    'Gagal menyimpan ke Supabase:',
                    error.message
                );
                return;
            }

            lastSavedAt = Date.now();

            console.log('Telemetry tersimpan:', data);
        } finally {
            saving = false;
        }
    } catch (err) {
        console.error(
            'Gagal memproses pesan MQTT:',
            err.message
        );
    }
});

// =====================================
// Menjalankan server
// =====================================
server.listen(PORT, () => {
    console.log(`Backend aktif di http://localhost:${PORT}`);
    console.log(`WebSocket siap di ws://localhost:${PORT}`);
});

server.on('close', () => {
    console.log('Server ditutup!');
});

server.on('error', (err) => {
    console.error('HTTP server error:', err.message);
});