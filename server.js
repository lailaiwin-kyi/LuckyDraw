require('dotenv').config();
const express = require('express');
const { WebSocketServer, WebSocket } = require('ws');
const http = require('http');
const path = require('path');
const mysql = require('mysql2/promise');

const app = express();
const server = http.createServer(app);
const wss = new WebSocketServer({ server });

app.use(express.json());
app.use(express.static(path.join(__dirname, 'public')));

// MySQL Database Connection Pool
const db = mysql.createPool({
    host: process.env.DB_HOST || 'localhost',
    user: process.env.DB_USER || 'root',
    password: process.env.DB_PASSWORD || '',
    database: process.env.DB_NAME || 'luckydraw',
    waitForConnections: true,
    connectionLimit: 10,
    queueLimit: 0
});

// Helper Function: Database ထဲမှ မဲပေါက်ဖူးသူများကို ဆွဲထုတ်ရန်
async function getWinnersFromDB() {
    try {
        const [rows] = await db.query(`
            SELECT user_id AS userId, name AS userName, won_prize AS prize, 
            DATE_FORMAT(spun_at, '%h:%i:%s %p') AS time 
            FROM users 
            WHERE has_spun = 1 AND won_prize IS NOT NULL 
            ORDER BY spun_at DESC
        `);
        return rows;
    } catch (err) {
        console.error('Error fetching winners:', err.message);
        return [];
    }
}

// Initialize Database Table & Insert Default Users if empty
async function initDatabase() {
    try {
        await db.query(`
            CREATE TABLE IF NOT EXISTS users (
                user_id VARCHAR(50) PRIMARY KEY,
                name VARCHAR(100) NOT NULL,
                has_spun TINYINT(1) DEFAULT 0,
                won_prize VARCHAR(100) DEFAULT NULL,
                spun_at TIMESTAMP NULL DEFAULT NULL
            )
        `);

        // Check if table is empty, insert initial demo users
        const [rows] = await db.query('SELECT COUNT(*) as count FROM users');
        if (rows[0].count === 0) {
            await db.query(`
                INSERT INTO users (user_id, name) VALUES 
                ('ID001', 'Alice'),
                ('ID002', 'Bob'),
                ('ID003', 'Charlie'),
                ('ID004', 'David'),
                ('ID005', 'Emma')
            `);
            console.log('✅ Default users inserted into MySQL.');
        }

        // Database ပွင့်လာပါက History Log ထဲသို့ DB မှ မဲပေါက်သူများ ကြိုတင်ထည့်သွင်းပေးခြင်း
        historyLog = await getWinnersFromDB();

        console.log('✅ Connected to MySQL Database successfully.');
    } catch (err) {
        console.error('❌ Database Initialization Error:', err.message);
    }
}
initDatabase();

// Default Prize List
const defaultPrizes = [
    { label: "Prize 1", color: "#0a4d70" },
    { label: "Prize 2", color: "#a11c47" },
    { label: "Prize 3", color: "#0a4d70" },
    { label: "Prize 4", color: "#a11c47" },
    { label: "Prize 5", color: "#0a4d70" },
    { label: "Prize 6", color: "#a11c47" },
    { label: "Prize 7", color: "#0a4d70" },
    { label: "Prize 8", color: "#a11c47" }
];

let prizes = [...defaultPrizes];
let isSpinning = false;
let historyLog = [];

function broadcast(data) {
    const message = JSON.stringify(data);
    wss.clients.forEach(client => {
        if (client.readyState === WebSocket.OPEN) {
            client.send(message);
        }
    });
}

wss.on('connection', async (ws) => {
    // Connection အသစ်ဝင်လာပါက DB မှ Winner List အဆန်းဆုံးကို ယူ၍ ပို့ပေးခြင်း
    historyLog = await getWinnersFromDB();

    // Send initial state upon connection (winners property အပါအဝင်)
    ws.send(JSON.stringify({ 
        type: 'INIT', 
        prizes, 
        isSpinning, 
        historyLog,
        winners: historyLog 
    }));

    ws.on('message', async (message) => {
        try {
            const data = JSON.parse(message);

            // 1. User Login Verification
            if (data.type === 'LOGIN_USER') {
                const userId = (data.userId || '').trim();

                const [rows] = await db.query('SELECT * FROM users WHERE user_id = ?', [userId]);

                if (rows.length === 0) {
                    ws.send(JSON.stringify({ type: 'LOGIN_RESPONSE', success: false, error: 'invalid user' }));
                } else if (rows[0].has_spun === 1) {
                    ws.send(JSON.stringify({ type: 'LOGIN_RESPONSE', success: false, error: 'you already spinned' }));
                } else {
                    ws.send(JSON.stringify({ 
                        type: 'LOGIN_RESPONSE', 
                        success: true, 
                        userId: rows[0].user_id, 
                        userName: rows[0].name 
                    }));
                }
            }

            // 2. Spin Request
            if (data.type === 'REQUEST_SPIN' && !isSpinning && prizes.length > 0) {
                const userId = data.userId;

                // Double check validity from MySQL
                const [rows] = await db.query('SELECT * FROM users WHERE user_id = ?', [userId]);

                if (rows.length === 0) {
                    ws.send(JSON.stringify({ type: 'SPIN_ERROR', error: 'invalid user' }));
                    return;
                }

                if (rows[0].has_spun === 1) {
                    ws.send(JSON.stringify({ type: 'SPIN_ERROR', error: 'you already spinned' }));
                    return;
                }

                // Mark user as spun in DB immediately
                await db.query('UPDATE users SET has_spun = 1 WHERE user_id = ?', [userId]);
                isSpinning = true;

                const winningIndex = Math.floor(Math.random() * prizes.length);
                const extraDegrees = Math.floor(Math.random() * 360);
                
                broadcast({
                    type: 'START_SPIN',
                    winningIndex,
                    extraDegrees,
                    userId,
                    userName: rows[0].name
                });

                setTimeout(async () => {
                    const wonPrize = prizes[winningIndex];
                    prizes.splice(winningIndex, 1); // Automatically remove won prize
                    isSpinning = false;

                    const timeString = new Date().toLocaleTimeString();

                    // Update database record with won prize
                    await db.query(
                        'UPDATE users SET won_prize = ?, spun_at = NOW() WHERE user_id = ?',
                        [wonPrize.label, userId]
                    );

                    const winRecord = {
                        userId,
                        userName: rows[0].name,
                        prize: wonPrize.label,
                        time: timeString
                    };
                    historyLog.unshift(winRecord);

                    // Winner အသစ်တိုးလာပါက Admin ရော Client ပါ တပြိုင်နက် Update ဖြစ်အောင် broadcast လုပ်ပေးခြင်း
                    broadcast({
                        type: 'SPIN_COMPLETE',
                        wonPrize,
                        prizes,
                        userId,
                        userName: rows[0].name,
                        winRecord,
                        winners: historyLog
                    });
                }, 4100);
            }

            // 3. Admin Actions
            if (data.type === 'ADD_PRIZE' && !isSpinning) {
                prizes.push(data.prize);
                broadcast({ type: 'UPDATE_PRIZES', prizes });
            }

            if (data.type === 'REMOVE_PRIZE' && !isSpinning) {
                prizes.splice(data.index, 1);
                broadcast({ type: 'UPDATE_PRIZES', prizes });
            }

            if (data.type === 'RESET_PRIZES' && !isSpinning) {
                prizes = [...defaultPrizes];
                broadcast({ type: 'UPDATE_PRIZES', prizes });
            }

            if (data.type === 'RESET_SPUN_USERS') {
                await db.query('UPDATE users SET has_spun = 0, won_prize = NULL, spun_at = NULL');
                historyLog = [];
                broadcast({ 
                    type: 'SYSTEM_MESSAGE', 
                    message: 'User spin limits reset by Admin!',
                    winners: [] 
                });
                broadcast({
                    type: 'UPDATE_WINNERS',
                    winners: []
                });
            }
        } catch (err) {
            console.error('WebSocket Message Processing Error:', err.message);
        }
    });
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
    console.log(`🚀 Server running on http://localhost:${PORT}`);
    console.log(` Client Page: http://localhost:${PORT}/index.html`);
    console.log(` Admin Page: http://localhost:${PORT}/server_admin.html`);
});