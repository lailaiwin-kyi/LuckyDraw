require('dotenv').config();
const express = require('express');
const { WebSocketServer, WebSocket } = require('ws');
const http = require('http');
const path = require('path');
const { Pool } = require('pg');

const app = express();
const server = http.createServer(app);
const wss = new WebSocketServer({ server });

app.use(express.json());
app.use(express.static(path.join(__dirname, 'public')));

// Render PostgreSQL Database Connection
const pool = new Pool({
    connectionString: process.env.DATABASE_URL,
    ssl: process.env.DATABASE_URL ? { rejectUnauthorized: false } : false
});

// Helper Function: Fetch all users from Database
async function getAllUsersFromDB() {
    try {
        const res = await pool.query(`
            SELECT 
                user_id AS "user_id",
                user_id AS "userId",
                name AS "name",
                has_spun AS "has_spun",
                won_prize AS "won_prize",
                TO_CHAR(spun_at, 'HH12:MI:SS AM') AS "spun_at"
            FROM users 
            ORDER BY user_id ASC
        `);
        return res.rows;
    } catch (err) {
        console.error('Error fetching users:', err.message);
        return [];
    }
}

// Helper Function: Fetch winners list
async function getWinnersFromDB() {
    try {
        const res = await pool.query(`
            SELECT 
                user_id AS "userId",
                user_id AS "userid",
                user_id AS "id",
                user_id AS "user_id",
                name AS "userName",
                name AS "username",
                name AS "name",
                won_prize AS "prize", 
                won_prize AS "won_prize", 
                won_prize AS "prizeName",
                TO_CHAR(spun_at, 'HH12:MI:SS AM') AS "time" 
            FROM users 
            WHERE has_spun = 1 AND won_prize IS NOT NULL 
            ORDER BY spun_at DESC
        `);
        return res.rows;
    } catch (err) {
        console.error('Error fetching winners:', err.message);
        return [];
    }
}

// Broadcast Users & Winners Updates
async function broadcastUserData() {
    const users = await getAllUsersFromDB();
    const winners = await getWinnersFromDB();
    broadcast({ type: 'UPDATE_USERS', users });
    broadcast({ type: 'UPDATE_WINNERS', historyLog: winners, winners });
}

// Initialize Database Table
async function initDatabase() {
    try {
        await pool.query(`
            CREATE TABLE IF NOT EXISTS users (
                user_id VARCHAR(50) PRIMARY KEY,
                name VARCHAR(100) NOT NULL,
                has_spun INT DEFAULT 0,
                won_prize VARCHAR(100) DEFAULT NULL,
                spun_at TIMESTAMP NULL DEFAULT NULL
            )
        `);

        const countRes = await pool.query('SELECT COUNT(*) as count FROM users');
        if (parseInt(countRes.rows[0].count) === 0) {
            await pool.query(`
                INSERT INTO users (user_id, name) VALUES 
                ('ID001', 'Alice'),
                ('ID002', 'Bob'),
                ('ID003', 'Charlie'),
                ('ID004', 'David'),
                ('ID005', 'Emma')
            `);
            console.log('✅ Default users inserted into PostgreSQL.');
        }

        historyLog = await getWinnersFromDB();
        console.log('✅ Connected to PostgreSQL Database successfully.');
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
    historyLog = await getWinnersFromDB();
    const usersList = await getAllUsersFromDB();

    ws.send(JSON.stringify({ 
        type: 'INIT', 
        prizes, 
        isSpinning, 
        historyLog,
        winners: historyLog,
        users: usersList
    }));

    ws.on('message', async (message) => {
        try {
            const data = JSON.parse(message);

            // 1. User Login Verification
            if (data.type === 'LOGIN_USER') {
                const userId = (data.userId || '').trim();
                const res = await pool.query('SELECT * FROM users WHERE user_id = $1', [userId]);

                if (res.rows.length === 0) {
                    ws.send(JSON.stringify({ type: 'LOGIN_RESPONSE', success: false, error: 'invalid user' }));
                } else if (res.rows[0].has_spun === 1) {
                    ws.send(JSON.stringify({ type: 'LOGIN_RESPONSE', success: false, error: 'you already spinned' }));
                } else {
                    ws.send(JSON.stringify({ 
                        type: 'LOGIN_RESPONSE', 
                        success: true, 
                        userId: res.rows[0].user_id, 
                        userName: res.rows[0].name 
                    }));
                }
            }

            // 2. Spin Request
            if (data.type === 'REQUEST_SPIN' && !isSpinning && prizes.length > 0) {
                const userId = data.userId;
                const res = await pool.query('SELECT * FROM users WHERE user_id = $1', [userId]);

                if (res.rows.length === 0) {
                    ws.send(JSON.stringify({ type: 'SPIN_ERROR', error: 'invalid user' }));
                    return;
                }

                if (res.rows[0].has_spun === 1) {
                    ws.send(JSON.stringify({ type: 'SPIN_ERROR', error: 'you already spinned' }));
                    return;
                }

                await pool.query('UPDATE users SET has_spun = 1 WHERE user_id = $1', [userId]);
                isSpinning = true;

                const winningIndex = Math.floor(Math.random() * prizes.length);
                const extraDegrees = Math.floor(Math.random() * 360);
                
                broadcast({
                    type: 'START_SPIN',
                    winningIndex,
                    extraDegrees,
                    userId,
                    userName: res.rows[0].name
                });

                setTimeout(async () => {
                    const wonPrize = prizes[winningIndex];
                    prizes.splice(winningIndex, 1);
                    isSpinning = false;

                    const timeString = new Date().toLocaleTimeString();

                    await pool.query(
                        'UPDATE users SET won_prize = $1, spun_at = NOW() WHERE user_id = $2',
                        [wonPrize.label, userId]
                    );

                    historyLog = await getWinnersFromDB();

                    const winRecord = {
                        userId,
                        userid: userId,
                        id: userId,
                        userName: res.rows[0].name,
                        username: res.rows[0].name,
                        name: res.rows[0].name,
                        prize: wonPrize.label,
                        won_prize: wonPrize.label,
                        prizeName: wonPrize.label,
                        time: timeString
                    };

                    broadcast({
                        type: 'SPIN_COMPLETE',
                        wonPrize,
                        prizes,
                        userId,
                        userName: res.rows[0].name,
                        winRecord,
                        historyLog,
                        winners: historyLog
                    });
                    
                    await broadcastUserData();
                }, 4100);
            }

            // 3. Admin Wheel Actions
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
                await pool.query('UPDATE users SET has_spun = 0, won_prize = NULL, spun_at = NULL');
                historyLog = [];
                broadcast({ 
                    type: 'SYSTEM_MESSAGE', 
                    message: 'User spin limits reset by Admin!',
                    historyLog: [],
                    winners: [] 
                });
                await broadcastUserData();
            }

            // 4. USER CRUD OPERATIONS (Insert, Update, Delete)
            if (data.type === 'ADD_USER') {
                const { userId, name } = data;
                await pool.query('INSERT INTO users (user_id, name) VALUES ($1, $2) ON CONFLICT (user_id) DO NOTHING', [userId, name]);
                await broadcastUserData();
            }

            if (data.type === 'UPDATE_USER') {
                const { userId, name } = data;
                await pool.query('UPDATE users SET name = $1 WHERE user_id = $2', [name, userId]);
                await broadcastUserData();
            }

            if (data.type === 'DELETE_USER') {
                const { userId } = data;
                await pool.query('DELETE FROM users WHERE user_id = $1', [userId]);
                await broadcastUserData();
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