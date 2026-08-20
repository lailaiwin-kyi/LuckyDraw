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

// Helper Function: Fetch winners list from winners table
async function getWinnersFromDB() {
    try {
        const res = await pool.query(`
            SELECT 
                w.user_id AS "userId",
                w.user_id AS "userid",
                w.user_id AS "id",
                w.user_id AS "user_id",
                u.name AS "userName",
                u.name AS "username",
                u.name AS "name",
                w.prize_name AS "prize", 
                w.prize_name AS "won_prize", 
                w.prize_name AS "prizeName",
                TO_CHAR(w.won_at, 'HH12:MI:SS AM') AS "time" 
            FROM winners w
            JOIN users u ON w.user_id = u.user_id
            ORDER BY w.won_at DESC
        `);
        return res.rows;
    } catch (err) {
        console.error('Error fetching winners:', err.message);
        return [];
    }
}

// Helper Function: Fetch all prizes from Database
async function getPrizesFromDB() {
    try {
        const res = await pool.query('SELECT id, label, color, disabled, quantity, initial_quantity FROM prizes ORDER BY id ASC');
        return res.rows;
    } catch (err) {
        console.error('Error fetching prizes:', err.message);
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

// Default Prize List (With Quantity)
const defaultPrizes = [
    { label: "Prize 1", color: "#0a4d70", disabled: false, quantity: 8, initial_quantity: 8 },
    { label: "Prize 2", color: "#a11c47", disabled: false, quantity: 5, initial_quantity: 5 },
    { label: "Prize 3", color: "#0a4d70", disabled: false, quantity: 5, initial_quantity: 5 },
    { label: "Prize 4", color: "#a11c47", disabled: false, quantity: 3, initial_quantity: 3 },
    { label: "Prize 5", color: "#0a4d70", disabled: false, quantity: 3, initial_quantity: 3 },
    { label: "Prize 6", color: "#a11c47", disabled: false, quantity: 2, initial_quantity: 2 },
    { label: "Prize 7", color: "#0a4d70", disabled: false, quantity: 2, initial_quantity: 2 },
    { label: "Prize 8", color: "#a11c47", disabled: false, quantity: 1, initial_quantity: 1 }
];

let prizes = [];
let isSpinning = false;
let historyLog = [];

// Initialize Database Tables
async function initDatabase() {
    try {
        // 1. Users Table
        await pool.query(`
            CREATE TABLE IF NOT EXISTS users (
                user_id VARCHAR(50) PRIMARY KEY,
                name VARCHAR(100) NOT NULL,
                has_spun INT DEFAULT 0,
                won_prize VARCHAR(100) DEFAULT NULL,
                spun_at TIMESTAMP NULL DEFAULT NULL
            )
        `);

        // 2. Prizes Table (added quantity)
        await pool.query(`
            CREATE TABLE IF NOT EXISTS prizes (
                id SERIAL PRIMARY KEY,
                label VARCHAR(100) NOT NULL,
                color VARCHAR(20) DEFAULT '#0a4d70',
                disabled BOOLEAN DEFAULT FALSE,
                quantity INT DEFAULT 1,
                initial_quantity INT DEFAULT 1,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        `);

        // 3. Winners Table
        await pool.query(`
            CREATE TABLE IF NOT EXISTS winners (
                id SERIAL PRIMARY KEY,
                user_id VARCHAR(50) REFERENCES users(user_id) ON DELETE CASCADE,
                prize_id INT REFERENCES prizes(id) ON DELETE SET NULL,
                prize_name VARCHAR(100) NOT NULL,
                won_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        `);

        // Insert Default Users
        const userCount = await pool.query('SELECT COUNT(*) as count FROM users');
        if (parseInt(userCount.rows[0].count) === 0) {
            await pool.query(`
                INSERT INTO users (user_id, name) VALUES 
                ('ID001', 'Alice'),
                ('ID002', 'Bob'),
                ('ID003', 'Charlie'),
                ('ID004', 'David'),
                ('ID005', 'Emma')
            `);
            console.log('✅ Default users inserted.');
        }

        // Insert Default Prizes
        const prizeCount = await pool.query('SELECT COUNT(*) as count FROM prizes');
        if (parseInt(prizeCount.rows[0].count) === 0) {
            for (const prize of defaultPrizes) {
                await pool.query(
                    'INSERT INTO prizes (label, color, disabled, quantity, initial_quantity) VALUES ($1, $2, $3, $4, $5)', 
                    [prize.label, prize.color, prize.disabled, prize.quantity, prize.initial_quantity]
                );
            }
            console.log('✅ Default prizes inserted.');
        }

        prizes = await getPrizesFromDB();
        historyLog = await getWinnersFromDB();
        console.log('✅ Connected to PostgreSQL Database successfully.');
    } catch (err) {
        console.error('❌ Database Initialization Error:', err.message);
    }
}
initDatabase();

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
    prizes = await getPrizesFromDB();

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
            prizes = await getPrizesFromDB();
            // Pick only prizes where disabled is false AND quantity > 0
            const availableIndices = prizes.map((p, index) => (p.disabled || p.quantity <= 0) ? null : index).filter(i => i !== null);

            if (data.type === 'REQUEST_SPIN' && !isSpinning && availableIndices.length > 0) {
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

                const winningIndex = availableIndices[Math.floor(Math.random() * availableIndices.length)];
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
                    
                    // Deduct prize quantity by 1, and set disabled = TRUE if quantity reaches 0
                    await pool.query(`
                        UPDATE prizes 
                        SET quantity = quantity - 1,
                            disabled = CASE WHEN (quantity - 1) <= 0 THEN TRUE ELSE FALSE END
                        WHERE id = $1
                    `, [wonPrize.id]);

                    prizes = await getPrizesFromDB();
                    isSpinning = false;

                    const timeString = new Date().toLocaleTimeString();

                    // Update User Record
                    await pool.query(
                        'UPDATE users SET won_prize = $1, spun_at = NOW() WHERE user_id = $2',
                        [wonPrize.label, userId]
                    );

                    // Insert into Winners Table
                    await pool.query(
                        'INSERT INTO winners (user_id, prize_id, prize_name) VALUES ($1, $2, $3)',
                        [userId, wonPrize.id, wonPrize.label]
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
                const { label, color, quantity } = data.prize;
                const qty = parseInt(quantity) || 1;
                await pool.query(
                    'INSERT INTO prizes (label, color, disabled, quantity, initial_quantity) VALUES ($1, $2, FALSE, $3, $4)', 
                    [label, color || '#0a4d70', qty, qty]
                );
                prizes = await getPrizesFromDB();
                broadcast({ type: 'UPDATE_PRIZES', prizes });
            }

            if (data.type === 'REMOVE_PRIZE' && !isSpinning) {
                const prizeToDelete = prizes[data.index];
                if (prizeToDelete) {
                    await pool.query('DELETE FROM prizes WHERE id = $1', [prizeToDelete.id]);
                    prizes = await getPrizesFromDB();
                    broadcast({ type: 'UPDATE_PRIZES', prizes });
                }
            }

            if (data.type === 'RESET_PRIZES' && !isSpinning) {
                await pool.query('DELETE FROM prizes');
                for (const prize of defaultPrizes) {
                    await pool.query(
                        'INSERT INTO prizes (label, color, disabled, quantity, initial_quantity) VALUES ($1, $2, $3, $4, $5)', 
                        [prize.label, prize.color, prize.disabled, prize.quantity, prize.initial_quantity]
                    );
                }
                prizes = await getPrizesFromDB();
                broadcast({ type: 'UPDATE_PRIZES', prizes });
            }

            if (data.type === 'RESET_SPUN_USERS') {
                await pool.query('UPDATE users SET has_spun = 0, won_prize = NULL, spun_at = NULL');
                await pool.query('DELETE FROM winners');
                await pool.query('UPDATE prizes SET quantity = initial_quantity, disabled = FALSE');
                prizes = await getPrizesFromDB();
                historyLog = [];
                broadcast({ 
                    type: 'SYSTEM_MESSAGE', 
                    message: 'User spin limits, winners history, and prize quantities reset by Admin!',
                    historyLog: [],
                    winners: [] 
                });
                broadcast({ type: 'UPDATE_PRIZES', prizes });
                await broadcastUserData();
            }

            // 4. USER CRUD OPERATIONS
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