const express = require('express');
const { WebSocketServer, WebSocket } = require('ws');
const http = require('http');
const path = require('path');

const app = express();
const server = http.createServer(app);
const wss = new WebSocketServer({ server });

app.use(express.static(path.join(__dirname, 'public')));

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

// Stored Allowed User IDs and their names
const validUsers = {
    "ID001": "Alice",
    "ID002": "Bob",
    "ID003": "Charlie",
    "ID004": "David",
    "ID005": "Emma"
};

// Track users who have already used their 1 spin chance
const spunUsers = new Set();

function broadcast(data) {
    const message = JSON.stringify(data);
    wss.clients.forEach(client => {
        if (client.readyState === WebSocket.OPEN) {
            client.send(message);
        }
    });
}

wss.on('connection', (ws) => {
    // Send initial state upon connection
    ws.send(JSON.stringify({ type: 'INIT', prizes, isSpinning, historyLog }));

    ws.on('message', (message) => {
        const data = JSON.parse(message);

        // 1. User Login Verification
        if (data.type === 'LOGIN_USER') {
            const userId = (data.userId || '').trim();

            if (!validUsers[userId]) {
                ws.send(JSON.stringify({ type: 'LOGIN_RESPONSE', success: false, error: 'invalid user' }));
            } else if (spunUsers.has(userId)) {
                ws.send(JSON.stringify({ type: 'LOGIN_RESPONSE', success: false, error: 'you already spinned' }));
            } else {
                ws.send(JSON.stringify({ 
                    type: 'LOGIN_RESPONSE', 
                    success: true, 
                    userId, 
                    userName: validUsers[userId] 
                }));
            }
        }

        // 2. Spin Request
        if (data.type === 'REQUEST_SPIN' && !isSpinning && prizes.length > 0) {
            const userId = data.userId;

            // Double check validity before spinning
            if (!validUsers[userId]) {
                ws.send(JSON.stringify({ type: 'SPIN_ERROR', error: 'invalid user' }));
                return;
            }

            if (spunUsers.has(userId)) {
                ws.send(JSON.stringify({ type: 'SPIN_ERROR', error: 'you already spinned' }));
                return;
            }

            // Mark user as spun immediately
            spunUsers.add(userId);
            isSpinning = true;

            const winningIndex = Math.floor(Math.random() * prizes.length);
            const extraDegrees = Math.floor(Math.random() * 360);
            
            broadcast({
                type: 'START_SPIN',
                winningIndex,
                extraDegrees,
                userId,
                userName: validUsers[userId]
            });

            setTimeout(() => {
                const wonPrize = prizes[winningIndex];
                prizes.splice(winningIndex, 1); // Automatically remove won prize
                isSpinning = false;

                const winRecord = {
                    userId,
                    userName: validUsers[userId],
                    prize: wonPrize.label,
                    time: new Date().toLocaleTimeString()
                };
                historyLog.unshift(winRecord);

                broadcast({
                    type: 'SPIN_COMPLETE',
                    wonPrize,
                    prizes,
                    userId,
                    userName: validUsers[userId],
                    winRecord
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
            spunUsers.clear();
            broadcast({ type: 'SYSTEM_MESSAGE', message: 'User spin limits reset by Admin!' });
        }
    });
});

server.listen(3000, () => {
    console.log('Server running on http://localhost:3000');
    console.log('Client Page: http://localhost:3000/index.html');
    console.log('Server Admin Page: http://localhost:3000/server_admin.html');
});