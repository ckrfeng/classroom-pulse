// Classroom Pulse realtime server.
// Serves the page and relays student votes to the teacher's board, grouped by room code.
// Run with: npm start   (then open the address it prints)

const http = require('http');
const fs = require('fs');
const os = require('os');
const path = require('path');
const { Server } = require('socket.io');

const PORT = Number(process.env.PORT) || 3000;
const PAGE_FILE = path.join(__dirname, 'index.html');
const CODE_CHARS = 'ABCDEFGHJKLMNPQRSTUVWXYZ'; // no I or O, same as the page
const CODE_RE = /^[A-Z]{4}$/;
const BROADCAST_INTERVAL_MS = 150;            // max snapshot rate to the teacher board
const HOST_GRACE_MS = 10 * 60 * 1000;         // keep a room alive this long after the teacher disconnects

// roomCode -> { hostId, votes: Map(studentId -> { val, updatedAt, socketId }), graceTimer, broadcastTimer }
const rooms = new Map();

const hostChannel = (code) => `host:${code}`;
const studentChannel = (code) => `students:${code}`;

function normalizeCode(value) {
    if (typeof value !== 'string') return null;
    const code = value.trim().toUpperCase();
    return CODE_RE.test(code) ? code : null;
}

function sanitizeStudentId(value) {
    if (typeof value !== 'string') return null;
    return /^[A-Za-z0-9-]{1,64}$/.test(value) ? value : null;
}

function clampVal(n) {
    return Math.max(-50, Math.min(50, Math.round(n)));
}

function newRoomCode() {
    let code;
    do {
        code = Array.from({ length: 4 }, () => CODE_CHARS[Math.floor(Math.random() * CODE_CHARS.length)]).join('');
    } while (rooms.has(code));
    return code;
}

function ensureRoom(code) {
    let room = rooms.get(code);
    if (!room) {
        room = { hostId: null, votes: new Map(), graceTimer: null, broadcastTimer: null };
        rooms.set(code, room);
    }
    return room;
}

function destroyRoom(code) {
    const room = rooms.get(code);
    if (!room) return;
    clearTimeout(room.graceTimer);
    clearTimeout(room.broadcastTimer);
    rooms.delete(code);
    io.to(studentChannel(code)).emit('session:ended');
}

function scheduleBroadcast(code) {
    const room = rooms.get(code);
    if (!room || room.broadcastTimer) return;
    room.broadcastTimer = setTimeout(() => {
        room.broadcastTimer = null;
        sendSnapshot(code);
    }, BROADCAST_INTERVAL_MS);
}

function sendSnapshot(code) {
    const room = rooms.get(code);
    if (!room) return;
    const votes = [];
    room.votes.forEach((vote, id) => votes.push({ id, val: vote.val, updatedAt: vote.updatedAt }));
    io.to(hostChannel(code)).emit('room:votes', { votes });
}

// Teacher's socket leaves its room; the room survives a grace period so a page refresh can reclaim it.
function releaseHost(socket) {
    const code = socket.data.hostRoom;
    if (!code) return;
    socket.data.hostRoom = null;
    socket.leave(hostChannel(code));

    const room = rooms.get(code);
    if (!room || room.hostId !== socket.id) return;
    room.hostId = null;
    clearTimeout(room.graceTimer);
    room.graceTimer = setTimeout(() => destroyRoom(code), HOST_GRACE_MS);
}

function removeStudent(socket) {
    const code = socket.data.room;
    if (!code) return;
    const room = rooms.get(code);
    if (room) {
        const vote = room.votes.get(socket.data.studentId);
        // Only remove the vote if it belongs to this socket (a refreshed tab may already have replaced it).
        if (vote && vote.socketId === socket.id) {
            room.votes.delete(socket.data.studentId);
            scheduleBroadcast(code);
        }
    }
    socket.leave(studentChannel(code));
    socket.data.room = null;
    socket.data.studentId = null;
}

function lanBaseUrls() {
    const urls = [];
    Object.values(os.networkInterfaces()).forEach((list) => {
        (list || []).forEach((iface) => {
            const isIPv4 = iface.family === 'IPv4' || iface.family === 4;
            if (isIPv4 && !iface.internal) urls.push(`http://${iface.address}:${PORT}/`);
        });
    });
    return urls;
}

const server = http.createServer((req, res) => {
    const url = new URL(req.url, 'http://localhost');

    if (url.pathname === '/api/info') {
        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ studentBaseUrls: lanBaseUrls() }));
        return;
    }

    if (url.pathname === '/' || url.pathname === '/index.html') {
        fs.readFile(PAGE_FILE, (err, data) => {
            if (err) {
                res.writeHead(500, { 'Content-Type': 'text/plain' });
                res.end('Classroom Pulse page file is missing.');
                return;
            }
            res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
            res.end(data);
        });
        return;
    }

    res.writeHead(404, { 'Content-Type': 'text/plain' });
    res.end('Not found');
});

const io = new Server(server);

io.on('connection', (socket) => {
    // ---- Teacher (smart board) ----------------------------------------------
    socket.on('host:open', (payload, ack) => {
        const reply = typeof ack === 'function' ? ack : () => {};
        releaseHost(socket);

        let code = normalizeCode(payload && payload.room);
        const existing = code ? rooms.get(code) : null;
        // Use a fresh code if none was requested or another live teacher already owns this one.
        if (!code || (existing && existing.hostId && existing.hostId !== socket.id)) {
            code = newRoomCode();
        }

        const room = ensureRoom(code);
        clearTimeout(room.graceTimer);
        room.graceTimer = null;
        room.hostId = socket.id;
        socket.data.hostRoom = code;
        socket.join(hostChannel(code));

        reply({ ok: true, room: code });
        sendSnapshot(code);
    });

    socket.on('host:close', (payload) => {
        const code = normalizeCode(payload && payload.room);
        const room = code ? rooms.get(code) : null;
        if (!room || room.hostId !== socket.id) return;
        socket.data.hostRoom = null;
        socket.leave(hostChannel(code));
        destroyRoom(code);
    });

    // ---- Student (dial) -----------------------------------------------------
    socket.on('student:join', (payload, ack) => {
        const reply = typeof ack === 'function' ? ack : () => {};
        const code = normalizeCode(payload && payload.room);
        const studentId = sanitizeStudentId(payload && payload.studentId);

        if (!code || !rooms.has(code)) {
            reply({ ok: false, error: `No live session with code ${code || '----'}. Check the code with your teacher.` });
            return;
        }
        if (!studentId) {
            reply({ ok: false, error: 'Could not identify this device. Refresh the page and try again.' });
            return;
        }

        removeStudent(socket);
        socket.data.room = code;
        socket.data.studentId = studentId;
        socket.join(studentChannel(code));
        reply({ ok: true, room: code });
    });

    socket.on('student:vote', (payload) => {
        const code = socket.data.room;
        const room = code ? rooms.get(code) : null;
        if (!room) return;

        const val = Number(payload && payload.val);
        if (!Number.isFinite(val)) return;

        room.votes.set(socket.data.studentId, { val: clampVal(val), updatedAt: Date.now(), socketId: socket.id });
        scheduleBroadcast(code);
    });

    socket.on('student:leave', () => removeStudent(socket));

    socket.on('disconnect', () => {
        removeStudent(socket);
        releaseHost(socket);
    });
});

server.listen(PORT, () => {
    console.log('\nClassroom Pulse is running.');
    console.log(`  Teacher (this computer): http://localhost:${PORT}/`);
    lanBaseUrls().forEach((url) => console.log(`  Students join at:        ${url}`));
    console.log('');
});
