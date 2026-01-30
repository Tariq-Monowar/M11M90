const express = require('express')
const { Pool } = require('pg')
const { Server } = require('socket.io')
const mediasoup = require('mediasoup')
const os = require('os')
const http = require('http')

const app = express()
const PORT = process.env.PORT || 3000

const pool = new Pool({
    connectionString: 'postgresql://neondb_owner:npg_5lBiKSDN2ZnT@ep-empty-wind-a1vittzf-pooler.ap-southeast-1.aws.neon.tech/neondb?sslmode=require&channel_binding=require'
})

app.use((req, res, next) => {
    res.header('Access-Control-Allow-Origin', '*')
    res.header('Access-Control-Allow-Methods', 'GET, POST, PUT, DELETE, OPTIONS')
    res.header('Access-Control-Allow-Headers', 'Origin, X-Requested-With, Content-Type, Accept')
    if (req.method === 'OPTIONS') return res.sendStatus(200)
    next()
})

app.use(express.json())
app.use(express.urlencoded({ extended: true }))

pool.connect((err, client, release) => {
    if (err) return console.error('Database error:', err.stack)
    console.log('✅ Database connected')
    release()
})

app.post('/users', async (req, res) => {
    try {
        const { userName, email } = req.body
        const result = await pool.query('INSERT INTO users (username, email) VALUES ($1, $2) RETURNING *', [userName, email])
        res.json(result.rows[0])
    } catch (error) {
        res.status(500).json({ error: error.message })
    }
})

app.get('/users', async (req, res) => {
    try {
        const result = await pool.query('SELECT * FROM users')
        res.json(result.rows)
    } catch (error) {
        res.status(500).json({ error: error.message })
    }
})

let worker = null
const rooms = new Map()
const participants = new Map()

function getLocalIp() {
    const interfaces = os.networkInterfaces()
    for (const name of Object.keys(interfaces)) {
        for (const iface of interfaces[name]) {
            if (iface.family === 'IPv4' && !iface.internal) {
                return iface.address
            }
        }
    }
    return '127.0.0.1'
}

async function createWorker() {
    worker = await mediasoup.createWorker({
        logLevel: 'warn',
        rtcMinPort: 40000,
        rtcMaxPort: 49999,
    })
    worker.on('died', () => {
        console.error('Mediasoup worker died')
        process.exit(1)
    })
    console.log('✅ Mediasoup worker created')
    return worker
}

async function getOrCreateRouter(roomId) {
    if (rooms.has(roomId)) return rooms.get(roomId)
    
    const router = await worker.createRouter({
        mediaCodecs: [
            { kind: 'audio', mimeType: 'audio/opus', clockRate: 48000, channels: 2 },
            { kind: 'video', mimeType: 'video/VP8', clockRate: 90000 },
        ],
    })
    rooms.set(roomId, router)
    return router
}

function getProducersForRoom(roomId) {
    const producers = []
    for (const [socketId, participant] of participants.entries()) {
        if (participant.roomId === roomId) {
            for (const producer of participant.producers.values()) {
                producers.push({ id: producer.id, kind: producer.kind, socketId })
            }
        }
    }
    return producers
}

function cleanupParticipant(socketId) {
    const participant = participants.get(socketId)
    if (!participant) return
    
    for (const transport of participant.transports.values()) {
        transport.close()
    }
    participants.delete(socketId)
    
    const roomId = participant.roomId
    const roomParticipants = Array.from(participants.values()).filter(p => p.roomId === roomId)
    
    if (roomParticipants.length === 0) {
        const router = rooms.get(roomId)
        if (router) {
            router.close()
            rooms.delete(roomId)
        }
    }
    
    return roomId
}

const httpServer = http.createServer(app)
const io = new Server(httpServer, {
    cors: { origin: '*', methods: ['GET', 'POST'] },
})

io.on('connection', (socket) => {
    console.log(`✅ Client connected: ${socket.id}`)
    
    socket.on('createRoom', async ({ roomId }, callback) => {
        try {
            const router = await getOrCreateRouter(roomId)
            participants.set(socket.id, {
                roomId,
                router,
                transports: new Map(),
                producers: new Map(),
                consumers: new Map(),
            })
            socket.join(roomId)
            callback({ rtpCapabilities: router.rtpCapabilities })
        } catch (error) {
            callback({ error: error.message })
        }
    })
    
    socket.on('createTransport', async ({ type }, callback) => {
        try {
            const participant = participants.get(socket.id)
            if (!participant) return callback({ error: 'Participant not found' })
            
            const announcedIp = process.env.MEDIASOUP_ANNOUNCED_IP || getLocalIp()
            const transport = await participant.router.createWebRtcTransport({
                listenIps: [{ ip: '0.0.0.0', announcedIp }],
                enableUdp: true,
                enableTcp: true,
                preferUdp: true,
            })
            
            participant.transports.set(transport.id, transport)
            transport.on('dtlsstatechange', (state) => {
                if (state === 'closed') transport.close()
            })
            
            callback({
                id: transport.id,
                iceParameters: transport.iceParameters,
                iceCandidates: transport.iceCandidates,
                dtlsParameters: transport.dtlsParameters,
            })
        } catch (error) {
            callback({ error: error.message })
        }
    })
    
    socket.on('connectTransport', async ({ transportId, dtlsParameters }, callback) => {
        try {
            const participant = participants.get(socket.id)
            if (!participant) return callback({ error: 'Participant not found' })
            
            const transport = participant.transports.get(transportId)
            if (!transport) return callback({ error: 'Transport not found' })
            
            await transport.connect({ dtlsParameters })
            callback({ success: true })
        } catch (error) {
            callback({ error: error.message })
        }
    })
    
    socket.on('produce', async ({ transportId, kind, rtpParameters }, callback) => {
        try {
            const participant = participants.get(socket.id)
            if (!participant) return callback({ error: 'Participant not found' })
            
            const transport = participant.transports.get(transportId)
            if (!transport) return callback({ error: 'Transport not found' })
            
            const producer = await transport.produce({ kind, rtpParameters })
            participant.producers.set(producer.id, producer)
            
            socket.to(participant.roomId).emit('newProducer', {
                producerId: producer.id,
                kind: producer.kind,
                socketId: socket.id,
            })
            
            callback({ id: producer.id })
        } catch (error) {
            callback({ error: error.message })
        }
    })
    
    socket.on('consume', async ({ transportId, producerId, rtpCapabilities }, callback) => {
        try {
            const participant = participants.get(socket.id)
            if (!participant) return callback({ error: 'Participant not found' })
            
            let producer = null
            let producerSocketId = null
            for (const [socketId, p] of participants.entries()) {
                if (p.producers.has(producerId)) {
                    producer = p.producers.get(producerId)
                    producerSocketId = socketId
                    break
                }
            }
            
            if (!producer) return callback({ error: 'Producer not found' })
            if (producerSocketId === socket.id) return callback({ error: 'Cannot consume own producer' })
            if (!participant.router.canConsume({ producerId, rtpCapabilities })) {
                return callback({ error: 'Cannot consume this producer - RTP capabilities mismatch' })
            }
            
            const recvTransport = participant.transports.get(transportId)
            if (!recvTransport) return callback({ error: 'Receive transport not found' })
            
            // Optimized: Shorter wait with faster retries (max 300ms instead of 1000ms)
            let retries = 0
            while (recvTransport.connectionState !== 'connected' && 
                   recvTransport.connectionState !== 'completed' && 
                   retries < 6) {
                await new Promise(resolve => setTimeout(resolve, 50))
                retries++
            }
            
            // Proceed even if not fully connected - mediasoup will handle it
            const consumer = await recvTransport.consume({
                producerId,
                rtpCapabilities,
                paused: false,
            })
            
            participant.consumers.set(consumer.id, consumer)
            
            callback({
                id: consumer.id,
                producerId: consumer.producerId,
                kind: consumer.kind,
                rtpParameters: consumer.rtpParameters,
            })
        } catch (error) {
            callback({ error: error.message })
        }
    })
    
    socket.on('getProducers', (data, callback) => {
        try {
            const cb = typeof data === 'function' ? data : callback
            const participant = participants.get(socket.id)
            if (!participant) return cb({ error: 'Participant not found' })
            cb({ producers: getProducersForRoom(participant.roomId) })
        } catch (error) {
            const cb = typeof data === 'function' ? data : callback
            if (cb && typeof cb === 'function') cb({ error: error.message })
        }
    })
    
    socket.on('resumeConsumer', async ({ consumerId }, callback) => {
        try {
            const participant = participants.get(socket.id)
            if (!participant) return callback({ error: 'Participant not found' })
            
            const consumer = participant.consumers.get(consumerId)
            if (!consumer) return callback({ error: 'Consumer not found' })
            
            if (consumer.paused) await consumer.resume()
            callback({ success: true })
        } catch (error) {
            callback({ error: error.message })
        }
    })
    
    socket.on('leaveRoom', () => {
        const roomId = cleanupParticipant(socket.id)
        if (roomId) {
            socket.leave(roomId)
            socket.to(roomId).emit('participantLeft', { socketId: socket.id })
        }
    })
    
    socket.on('disconnect', () => {
        const roomId = cleanupParticipant(socket.id)
        if (roomId) {
            socket.to(roomId).emit('participantLeft', { socketId: socket.id })
        }
    })
})

async function startServer() {
    await createWorker()
    httpServer.listen(PORT, () => {
        console.log(`🚀 Server running on http://localhost:${PORT}`)
    })
}

startServer()