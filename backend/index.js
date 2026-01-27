const express = require('express')
const { Pool } = require('pg')
const { Server } = require('socket.io')
const mediasoup = require('mediasoup')

const app = express()
const PORT = process.env.PORT || 3000

// Database configuration
const databaseUrl = 'postgresql://neondb_owner:npg_5lBiKSDN2ZnT@ep-empty-wind-a1vittzf-pooler.ap-southeast-1.aws.neon.tech/neondb?sslmode=require&channel_binding=require'

const pool = new Pool({
    connectionString: databaseUrl
})

// Enable CORS for all routes
app.use((req, res, next) => {
    res.header('Access-Control-Allow-Origin', '*')
    res.header('Access-Control-Allow-Methods', 'GET, POST, PUT, DELETE, OPTIONS')
    res.header('Access-Control-Allow-Headers', 'Origin, X-Requested-With, Content-Type, Accept')
    if (req.method === 'OPTIONS') {
        return res.sendStatus(200)
    }
    next()
})

app.use(express.json())
app.use(express.urlencoded({ extended: true }))

pool.connect((err, client, release) => {
    if (err) {
        return console.error('Error acquiring client', err.stack)
    }
    console.log('✅ Database connected successfully')
    release()
})

//----------------Logic------------------

// create user {userName, email}
app.post('/users', async (req, res) => {
    const { userName, email } = req.body
    try {
        const result = await pool.query('INSERT INTO users (username, email) VALUES ($1, $2) RETURNING *', [userName, email])
        res.json(result.rows[0])
    } catch (error) {
        res.status(500).json({ error: error.message })
    }
})


// get all users
app.get('/users', async (req, res) => {
    try {
        const result = await pool.query('SELECT * FROM users')
        res.json(result.rows)
    } catch (error) {
        res.status(500).json({ error: error.message })
    }
})

//------------------mediasoup For Group Call----------------------

// Mediasoup worker
let worker = null

// Store rooms: { roomId: router }
const rooms = new Map()

// Store participants: { socketId: { roomId, router, transports, producers, consumers } }
const participants = new Map()

// Get local IP address
function getLocalIp() {
    const os = require('os')
    const interfaces = os.networkInterfaces()
    for (const name of Object.keys(interfaces)) {
        for (const iface of interfaces[name]) {
            if (iface.family === 'IPv4' && !iface.internal) {
                return iface.address
            }
        }
    }
    return '127.0.0.1' // Fallback to localhost
}

// Initialize mediasoup worker
async function createWorker() {
    worker = await mediasoup.createWorker({
        logLevel: 'warn',
        rtcMinPort: 40000,
        rtcMaxPort: 49999,
    })
    
    worker.on('died', () => {
        console.error('Mediasoup worker died, exiting...')
        process.exit(1)
    })
    
    console.log('✅ Mediasoup worker created')
    return worker
}

// Create or get room router
async function getOrCreateRouter(roomId) {
    let router = rooms.get(roomId)
    
    if (!router) {
        router = await worker.createRouter({
            mediaCodecs: [
                {
                    kind: 'audio',
                    mimeType: 'audio/opus',
                    clockRate: 48000,
                    channels: 2,
                },
                {
                    kind: 'video',
                    mimeType: 'video/VP8',
                    clockRate: 90000,
                },
            ],
        })
        rooms.set(roomId, router)
        console.log(`✅ Room created: ${roomId}`)
    }
    
    return router
}

// Get participant's producers in a room
function getProducersForRoom(roomId) {
    const producers = []
    for (const [socketId, participant] of participants.entries()) {
        if (participant.roomId === roomId) {
            for (const producer of participant.producers.values()) {
                producers.push({
                    id: producer.id,
                    kind: producer.kind,
                    socketId: socketId,
                })
            }
        }
    }
    return producers
}

// Create HTTP server and Socket.IO
const httpServer = require('http').createServer(app)
const io = new Server(httpServer, {
    cors: {
        origin: '*',
        methods: ['GET', 'POST'],
    },
})

// Socket.IO connection handler
io.on('connection', (socket) => {
    console.log(`✅ Client connected: ${socket.id}`)
    
    // Create or join room
    socket.on('createRoom', async ({ roomId }, callback) => {
        try {
            const router = await getOrCreateRouter(roomId)
            const rtpCapabilities = router.rtpCapabilities
            
            participants.set(socket.id, {
                roomId,
                router,
                transports: new Map(),
                producers: new Map(),
                consumers: new Map(),
            })
            
            socket.join(roomId)
            callback({ rtpCapabilities })
            console.log(`✅ Socket ${socket.id} created/joined room: ${roomId}`)
        } catch (error) {
            console.error('Error creating room:', error)
            callback({ error: error.message })
        }
    })
    
    // Create transport (send or recv)
    socket.on('createTransport', async ({ type }, callback) => {
        try {
            const participant = participants.get(socket.id)
            if (!participant) {
                return callback({ error: 'Participant not found' })
            }
            
            const { router } = participant
            const localIp = getLocalIp()
            
            // Use local IP for development, or detect public IP for production
            const announcedIp = process.env.MEDIASOUP_ANNOUNCED_IP || localIp
            
            const transport = await router.createWebRtcTransport({
                listenIps: [{ ip: '0.0.0.0', announcedIp }],
                enableUdp: true,
                enableTcp: true,
                preferUdp: true,
            })
            
            participant.transports.set(transport.id, transport)
            
            transport.on('dtlsstatechange', (dtlsState) => {
                console.log(`Transport ${transport.id} DTLS state: ${dtlsState}`)
                if (dtlsState === 'closed') {
                    transport.close()
                }
            })
            
            transport.on('icestatechange', (iceState) => {
                console.log(`Transport ${transport.id} ICE state: ${iceState}`)
            })
            
            console.log(`✅ Transport created: ${transport.id} (${type}), announcedIp: ${announcedIp}`)
            
            callback({
                id: transport.id,
                iceParameters: transport.iceParameters,
                iceCandidates: transport.iceCandidates,
                dtlsParameters: transport.dtlsParameters,
            })
        } catch (error) {
            console.error('Error creating transport:', error)
            callback({ error: error.message })
        }
    })
    
    // Connect transport
    socket.on('connectTransport', async ({ transportId, dtlsParameters }, callback) => {
        try {
            const participant = participants.get(socket.id)
            if (!participant) {
                return callback({ error: 'Participant not found' })
            }
            
            const transport = participant.transports.get(transportId)
            if (!transport) {
                return callback({ error: 'Transport not found' })
            }
            
            await transport.connect({ dtlsParameters })
            console.log(`✅ Transport ${transportId} connected for ${socket.id}, state: ${transport.connectionState}`)
            callback({ success: true })
        } catch (error) {
            console.error('Error connecting transport:', error)
            callback({ error: error.message })
        }
    })
    
    // Produce audio/video
    socket.on('produce', async ({ transportId, kind, rtpParameters }, callback) => {
        try {
            const participant = participants.get(socket.id)
            if (!participant) {
                return callback({ error: 'Participant not found' })
            }
            
            const transport = participant.transports.get(transportId)
            if (!transport) {
                return callback({ error: 'Transport not found' })
            }
            
            const producer = await transport.produce({ kind, rtpParameters })
            participant.producers.set(producer.id, producer)
            
            // Notify other participants in the room (exclude sender)
            socket.to(participant.roomId).emit('newProducer', {
                producerId: producer.id,
                kind: producer.kind,
                socketId: socket.id,
            })
            
            console.log(`✅ Producer created: ${producer.id} (${kind}) for ${socket.id}, notifying room ${participant.roomId}`)
            
            callback({ id: producer.id })
        } catch (error) {
            console.error('Error producing:', error)
            callback({ error: error.message })
        }
    })
    
    // Consume producer from another participant
    socket.on('consume', async ({ transportId, producerId, rtpCapabilities }, callback) => {
        try {
            const participant = participants.get(socket.id)
            if (!participant) {
                return callback({ error: 'Participant not found' })
            }
            
            const { router } = participant
            
            // Find the producer
            let producer = null
            let producerSocketId = null
            for (const [socketId, p] of participants.entries()) {
                if (p.producers.has(producerId)) {
                    producer = p.producers.get(producerId)
                    producerSocketId = socketId
                    break
                }
            }
            
            if (!producer) {
                return callback({ error: 'Producer not found' })
            }
            
            // Don't consume own producer
            if (producerSocketId === socket.id) {
                return callback({ error: 'Cannot consume own producer' })
            }
            
            // Check if router can consume this producer
            if (!router.canConsume({ producerId, rtpCapabilities })) {
                return callback({ error: 'Cannot consume this producer - RTP capabilities mismatch' })
            }
            
            // Get recv transport - must exist, created by frontend
            const recvTransport = participant.transports.get(transportId)
            if (!recvTransport) {
                return callback({ error: 'Receive transport not found. Please create it first.' })
            }
            
            // Wait for transport to be connected (with timeout)
            let retries = 0
            while (recvTransport.connectionState !== 'connected' && recvTransport.connectionState !== 'completed' && retries < 10) {
                await new Promise(resolve => setTimeout(resolve, 100))
                retries++
            }
            
            if (recvTransport.connectionState !== 'connected' && recvTransport.connectionState !== 'completed') {
                console.warn(`⚠️ Recv transport state: ${recvTransport.connectionState}, proceeding anyway...`)
            }
            
            // Create consumer on the recv transport
            const consumer = await recvTransport.consume({
                producerId,
                rtpCapabilities,
                paused: false, // Create unpaused
            })
            
            participant.consumers.set(consumer.id, consumer)
            
            console.log(`✅ Consumer created: ${consumer.id} (${consumer.kind}) for ${socket.id}, producer: ${producerId}, paused: ${consumer.paused}`)
            
            callback({
                id: consumer.id,
                producerId: consumer.producerId,
                kind: consumer.kind,
                rtpParameters: consumer.rtpParameters,
            })
        } catch (error) {
            console.error('Error consuming:', error)
            callback({ error: error.message })
        }
    })
    
    // Get existing producers in room (when joining)
    socket.on('getProducers', (data, callback) => {
        try {
            // Handle both cases: with data and without data
            const cb = typeof data === 'function' ? data : callback
            
            if (!cb || typeof cb !== 'function') {
                console.error('getProducers: callback is not a function')
                return
            }
            
            const participant = participants.get(socket.id)
            if (!participant) {
                return cb({ error: 'Participant not found' })
            }
            
            const producers = getProducersForRoom(participant.roomId)
            cb({ producers })
        } catch (error) {
            console.error('Error getting producers:', error)
            const cb = typeof data === 'function' ? data : callback
            if (cb && typeof cb === 'function') {
                cb({ error: error.message })
            }
        }
    })
    
    // Resume consumer
    socket.on('resumeConsumer', async ({ consumerId }, callback) => {
        try {
            const participant = participants.get(socket.id)
            if (!participant) {
                return callback({ error: 'Participant not found' })
            }
            
            const consumer = participant.consumers.get(consumerId)
            if (!consumer) {
                return callback({ error: 'Consumer not found' })
            }
            
            if (consumer.paused) {
                await consumer.resume()
                console.log(`✅ Consumer ${consumerId} resumed`)
            } else {
                console.log(`ℹ️ Consumer ${consumerId} already active`)
            }
            
            callback({ success: true })
        } catch (error) {
            console.error('Error resuming consumer:', error)
            callback({ error: error.message })
        }
    })
    
    // Leave room
    socket.on('leaveRoom', () => {
        const participant = participants.get(socket.id)
        if (participant) {
            // Close all transports
            for (const transport of participant.transports.values()) {
                transport.close()
            }
            
            // Remove participant
            participants.delete(socket.id)
            
            // Clean up room if empty
            const roomId = participant.roomId
            const roomParticipants = Array.from(participants.values()).filter(
                (p) => p.roomId === roomId
            )
            
            if (roomParticipants.length === 0) {
                const router = rooms.get(roomId)
                if (router) {
                    router.close()
                    rooms.delete(roomId)
                    console.log(`✅ Room closed: ${roomId}`)
                }
            }
            
            socket.leave(roomId)
            socket.to(roomId).emit('participantLeft', { socketId: socket.id })
            console.log(`✅ Socket ${socket.id} left room: ${roomId}`)
        }
    })
    
    // Handle disconnect
    socket.on('disconnect', () => {
        const participant = participants.get(socket.id)
        if (participant) {
            // Close all transports
            for (const transport of participant.transports.values()) {
                transport.close()
            }
            
            // Remove participant
            participants.delete(socket.id)
            
            // Clean up room if empty
            const roomId = participant.roomId
            const roomParticipants = Array.from(participants.values()).filter(
                (p) => p.roomId === roomId
            )
            
            if (roomParticipants.length === 0) {
                const router = rooms.get(roomId)
                if (router) {
                    router.close()
                    rooms.delete(roomId)
                    console.log(`✅ Room closed: ${roomId}`)
                }
            }
            
            socket.to(roomId).emit('participantLeft', { socketId: socket.id })
            console.log(`✅ Socket ${socket.id} disconnected`)
        }
    })
})
 
//---------------------------------------------------------------
// Start server
async function startServer() {
    await createWorker()
    httpServer.listen(PORT, () => {
    console.log(`🚀 Server is running on http://localhost:${PORT}`)
})
}

startServer()