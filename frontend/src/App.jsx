import { useState, useEffect, useRef } from 'react'
import { io } from 'socket.io-client'
import * as mediasoupClient from 'mediasoup-client'
import './App.css'

const SERVER_URL = 'http://localhost:3000'

// Remote Video Component to handle stream updates properly
function RemoteVideo({ socketId, stream }) {
  const videoRef = useRef(null)

  useEffect(() => {
    const video = videoRef.current
    if (!video) return

    if (!stream) {
      console.log(`⚠️ No stream provided for ${socketId}`)
      video.srcObject = null
      return
    }

    console.log(`🎥 Setting stream for remote video ${socketId}`)
    console.log(`   Stream has ${stream.getTracks().length} tracks`)
    console.log(`   Video tracks: ${stream.getVideoTracks().length}`)
    console.log(`   Audio tracks: ${stream.getAudioTracks().length}`)
    
    // Log track details
    stream.getTracks().forEach(track => {
      console.log(`   - Track ${track.id}: ${track.kind}, enabled: ${track.enabled}, readyState: ${track.readyState}, muted: ${track.muted}`)
    })

    // Clear previous stream
    const previousStream = video.srcObject
    if (previousStream) {
      video.srcObject = null
    }
    
    // Wait for next frame to ensure DOM is ready
    requestAnimationFrame(() => {
      // Set new stream
      video.srcObject = stream
      console.log(`✅ Stream set for ${socketId}`)

      // Ensure video tracks are enabled
      stream.getVideoTracks().forEach(track => {
        track.enabled = true
        console.log(`   Video track ${track.id} enabled: ${track.enabled}`)
      })

      // Try to play
      const playPromise = video.play()
      if (playPromise !== undefined) {
        playPromise
          .then(() => {
            console.log(`✅ Video playing for ${socketId}`)
            console.log(`   Video dimensions: ${video.videoWidth}x${video.videoHeight}`)
          })
          .catch((error) => {
            console.warn(`⚠️ Play failed for ${socketId}:`, error.message)
            // Retry after a short delay
            setTimeout(() => {
              video.play().catch(e => console.warn('Retry play failed:', e))
            }, 500)
          })
      }
    })

    // Monitor video element events
    const onLoadedMetadata = () => {
      console.log(`📹 Metadata loaded for ${socketId}: ${video.videoWidth}x${video.videoHeight}`)
      if (video.paused) {
        video.play().catch(e => console.warn('Play after metadata failed:', e))
      }
    }

    const onPlaying = () => {
      console.log(`▶️ Video playing for ${socketId}, dimensions: ${video.videoWidth}x${video.videoHeight}`)
    }

    const onPause = () => {
      console.log(`⏸️ Video paused for ${socketId}`)
    }

    const onError = (e) => {
      console.error(`❌ Video error for ${socketId}:`, e)
      console.error(`   Error details:`, video.error)
    }

    const onStalled = () => {
      console.warn(`⚠️ Video stalled for ${socketId}`)
    }

    const onWaiting = () => {
      console.warn(`⏳ Video waiting for ${socketId}`)
    }

    // Monitor stream track events
    const trackEndedHandler = (event) => {
      console.warn(`⚠️ Track ended for ${socketId}:`, event.target.kind)
    }

    stream.getTracks().forEach(track => {
      track.addEventListener('ended', trackEndedHandler)
    })

    video.addEventListener('loadedmetadata', onLoadedMetadata)
    video.addEventListener('playing', onPlaying)
    video.addEventListener('pause', onPause)
    video.addEventListener('error', onError)
    video.addEventListener('stalled', onStalled)
    video.addEventListener('waiting', onWaiting)

    return () => {
      video.removeEventListener('loadedmetadata', onLoadedMetadata)
      video.removeEventListener('playing', onPlaying)
      video.removeEventListener('pause', onPause)
      video.removeEventListener('error', onError)
      video.removeEventListener('stalled', onStalled)
      video.removeEventListener('waiting', onWaiting)
      
      stream.getTracks().forEach(track => {
        track.removeEventListener('ended', trackEndedHandler)
      })
      
      if (video.srcObject) {
        video.srcObject = null
      }
    }
  }, [stream, socketId])

  // Check if stream has video tracks
  const hasVideo = stream && stream.getVideoTracks().length > 0
  const videoTracks = stream ? stream.getVideoTracks() : []
  const allTracksEnabled = videoTracks.every(track => track.enabled && track.readyState === 'live')

  return (
    <div className="video-container" id={`remote-${socketId}`}>
      <video
        ref={videoRef}
        autoPlay
        playsInline
        muted={false}
        style={{
          width: '100%',
          height: '100%',
          objectFit: 'cover',
          display: 'block',
          backgroundColor: '#000'
        }}
      />
      <div className="video-label">
        Participant
        {!hasVideo && <span style={{ fontSize: '10px', display: 'block' }}>No video</span>}
        {hasVideo && !allTracksEnabled && <span style={{ fontSize: '10px', display: 'block' }}>Connecting...</span>}
      </div>
    </div>
  )
}

function App() {
  const [status, setStatus] = useState('disconnected')
  const [username, setUsername] = useState('User')
  const [roomId, setRoomId] = useState('room-1')
  const [users, setUsers] = useState([])
  const [error, setError] = useState('')
  const [isAudioEnabled, setIsAudioEnabled] = useState(true)
  const [isVideoEnabled, setIsVideoEnabled] = useState(true)
  const [inCall, setInCall] = useState(false)
  const [remoteVideos, setRemoteVideos] = useState(new Map())

  // Refs for mediasoup
  const socketRef = useRef(null)
  const deviceRef = useRef(null)
  const sendTransportRef = useRef(null)
  const recvTransportRef = useRef(null)
  const producersRef = useRef(new Map())
  const consumersRef = useRef(new Map())
  const producerToSocketRef = useRef(new Map())
  const socketToStreamRef = useRef(new Map())
  const localStreamRef = useRef(null)
  const currentRoomIdRef = useRef(null)

  // Refs for video elements
  const localVideoRef = useRef(null)
  const remoteVideosRef = useRef(new Map())

  // Load users on mount
  useEffect(() => {
    loadUsers()
  }, [])

  const loadUsers = async () => {
    try {
      const response = await fetch(`${SERVER_URL}/users`)
      const data = await response.json()
      setUsers(data)
    } catch (error) {
      console.error('Error loading users:', error)
    }
  }

  const socketRequest = (event, data = {}) => {
    return new Promise((resolve, reject) => {
      if (!socketRef.current) {
        reject(new Error('Socket not connected'))
        return
      }

      socketRef.current.emit(event, data, (response) => {
        if (response.error) {
          reject(new Error(response.error))
        } else {
          resolve(response)
        }
      })
    })
  }

  const connect = async () => {
    try {
      if (!username || !roomId) {
        setError('Please enter your name and room ID')
        return
      }

      // Connect to Socket.IO
      socketRef.current = io(SERVER_URL)
      setStatus('connecting')

      socketRef.current.on('connect', async () => {
        console.log('Socket connected')
        setStatus('connected')
        await joinRoom(roomId)
      })

      socketRef.current.on('disconnect', () => {
        console.log('Socket disconnected')
        setStatus('disconnected')
      })

      socketRef.current.on('newProducer', async ({ producerId, kind, socketId }) => {
        console.log('🔔 New producer event received:', { producerId, kind, socketId })
        
        if (socketId === socketRef.current.id) {
          console.log('⚠️ Ignoring own producer')
          return
        }

        // Wait for recv transport and device to be ready
        let retries = 0
        while ((!recvTransportRef.current || !deviceRef.current?.loaded) && retries < 30) {
          await new Promise(resolve => setTimeout(resolve, 200))
          retries++
        }

        if (!recvTransportRef.current) {
          console.error(`❌ Recv transport not ready after waiting, cannot consume producer ${producerId}`)
          return
        }

        if (!deviceRef.current?.loaded) {
          console.error(`❌ Device not loaded after waiting, cannot consume producer ${producerId}`)
          return
        }

        console.log(`📥 Consuming new producer: ${producerId} (${kind}) from ${socketId}`)
        producerToSocketRef.current.set(producerId, socketId)
        
        try {
          await consumeProducer(producerId, kind)
        } catch (error) {
          console.error(`❌ Failed to consume new producer ${producerId}:`, error)
        }
      })

      socketRef.current.on('participantLeft', ({ socketId }) => {
        console.log('Participant left:', socketId)
        removeParticipant(socketId)
      })

    } catch (error) {
      console.error('Connection error:', error)
      setError('Failed to connect: ' + error.message)
    }
  }

  const joinRoom = async (roomId) => {
    try {
      currentRoomIdRef.current = roomId
      setStatus('in-call')
      setInCall(true)

      // Create or join room
      const { rtpCapabilities } = await socketRequest('createRoom', { roomId })
      console.log('Room joined, RTP capabilities:', rtpCapabilities)

      // Create mediasoup device
      deviceRef.current = new mediasoupClient.Device()
      await deviceRef.current.load({ routerRtpCapabilities: rtpCapabilities })
      console.log('Device loaded')

      // Create transports
      await createSendTransport()
      await createRecvTransport()
      
      // Wait for transports to be ready
      await new Promise(resolve => setTimeout(resolve, 800))

      // Start producing local media
      await produceMedia()
      
      // Wait for producers to be registered
      await new Promise(resolve => setTimeout(resolve, 500))

      // Get existing producers
      const { producers: existingProducers } = await socketRequest('getProducers')
      console.log('📋 Existing producers in room:', existingProducers)

      // Consume existing producers
      if (recvTransportRef.current && deviceRef.current?.loaded) {
        for (const producer of existingProducers) {
          if (producer.socketId === socketRef.current.id) {
            console.log(`⚠️ Skipping own producer ${producer.id}`)
            continue
          }
          
          console.log(`📥 Consuming existing producer: ${producer.id} (${producer.kind}) from ${producer.socketId}`)
          producerToSocketRef.current.set(producer.id, producer.socketId)
          
          try {
            await consumeProducer(producer.id, producer.kind)
            // Small delay between consumers
            await new Promise(resolve => setTimeout(resolve, 200))
          } catch (error) {
            console.error(`❌ Failed to consume producer ${producer.id}:`, error)
          }
        }
      } else {
        console.warn('⚠️ Cannot consume producers: recvTransport or device not ready')
      }

      loadUsers()

    } catch (error) {
      console.error('Join room error:', error)
      setError('Failed to join room: ' + error.message)
    }
  }

  const createSendTransport = async () => {
    const { id, iceParameters, iceCandidates, dtlsParameters } = await socketRequest('createTransport', { type: 'send' })

    sendTransportRef.current = deviceRef.current.createSendTransport({
      id,
      iceParameters,
      iceCandidates,
      dtlsParameters,
    })

    sendTransportRef.current.on('connect', async ({ dtlsParameters }, callback, errback) => {
      try {
        console.log('Connecting send transport...')
        await socketRequest('connectTransport', {
          transportId: sendTransportRef.current.id,
          dtlsParameters,
        })
        console.log('✅ Send transport connected')
        callback()
      } catch (error) {
        console.error('❌ Send transport connection error:', error)
        errback(error)
      }
    })

    sendTransportRef.current.on('produce', async ({ kind, rtpParameters }, callback, errback) => {
      try {
        console.log(`Producing ${kind}...`)
        const { id } = await socketRequest('produce', {
          transportId: sendTransportRef.current.id,
          kind,
          rtpParameters,
        })
        console.log(`✅ Producer created: ${id} (${kind})`)
        callback({ id })
      } catch (error) {
        console.error('❌ Produce error:', error)
        errback(error)
      }
    })
  }

  const createRecvTransport = async () => {
    const { id, iceParameters, iceCandidates, dtlsParameters } = await socketRequest('createTransport', { type: 'recv' })

    recvTransportRef.current = deviceRef.current.createRecvTransport({
      id,
      iceParameters,
      iceCandidates,
      dtlsParameters,
    })

    recvTransportRef.current.on('connect', async ({ dtlsParameters }, callback, errback) => {
      try {
        console.log('Connecting recv transport...')
        await socketRequest('connectTransport', {
          transportId: recvTransportRef.current.id,
          dtlsParameters,
        })
        console.log('✅ Recv transport connected, state:', recvTransportRef.current.connectionState)
        callback()
      } catch (error) {
        console.error('❌ Recv transport connection error:', error)
        errback(error)
      }
    })

    recvTransportRef.current.on('connectionstatechange', (state) => {
      console.log(`📡 Recv transport connection state: ${state}`)
    })
  }

  const produceMedia = async () => {
    try {
      const stream = await navigator.mediaDevices.getUserMedia({
        audio: true,
        video: { width: 1280, height: 720 },
      })

      localStreamRef.current = stream
      
      // Display local video
      if (localVideoRef.current) {
        localVideoRef.current.srcObject = stream
      }

      // Produce audio
      const audioTrack = stream.getAudioTracks()[0]
      if (audioTrack) {
        const audioProducer = await sendTransportRef.current.produce({ track: audioTrack })
        producersRef.current.set('audio', audioProducer)
        producersRef.current.set(audioProducer.id, audioProducer)
        console.log(`✅ Audio producer created: ${audioProducer.id}`)
      }

      // Produce video
      const videoTrack = stream.getVideoTracks()[0]
      if (videoTrack) {
        const videoProducer = await sendTransportRef.current.produce({ track: videoTrack })
        producersRef.current.set('video', videoProducer)
        producersRef.current.set(videoProducer.id, videoProducer)
        console.log(`✅ Video producer created: ${videoProducer.id}`)
      }

    } catch (error) {
      console.error('Error producing media:', error)
      setError('Failed to access camera/microphone: ' + error.message)
    }
  }

  const consumeProducer = async (producerId, kind = null) => {
    try {
      if (consumersRef.current.has(producerId)) {
        console.log(`⚠️ Already consuming producer ${producerId}, skipping`)
        return
      }

      if (!recvTransportRef.current) {
        throw new Error('Recv transport not ready')
      }

      if (!deviceRef.current?.loaded) {
        throw new Error('Device not loaded')
      }

      const socketId = producerToSocketRef.current.get(producerId)
      if (!socketId) {
        console.warn(`⚠️ No socketId found for producer ${producerId}`)
      }

      if (socketId === socketRef.current.id) {
        console.log(`⚠️ Skipping own producer ${producerId}`)
        return
      }

      // Wait for transport to be connected
      let retries = 0
      while (
        recvTransportRef.current.connectionState !== 'connected' &&
        recvTransportRef.current.connectionState !== 'completed' &&
        retries < 20
      ) {
        await new Promise(resolve => setTimeout(resolve, 100))
        retries++
      }

      if (recvTransportRef.current.connectionState !== 'connected' && 
          recvTransportRef.current.connectionState !== 'completed') {
        console.warn(`⚠️ Recv transport not fully connected (${recvTransportRef.current.connectionState}), proceeding anyway...`)
      }

      console.log(`📥 Consuming producer ${producerId} (${kind}) from socket ${socketId}...`)

      // Request consumer creation from server
      const { id, producerId: prodId, kind: mediaKind, rtpParameters } = await socketRequest('consume', {
        transportId: recvTransportRef.current.id,
        producerId,
        rtpCapabilities: deviceRef.current.rtpCapabilities,
      })

      console.log(`✅ Server created consumer ${id} for producer ${prodId} (${mediaKind})`)

      // Create consumer locally
      const consumer = await recvTransportRef.current.consume({
        id,
        producerId: prodId,
        kind: mediaKind,
        rtpParameters,
      })

      console.log(`✅ Consumer created locally: ${consumer.id}, track ID: ${consumer.track.id}, kind: ${consumer.kind}`)
      console.log(`   Track enabled: ${consumer.track.enabled}, readyState: ${consumer.track.readyState}`)

      consumersRef.current.set(producerId, consumer)

      // Ensure track is enabled
      consumer.track.enabled = true

      // Resume consumer on server
      try {
        await socketRequest('resumeConsumer', { consumerId: consumer.id })
        console.log(`✅ Consumer ${id} resumed on server`)
      } catch (error) {
        console.warn(`⚠️ Failed to resume consumer on server:`, error)
      }

      // Wait a bit for video tracks
      if (mediaKind === 'video') {
        await new Promise(resolve => setTimeout(resolve, 300))
      }

      // Add track to video element
      await addRemoteVideo(consumer.track, producerId, mediaKind, socketId)

    } catch (error) {
      console.error(`❌ Error consuming producer ${producerId}:`, error)
      throw error
    }
  }

  const addRemoteVideo = async (track, producerId, kind, socketId) => {
    console.log(`🎬 addRemoteVideo called: producerId=${producerId}, kind=${kind}, socketId=${socketId}`)

    if (!socketId) {
      console.error(`❌ No socketId provided for producer ${producerId}`)
      return
    }

    // Ensure track is enabled
    track.enabled = true
    
    // Monitor track events
    track.onended = () => {
      console.log(`⚠️ Track ${track.id} ended`)
    }
    
    track.onmute = () => {
      console.log(`⚠️ Track ${track.id} muted`)
    }
    
    track.onunmute = () => {
      console.log(`✅ Track ${track.id} unmuted`)
    }

    // Get or create stream for this socket
    let stream = socketToStreamRef.current.get(socketId)
    
    if (!stream) {
      // Create new stream
      stream = new MediaStream()
      stream.addTrack(track)
      socketToStreamRef.current.set(socketId, stream)
      console.log(`📦 Created NEW stream for socket ${socketId} with ${kind} track`)
    } else {
      // Check if track already exists
      const existingTrack = stream.getTracks().find(t => t.id === track.id)
      if (!existingTrack) {
        // Always create a new stream reference to force React update
        const newStream = new MediaStream()
        // Copy existing tracks
        stream.getTracks().forEach(t => {
          if (t.readyState === 'live') {
            newStream.addTrack(t)
          }
        })
        // Add new track
        newStream.addTrack(track)
        stream = newStream
        socketToStreamRef.current.set(socketId, stream)
        console.log(`✅ Updated stream for ${socketId} with ${stream.getTracks().length} tracks (added ${kind})`)
      } else {
        console.log(`⚠️ Track ${track.id} already in stream, skipping`)
        return // Don't update if track already exists
      }
    }

    console.log(`✅ Track enabled: ${track.enabled}, readyState: ${track.readyState}, muted: ${track.muted}`)

    // Verify track is in stream
    const trackInStream = stream.getTracks().find(t => t.id === track.id)
    if (!trackInStream) {
      console.error(`❌ Track ${track.id} was not added to stream!`)
      return
    }

    console.log(`📊 Stream info for ${socketId}:`)
    console.log(`   Total tracks: ${stream.getTracks().length}`)
    console.log(`   Video tracks: ${stream.getVideoTracks().length}`)
    console.log(`   Audio tracks: ${stream.getAudioTracks().length}`)
    stream.getTracks().forEach(t => {
      console.log(`   - Track ${t.id}: ${t.kind}, enabled: ${t.enabled}, readyState: ${t.readyState}`)
    })

    // Update state to trigger re-render with new stream reference
    setRemoteVideos(prev => {
      const newMap = new Map(prev)
      newMap.set(socketId, stream) // This will trigger useEffect in RemoteVideo component
      return newMap
    })

    // Wait a bit for React to render
    await new Promise(resolve => setTimeout(resolve, 150))
  }

  const removeParticipant = (socketId) => {
    // Remove consumers
    for (const [producerId, sId] of producerToSocketRef.current.entries()) {
      if (sId === socketId) {
        const consumer = consumersRef.current.get(producerId)
        if (consumer) {
          consumer.close()
          consumersRef.current.delete(producerId)
        }
        producerToSocketRef.current.delete(producerId)
      }
    }

    socketToStreamRef.current.delete(socketId)
    remoteVideosRef.current.delete(socketId)
    setRemoteVideos(prev => {
      const newMap = new Map(prev)
      newMap.delete(socketId)
      return newMap
    })
  }

  const toggleAudio = () => {
    if (localStreamRef.current) {
      const audioTrack = localStreamRef.current.getAudioTracks()[0]
      if (audioTrack) {
        setIsAudioEnabled(!isAudioEnabled)
        audioTrack.enabled = isAudioEnabled
      }
    }
  }

  const toggleVideo = () => {
    if (localStreamRef.current) {
      const videoTrack = localStreamRef.current.getVideoTracks()[0]
      if (videoTrack) {
        setIsVideoEnabled(!isVideoEnabled)
        videoTrack.enabled = isVideoEnabled
      }
    }
  }

  const leaveRoom = async () => {
    try {
      // Stop local stream
      if (localStreamRef.current) {
        localStreamRef.current.getTracks().forEach(track => track.stop())
      }

      // Close producers
      for (const producer of producersRef.current.values()) {
        producer.close()
      }
      producersRef.current.clear()

      // Close consumers
      for (const consumer of consumersRef.current.values()) {
        consumer.close()
      }
      consumersRef.current.clear()
      producerToSocketRef.current.clear()
      socketToStreamRef.current.clear()
      remoteVideosRef.current.clear()

      // Close transports
      if (sendTransportRef.current) {
        sendTransportRef.current.close()
      }
      if (recvTransportRef.current) {
        recvTransportRef.current.close()
      }

      // Leave room
      if (socketRef.current) {
        socketRef.current.emit('leaveRoom')
        socketRef.current.disconnect()
        socketRef.current = null
      }

      setInCall(false)
      setStatus('disconnected')
      setRemoteVideos(new Map())

    } catch (error) {
      console.error('Error leaving room:', error)
    }
  }

  return (
    <div className="container">
      <div className="header">
        <h1>🎥 Professional Video Meeting</h1>
        <div className={`status ${status}`}>
          {status === 'disconnected' && 'Disconnected'}
          {status === 'connecting' && 'Connecting...'}
          {status === 'connected' && 'Connected'}
          {status === 'in-call' && 'In Call'}
        </div>
      </div>

      {!inCall && (
        <div className="setup-section">
          <h2>Setup</h2>
          <div className="form-group">
            <label htmlFor="username">Your Name</label>
            <input
              type="text"
              id="username"
              value={username}
              onChange={(e) => setUsername(e.target.value)}
              placeholder="Enter your name"
            />
          </div>
          <div className="form-group">
            <label htmlFor="roomId">Room ID</label>
            <input
              type="text"
              id="roomId"
              value={roomId}
              onChange={(e) => setRoomId(e.target.value)}
              placeholder="Enter room ID"
            />
          </div>
          <button className="btn btn-primary" onClick={connect}>
            Connect & Join Room
          </button>
          {error && <div className="error show">{error}</div>}
        </div>
      )}

      <div className="users-list" style={{ display: inCall ? 'block' : 'none' }}>
        <h3>Users in Database</h3>
        <div className="users-container">
          {users.length === 0 ? (
            <div className="loading">No users found</div>
          ) : (
            users.map((user, index) => (
              <div key={index} className="user-item">
                <div>
                  <strong>{user.username || 'Unknown'}</strong>
                  <div style={{ fontSize: '12px', color: '#6b7280' }}>{user.email || ''}</div>
                </div>
              </div>
            ))
          )}
        </div>
        <button className="btn btn-secondary" onClick={loadUsers}>
          Refresh Users
        </button>
      </div>

      {inCall && (
        <div className="video-section active">
          <h2 style={{ color: '#667eea', marginBottom: '20px' }}>Video Call</h2>
          <div className="video-grid">
            {/* Local video */}
            <div className="video-container" id="local-video">
              <video
                ref={localVideoRef}
                autoPlay
                playsInline
                muted
                style={{
                  width: '100%',
                  height: '100%',
                  objectFit: 'cover',
                  display: 'block',
                  backgroundColor: '#000'
                }}
              />
              <div className="video-label">You (Local)</div>
            </div>

            {/* Remote videos */}
            {Array.from(remoteVideos.entries()).map(([socketId, stream]) => (
              <RemoteVideo
                key={socketId}
                socketId={socketId}
                stream={stream}
              />
            ))}
          </div>
          <div className="controls-bar">
            <button className="btn btn-primary" onClick={toggleAudio}>
              {isAudioEnabled ? '🔊 Unmute' : '🔇 Mute'}
            </button>
            <button className="btn btn-primary" onClick={toggleVideo}>
              {isVideoEnabled ? '📹 Video On' : '📷 Video Off'}
            </button>
            <button className="btn btn-danger" onClick={leaveRoom}>
              Leave Call
            </button>
          </div>
        </div>
      )}
    </div>
  )
}

export default App
