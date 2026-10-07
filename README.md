# Real-Time Location Tracker

[![License](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)
[![Node.js](https://img.shields.io/badge/Node.js-18+-green.svg)](https://nodejs.org)
[![Status](https://img.shields.io/badge/status-active-brightgreen.svg)](#)

A production-ready real-time location sharing platform built with Node.js, Express, Socket.IO, Apache Kafka, and Leaflet. The application demonstrates scalable event-driven architecture for handling distributed location updates with sub-second latency.

## 🎯 Overview

This project implements a real-time geolocation pipeline with event streaming capabilities:

- **Client Side**: Browser-based geolocation capture with live map visualization
- **Server Side**: Express.js + Socket.IO for real-time communication
- **Event Streaming**: Apache Kafka for asynchronous location event processing
- **Scalability**: Kafka consumers can horizontally scale to handle millions of location updates
- **Persistence Layer**: Database processor ready for location data storage

**Use Cases:**
- Real-time fleet tracking systems
- Location-based social networks
- Emergency response coordination
- Ride-sharing applications
- IoT device location monitoring

## ✨ Features

- ⚡ **Real-Time Updates** - Sub-second location broadcasting via Socket.IO
- 📍 **Browser Geolocation** - Native geolocation API with high accuracy mode
- 📊 **Kafka Event Streaming** - Scalable event-driven architecture
- 🗺️ **Interactive Map UI** - Live marker updates using Leaflet + OpenStreetMap
- 🔄 **Auto-Refresh** - Location updates every 10 seconds
- 🏥 **Health Monitoring** - Built-in health check endpoint
- 🐳 **Docker Support** - Pre-configured Kafka setup via Docker Compose
- 📈 **Event Processing** - Sample consumer for analytics and storage

## 🛠️ Tech Stack

| Component | Technology | Purpose |
|-----------|-----------|---------|
| **Runtime** | Node.js 18+ | Server-side JavaScript runtime |
| **Web Framework** | Express.js | HTTP server and routing |
| **Real-Time Communication** | Socket.IO | Bidirectional event streaming |
| **Event Broker** | Apache Kafka 4.2.0 | Distributed message queue |
| **Kafka Client** | KafkaJS | JavaScript Kafka producer/consumer |
| **Frontend Map** | Leaflet.js | Interactive map library |
| **Map Tiles** | OpenStreetMap | Free map data provider |
| **Infrastructure** | Docker Compose | Container orchestration |

## 📁 Project Structure

```
Real-Time-Location-Tracker/
├── index.js                 # Main server: Express + Socket.IO + Kafka producer/consumer
├── kafka-client.js          # Kafka client initialization and configuration
├── kafka-admin.js           # Kafka admin utilities (topic creation)
├── database-processor.js    # Sample Kafka consumer for data processing
├── docker-compose.yml       # Kafka container configuration
├── public/
│   └── index.html          # Frontend UI with Leaflet map
├── package.json            # Node.js dependencies
└── README.md               # Documentation
```

## 🔄 How It Works

### Architecture Flow

1. **Client Geolocation** → Browser requests user's GPS coordinates
2. **Socket.IO Emission** → Client sends location via WebSocket to server
3. **Kafka Producer** → Server publishes location event to `location-updates` topic
4. **Topic Storage** → Kafka persists messages with 2 partitions
5. **Kafka Consumer** → Real-time consumer broadcasts to all connected clients
6. **Map Update** → Frontend receives event and renders marker on Leaflet map
7. **Data Processing** → Database processor consumer logs events for persistence

## 📋 Prerequisites

- **Node.js** 18+ ([Download](https://nodejs.org))
- **npm** 9+ (comes with Node.js)
- **Docker** 20.10+ ([Install](https://docs.docker.com/get-docker))
- **Docker Compose** 2.0+ ([Install](https://docs.docker.com/compose/install))
- **Modern Browser** with Geolocation support (Chrome, Firefox, Safari, Edge)

## 🚀 Quick Start

### 1. Clone Repository

```bash
git clone https://github.com/RITURAJ-techie/Real-Time-Location-Tracker.git
cd Real-Time-Location-Tracker
```

### 2. Install Dependencies

```bash
npm install
```

This installs:
- `express` - Web framework
- `socket.io` - Real-time communication
- `kafkajs` - Kafka client library

### 3. Start Kafka Broker

```bash
docker compose up -d
```

**What this does:**
- Pulls Apache Kafka 4.2.0 image
- Starts single-node Kafka cluster
- Exposes broker on `localhost:9092`
- Enables KRaft mode (combined broker + controller)

**Verify Kafka is running:**
```bash
docker ps
```

Expected output:
```
CONTAINER ID   IMAGE                PORTS
abc123def456   apache/kafka:4.2.0   9092/tcp, 9093/tcp
```

### 4. Create Kafka Topic

```bash
node kafka-admin.js
```

**Output:**
```
Kafka admin connecting...
Kafka Admin Connecting Success ...
Topic 'location-updates' created with 2 partitions
```

### 5. Start Application Server

```bash
node index.js
```

**Output:**
```
Server is running on http://localhost:8000
```

### 6. Access Application

Open your browser and navigate to:
```
http://localhost:8000
```

Grant geolocation permission when prompted. Your location will appear on the map.

## 🔧 Configuration

### Environment Variables

```bash
# Set custom port (default: 8000)
PORT=8080 node index.js

# Set Kafka broker address (default: localhost:9092)
KAFKA_BROKERS=kafka1:9092,kafka2:9092 node index.js
```

### Kafka Configuration

Edit `docker-compose.yml` to modify:

```yaml
services:
  kafka:
    environment:
      KAFKA_NODE_ID: 1                           # Broker node ID
      KAFKA_PROCESS_ROLES: 'broker,controller'   # Dual role in KRaft
      KAFKA_CONTROLLER_QUORUM_VOTERS: '1@kafka:9093'  # Quorum config
      KAFKA_LISTENERS: 'PLAINTEXT://0.0.0.0:9092,CONTROLLER://0.0.0.0:9093'
      KAFKA_ADVERTISED_LISTENERS: 'PLAINTEXT://localhost:9092'
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
```

## 📡 Kafka Deep Dive

### What is Apache Kafka?

Apache Kafka is a distributed event streaming platform designed for:
- **High Throughput** - Millions of messages per second
- **Scalability** - Horizontal scaling across brokers
- **Durability** - Messages persisted to disk
- **Fault Tolerance** - Replication across nodes
- **Real-Time Processing** - Low-latency event consumption

### Topic Configuration

**Topic Name:** `location-updates`

**Partitions:** 2
- Messages distributed across 2 partitions for parallel processing
- Default partition key: `socket.id` (ensures same user's locations go to same partition)

**Replication Factor:** 1 (single-node setup)

### Producer (Server)

The main server (`index.js`) acts as a Kafka **producer**:

```javascript
await kafkaProducer.send({
  topic: 'location-updates',
  messages: [{
    key: socket.id,  // Route same client to same partition
    value: JSON.stringify({
      id: socket.id,
      latitude: 28.6139,
      longitude: 77.2090
    })
  }]
});
```

**Message Format:**
```json
{
  "id": "socket_123abc",
  "latitude": 28.6139,
  "longitude": 77.2090
}
```

### Consumer (Real-Time Broadcast)

The server also runs a **consumer** to read published events and broadcast via Socket.IO:

```javascript
await kafkaConsumer.subscribe({
  topics: ['location-updates'],
  fromBeginning: true  // Process all historical messages on startup
});

kafkaConsumer.run({
  eachMessage: async ({ topic, partition, message }) => {
    const data = JSON.parse(message.value.toString());
    io.emit('server:location:update', data);  // Broadcast to clients
  }
});
```

### Consumer Group

**Consumer Group ID:** `socket-server-${PORT}`
- Each Socket.IO server instance is a separate consumer group
- Enables independent message consumption
- Useful for scaling across multiple server instances

### Database Processor Consumer

`database-processor.js` is a separate consumer for data persistence:

```javascript
const kafkaConsumer = kafkaClient.consumer({
  groupId: 'database-processor'
});

kafkaConsumer.run({
  eachMessage: async ({ message }) => {
    const data = JSON.parse(message.value.toString());
    console.log(`INSERT INTO DB LOCATION`, data);
    // TODO: Implement actual database persistence
  }
});
```

**Run in separate terminal:**
```bash
node database-processor.js
```

### Scaling Kafka

For production deployment with high throughput:

```bash
# Start multiple Kafka brokers
docker compose up -d --scale kafka=3

# Create topic with replication
KAFKA_REPLICATION_FACTOR=3 node kafka-admin.js

# Scale consumers
node database-processor.js &
PORT=8001 node index.js &
PORT=8002 node index.js &
```

### Monitoring Kafka

**List topics:**
```bash
docker exec kafka kafka-topics.sh --list --bootstrap-server localhost:9092
```

**Describe topic:**
```bash
docker exec kafka kafka-topics.sh --describe --topic location-updates --bootstrap-server localhost:9092
```

**Consume messages from beginning:**
```bash
docker exec kafka kafka-console-consumer.sh \
  --topic location-updates \
  --from-beginning \
  --bootstrap-server localhost:9092
```

**Check consumer group lag:**
```bash
docker exec kafka kafka-consumer-groups.sh \
  --group socket-server-8000 \
  --describe \
  --bootstrap-server localhost:9092
```

## 🌐 API Reference

### HTTP Endpoints

#### Health Check
```http
GET /health
```

**Response:**
```json
{
  "healthy": true
}
```

**Status Code:** `200 OK`

### Socket.IO Events

#### Client → Server

**Event:** `client:location:update`
```javascript
socket.emit('client:location:update', {
  latitude: 28.6139,
  longitude: 77.2090
});
```

**Payload:**
| Field | Type | Description |
|-------|------|-------------|
| `latitude` | Number | Geographic latitude (-90 to 90) |
| `longitude` | Number | Geographic longitude (-180 to 180) |

#### Server → Client

**Event:** `server:location:update`
```javascript
socket.on('server:location:update', (data) => {
  const { id, latitude, longitude } = data;
  // Update map marker for client with given id
});
```

**Payload:**
| Field | Type | Description |
|-------|------|-------------|
| `id` | String | Socket ID of the location sender |
| `latitude` | Number | User's latitude |
| `longitude` | Number | User's longitude |

## 🎨 Frontend Architecture

### Geolocation API

The frontend uses HTML5 Geolocation API with high accuracy:

```javascript
navigator.geolocation.getCurrentPosition(
  (position) => {
    const { latitude, longitude } = position.coords;
    // Send to server via Socket.IO
  },
  (error) => console.error(error),
  { enableHighAccuracy: true }  // Request GPS instead of network
);
```

### Map Rendering

- **Library:** Leaflet.js 1.9.4
- **Tile Provider:** OpenStreetMap (free, no API key needed)
- **Default View:** London (51.505°N, 0.09°W)
- **Zoom Level:** 13

### Marker Management

- **Your Marker:** Blue marker at your location (labeled "You are here")
- **Remote Markers:** Other users' locations with Socket ID popups
- **Update Frequency:** Every 10 seconds

```javascript
setInterval(async () => {
  await updateLocation();
}, 10 * 1000);  // 10 second interval
```

## 🐳 Docker & Deployment

### Docker Compose Services

**Kafka Service Configuration:**

```yaml
services:
  kafka:
    image: apache/kafka:4.2.0
    container_name: kafka
    ports:
      - '9092:9092'  # Broker port (PLAINTEXT)
      - '9093:9093'  # Controller port (internal)
    environment:
      # KRaft Mode Configuration
      KAFKA_NODE_ID: 1
      KAFKA_PROCESS_ROLES: 'broker,controller'
      KAFKA_CONTROLLER_QUORUM_VOTERS: '1@kafka:9093'
      
      # Listener Configuration
      KAFKA_LISTENERS: 'PLAINTEXT://0.0.0.0:9092,CONTROLLER://0.0.0.0:9093'
      KAFKA_ADVERTISED_LISTENERS: 'PLAINTEXT://localhost:9092'
      KAFKA_CONTROLLER_LISTENER_NAMES: 'CONTROLLER'
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: 'CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT'
      
      # Replication Settings
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 1
```

### Common Docker Commands

```bash
# Start services
docker compose up -d

# View logs
docker compose logs -f kafka

# Stop services
docker compose down

# Remove volumes (reset Kafka data)
docker compose down -v

# Access Kafka container shell
docker exec -it kafka bash
```

## 📊 Performance Considerations

| Metric | Target | Notes |
|--------|--------|-------|
| Latency | < 1s | Socket.IO + Kafka pipeline |
| Throughput | 1000 events/sec | Per partition |
| Consumer Lag | < 100ms | Real-time broadcast |
| Connection Pool | 100+ concurrent | Socket.IO scaling |

## 🔒 Security Notes

**Current Implementation (Development Only):**
- No authentication/authorization
- Unencrypted Kafka connections
- Public geolocation sharing
- CORS enabled for all origins

**Production Recommendations:**
- Enable Kafka SSL/TLS encryption
- Implement JWT-based Socket.IO authentication
- Add geolocation privacy controls
- Rate limit location updates
- Implement user consent management
- Add audit logging

## 🚨 Troubleshooting

### Kafka Connection Error
```
Error: Broker: KafkaUnavailableError: Broker is not available
```
**Solution:** Ensure Kafka container is running
```bash
docker compose up -d
docker ps
```

### Geolocation Permission Denied
```
Error: User denied access to geolocation
```
**Solution:** Allow geolocation in browser settings or use HTTPS in production

### Port Already in Use
```
Error: listen EADDRINUSE: address already in use :::8000
```
**Solution:** Use different port or kill existing process
```bash
PORT=8001 node index.js
# Or
lsof -ti:8000 | xargs kill -9
```

### Kafka Topic Already Exists
```
Error: Topic 'location-updates' already exists
```
**Solution:** Remove existing topic or skip kafka-admin.js on subsequent runs

## 📚 Additional Resources

- [Socket.IO Documentation](https://socket.io/docs)
- [KafkaJS Guide](https://kafka.js.org)
- [Apache Kafka Official Docs](https://kafka.apache.org/documentation)
- [Leaflet.js API Reference](https://leafletjs.com/reference)
- [HTML5 Geolocation API](https://developer.mozilla.org/en-US/docs/Web/API/Geolocation_API)
- [Express.js Guide](https://expressjs.com)

## 📄 License

This project is open source and available under the MIT License.

## 👤 Author

**RITURAJ-techie**
- GitHub: [@RITURAJ-techie](https://github.com/RITURAJ-techie)

## 🤝 Contributing

Contributions are welcome! To contribute:

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## 📝 Roadmap

- [ ] Database persistence layer (MongoDB/PostgreSQL)
- [ ] User authentication system
- [ ] Geofencing capabilities
- [ ] Historical location tracking
- [ ] Advanced analytics dashboard
- [ ] Mobile app development
- [ ] Multi-region Kafka deployment
- [ ] WebRTC for peer-to-peer communication
- [ ] Location privacy controls
- [ ] Kubernetes deployment configs

## 💡 Future Enhancements

**Kafka Enhancements:**
- Multi-broker replication for fault tolerance
- Schema Registry for message validation
- Exactly-once semantics for critical updates
- Topic retention policies and compaction
- Dead letter queue for failed messages

**Application Features:**
- User profiles and friend list
- Location history visualization
- Real-time notifications
- Search and discovery
- Privacy zones and geofencing
- Integration with third-party maps (Google Maps, Mapbox)

## ⚡ Performance Optimization Tips

1. **Increase Update Interval** - Reduce frequency from 10s to 30s for lower bandwidth
2. **Partition Strategy** - Use location-based partitioning for geographic sharding
3. **Compression** - Enable Kafka message compression for network efficiency
4. **Connection Pooling** - Reuse Kafka connections across requests
5. **Caching** - Cache last known locations to reduce Kafka reads

---

**Last Updated:** October 2026
**Status:** Active Development
