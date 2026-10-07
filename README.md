# Real-Time Location Tracker

A lightweight real-time location sharing application built with Node.js, Express, Socket.IO, Kafka, and Leaflet. The app captures a user's live geolocation, broadcasts updates to connected clients, and processes location events through Kafka for scalable event-driven handling.

## Overview

This project demonstrates a real-time geolocation pipeline where:

- Clients share their current location from the browser
- Socket.IO streams updates to the server in real time
- The server publishes events to a Kafka topic
- Consumers process and log the incoming location events
- A map UI renders nearby user positions dynamically

## Features

- Real-time location broadcasting using Socket.IO
- Browser geolocation support
- Kafka-based event streaming for location updates
- Live map visualization using Leaflet and OpenStreetMap tiles
- Health check endpoint for service monitoring
- Dockerized Kafka setup for local development

## Tech Stack

- Node.js
- Express.js
- Socket.IO
- KafkaJS
- Docker Compose
- Leaflet
- JavaScript / HTML / CSS

## Project Structure

```text
Real-Time-Location-Tracker/
├── docker-compose.yml
├── index.js
├── kafka-client.js
├── kafka-admin.js
├── database-processor.js
├── public/
│   └── index.html
└── README.md
```

## How It Works

1. The browser obtains permission to access the user's current location.
2. The frontend sends the latitude and longitude to the backend via Socket.IO.
3. The server publishes the location payload to the Kafka topic `location-updates`.
4. Kafka consumers receive the event and process it asynchronously.
5. The client receives updates from the server and updates the map markers in real time.

## Prerequisites

Before running the project, make sure you have:

- Node.js 18+ installed
- npm installed
- Docker and Docker Compose installed
- A browser with geolocation access enabled

## Installation

1. Clone the repository:

```bash
git clone https://github.com/RITURAJ-techie/Real-Time-Location-Tracker.git
cd Real-Time-Location-Tracker
```

2. Install dependencies:

```bash
npm init -y
npm install express socket.io kafkajs
```

3. Start Kafka using Docker Compose:

```bash
docker compose up -d
```

4. Create the Kafka topic:

```bash
node kafka-admin.js
```

5. Start the application server:

```bash
node index.js
```

6. Open the app in your browser:

```text
http://localhost:8000
```

## Environment and Runtime

The app uses the following default runtime settings:

- Port: `8000`
- Kafka broker: `localhost:9092`
- Kafka topic: `location-updates`

You can override the port with:

```bash
PORT=8080 node index.js
```

## Endpoints

### Health Check

```http
GET /health
```

Response:

```json
{
  "healthy": true
}
```

## Socket Events

### Client to Server

```javascript
socket.emit('client:location:update', {
  latitude: 28.6139,
  longitude: 77.2090
});
```

### Server to Client

```javascript
socket.on('server:location:update', (data) => {
  console.log(data);
});
```

## Notes

- The frontend uses the browser's Geolocation API and renders a map with Leaflet.
- The app is designed for local development and demonstration purposes.
- The Kafka consumer script (`database-processor.js`) is included as a sample for processing events, but can be extended to store data in a database or analytics system.

## License

This project is open for educational and personal use. Please add your own license if you intend to distribute it publicly.

## Author

RITURAJ-techie

## Contributing

Contributions are welcome. If you would like to improve the project, feel free to open a pull request or share enhancements.
