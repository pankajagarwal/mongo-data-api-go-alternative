# Data API Alternative (Go Version)

This is work in progress for a Go implementation of the Data API server with MongoDB integration and API key authentication.

## Features

- MongoDB CRUD operations
- API key authentication
- Request logging
- Environment variable configuration
- Health check endpoint
- Docker support

## Setup

### Local Development

1. Install Go (version 1.22 or higher)
2. Clone the repository
3. Set the following environment variables:
   ```bash
   export PORT=3000
   export API_KEY=your_api_key_here
   export MONGO_URI=mongodb://localhost:27017
   ```
4. Install dependencies:
   ```bash
   go mod download
   ```
5. Run the server:
   ```bash
   go run main.go
   ```

### Docker Setup

1. Make sure Docker and Docker Compose are installed
2. Set the following environment variables:
   ```bash
   export API_KEY=your_api_key_here
   ```
3. Build and start the containers:
   ```bash
   docker-compose up --build
   ```
4. To run in detached mode:
   ```bash
   docker-compose up -d
   ```
5. To stop the containers:
   ```bash
   docker-compose down
   ```

## API Usage

All API endpoints require the following headers:
- `apiKey`: Your API key

### Health Check
```
GET /api/health
```

### MongoDB Operations

#### Metrics
```
curl http://127.0.0.1:3000/metrics -H "apiKey: your_api_key"
```

#### Insert One Document
```
curl -X POST http://127.0.0.1:3000/api/insertOne -H "Content-Type: application/json" -H "apiKey: test_key" -d '{"database": "your_database", "collection": "your_collection", "document": {"field1": "value1", "field2": "value2"}}'
```

#### Find One Document
```
curl -X POST http://127.0.0.1:3000/api/findOne -H "Content-Type: application/json" -H "apiKey: test_key" -d '{"database": "your_database", "collection": "your_collection", "filter": {"field1": "value1"}}'
```

#### Delete One Document
```
curl -X POST http://127.0.0.1:3000/api/deleteOne -H "Content-Type: application/json" -H "apiKey: test_key" -d '{"database": "your_database", "collection": "your_collection", "filter": {"field1": "value1"}}'
```

#### Aggregate
```
curl -s "http://127.0.0.1:3000/api/aggregate" \
  -X POST -H "apiKey: test_key -H 'Content-Type: application/ejson' -H "Accept: application/json" -d '{
    "dataSource": "mongodb-atlas",
    "database": "your_database",
    "collection": "your_collection",
    "pipeline": [
      {
        "$match": { "field1": "value1" }
      },
      {
        "$group": {
          "_id": "$status",
          "count": { "$sum": 1 },
          "tasks": { "$push": "$text" }
        }
      },
      {
        "$sort": { "count": -1 }
      }
    ]
  }'

```


## Error Responses

- 400 Bad Request: Invalid request body
- 403 Forbidden: Invalid API key
- 404 Not Found: Document not found
- 500 Internal Server Error: Server error
