# Data API Alternative (Go Version)

This is work in progress for a Go implementation of the Data API server with MongoDB integration and API key authentication.

## Features

- MongoDB CRUD operations
- API key and secret authentication
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
   export API_SECRET=your_api_secret_here
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
   export API_SECRET=your_api_secret_here
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
- `X-API-Key`: Your API key
- `X-API-Secret`: Your API secret

### Health Check
```
GET /api/health
```

### MongoDB Operations

#### Insert One Document
```
curl -X POST http://127.0.0.1:3000/api/insertOne   -H "Content-Type: application/json"   -H "X-API-Key: your_api_key"   -H "X-API-Secret: your_api_secret"   -d '{"database": "your_database", "collection": "your_collection", "document": {"field1": "value1", "field2": "value2"}}'
```

#### Find One Document
```
GET /api/findOne
{
    "database": "your_database",
    "collection": "your_collection",
    "filter": {
        "field": "value"
    },
    "projection": {
        "field1": 1,
        "field2": 1
    }
}
```

#### Find Multiple Documents
```
POST /api/find
{
    "database": "your_database",
    "collection": "your_collection",
    "filter": {
        "field": "value"
    },
    "projection": {
        "field1": 1,
        "field2": 1
    },
    "sort": {
        "field": 1
    },
    "limit": 10
}
```

#### Update One Document
```
POST /api/updateOne
{
    "database": "your_database",
    "collection": "your_collection",
    "filter": {
        "field": "value"
    },
    "update": {
        "$set": {
            "field": "new_value"
        }
    }
}
```

#### Delete One Document
```
POST /api/deleteOne
{
    "database": "your_database",
    "collection": "your_collection",
    "filter": {
        "field": "value"
    }
}
```

#### Aggregate Documents
```
POST /api/aggregate
{
    "database": "your_database",
    "collection": "your_collection",
    "pipeline": [
        {
            "$match": {
                "field": "value"
            }
        },
        {
            "$group": {
                "_id": "$field",
                "count": { "$sum": 1 }
            }
        }
    ]
}
```

## Error Responses

- 400 Bad Request: Invalid request body
- 403 Forbidden: Invalid API key or secret
- 404 Not Found: Document not found
- 500 Internal Server Error: Server error
