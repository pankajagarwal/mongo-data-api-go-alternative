package handlers

import (
	"context"
	"encoding/json"
	"log"
	"time"

	"mongo-data-api-go-alternative/db"
	"mongo-data-api-go-alternative/metrics"

	"github.com/gofiber/fiber/v2"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type Document struct {
	Database   string                   `json:"database" binding:"required"`
	Collection string                   `json:"collection" binding:"required"`
	Document   map[string]interface{}   `json:"document"`
	Documents  []map[string]interface{} `json:"documents"`
	Filter     map[string]interface{}   `json:"filter"`
	Update     map[string]interface{}   `json:"update"`
	Projection map[string]interface{}   `json:"projection"`
	Sort       map[string]interface{}   `json:"sort"`
	Limit      int64                    `json:"limit"`
	Pipeline   []map[string]interface{} `json:"pipeline"`
}

// Helper function to deserialize incoming data
func deserializeInput(input interface{}) (interface{}, error) {
	// Convert input to JSON string
	jsonData, err := json.Marshal(input)
	if err != nil {
		return nil, err
	}

	// Deserialize JSON string to BSON
	var bsonData interface{}
	err = bson.UnmarshalExtJSON(jsonData, true, &bsonData)
	if err != nil {
		return nil, err
	}

	return bsonData, nil
}

// Helper function to serialize outgoing data
func serializeOutput(output interface{}) (string, error) {
	// Serialize BSON data to EJSON
	ejsonBytes, err := bson.MarshalExtJSON(output, true, true)
	if err != nil {
		return "", err
	}

	return string(ejsonBytes), nil
}

// InsertOne handles document insertion
func InsertOne(c *fiber.Ctx) error {
	start := time.Now()
	var doc Document
	if err := c.BodyParser(&doc); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": err.Error()})
	}

	// Deserialize the incoming document
	deserializedDoc, err := deserializeInput(doc.Document)
	if err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "Failed to deserialize document"})
	}

	collection := db.GetCollection(doc.Database, doc.Collection)
	result, err := collection.InsertOne(context.Background(), deserializedDoc)
	duration := time.Since(start).Seconds()

	// Record MongoDB operation metrics
	metrics.RecordMongoOperation("insertOne", doc.Database, doc.Collection, duration, err)

	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": err.Error()})
	}

	// Serialize the result before returning
	serializedResult, err := serializeOutput(result.InsertedID)
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": "Failed to serialize result"})
	}

	return c.JSON(fiber.Map{"insertedId": serializedResult})
}

// InsertMany handles inserting multiple documents
func InsertMany(c *fiber.Ctx) error {
	var doc Document
	if err := c.BodyParser(&doc); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": err.Error()})
	}

	// Deserialize the incoming documents
	var deserializedDocs []interface{}
	for _, document := range doc.Documents {
		deserializedDoc, err := deserializeInput(document)
		if err != nil {
			return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "Failed to deserialize document"})
		}
		deserializedDocs = append(deserializedDocs, deserializedDoc)
	}

	collection := db.GetCollection(doc.Database, doc.Collection)
	result, err := collection.InsertMany(context.Background(), deserializedDocs)
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": err.Error()})
	}

	// Serialize the result before returning
	serializedResult, err := serializeOutput(result.InsertedIDs)
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": "Failed to serialize result"})
	}

	return c.JSON(fiber.Map{"insertedIds": serializedResult})
}

// FindOne handles single document retrieval
func FindOne(c *fiber.Ctx) error {
	var doc Document
	if err := c.BodyParser(&doc); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": err.Error()})
	}

	// Deserialize the filter and projection
	deserializedFilter, err := deserializeInput(doc.Filter)
	if err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "Failed to deserialize filter"})
	}

	deserializedProjection, err := deserializeInput(doc.Projection)
	if err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "Failed to deserialize projection"})
	}

	collection := db.GetCollection(doc.Database, doc.Collection)

	var result bson.M
	err = collection.FindOne(context.Background(), deserializedFilter, options.FindOne().SetProjection(deserializedProjection)).Decode(&result)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			return c.Status(fiber.StatusNotFound).JSON(fiber.Map{"error": "No document found"})
		}
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": err.Error()})
	}

	// Serialize the result before returning
	serializedResult, err := serializeOutput(result)
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": "Failed to serialize result"})
	}

	return c.JSON(fiber.Map{"document": serializedResult})
}

// Find handles multiple document retrieval
func Find(c *fiber.Ctx) error {
	var doc Document
	if err := c.BodyParser(&doc); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": err.Error()})
	}

	// Deserialize the filter and projection
	deserializedFilter, err := deserializeInput(doc.Filter)
	if err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "Failed to deserialize filter"})
	}

	deserializedProjection, err := deserializeInput(doc.Projection)
	if err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "Failed to deserialize projection"})
	}

	collection := db.GetCollection(doc.Database, doc.Collection)
	opts := options.Find().SetProjection(deserializedProjection).SetSort(doc.Sort).SetLimit(doc.Limit)
	cursor, err := collection.Find(context.Background(), deserializedFilter, opts)
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": err.Error()})
	}
	defer cursor.Close(context.Background())

	var results []bson.M
	if err = cursor.All(context.Background(), &results); err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": err.Error()})
	}

	// Serialize the results before returning
	serializedResults, err := serializeOutput(results)
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": "Failed to serialize results"})
	}

	return c.JSON(fiber.Map{"documents": serializedResults})
}

// UpdateOne handles updating a single document
func UpdateOne(c *fiber.Ctx) error {
	var doc Document
	if err := c.BodyParser(&doc); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": err.Error()})
	}

	// Deserialize the filter and update
	deserializedFilter, err := deserializeInput(doc.Filter)
	if err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "Failed to deserialize filter"})
	}

	deserializedUpdate, err := deserializeInput(doc.Update)
	if err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "Failed to deserialize update"})
	}

	collection := db.GetCollection(doc.Database, doc.Collection)
	result, err := collection.UpdateOne(context.Background(), deserializedFilter, deserializedUpdate)
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": err.Error()})
	}

	// Serialize the result before returning
	serializedResult, err := serializeOutput(result)
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": "Failed to serialize result"})
	}

	return c.JSON(fiber.Map{"result": serializedResult})
}

// UpdateMany handles updating multiple documents
func UpdateMany(c *fiber.Ctx) error {
	var doc Document
	if err := c.BodyParser(&doc); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": err.Error()})
	}

	// Deserialize the filter and update
	deserializedFilter, err := deserializeInput(doc.Filter)
	if err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "Failed to deserialize filter"})
	}

	deserializedUpdate, err := deserializeInput(doc.Update)
	if err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "Failed to deserialize update"})
	}

	collection := db.GetCollection(doc.Database, doc.Collection)
	result, err := collection.UpdateMany(context.Background(), deserializedFilter, deserializedUpdate)
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": err.Error()})
	}

	// Serialize the result before returning
	serializedResult, err := serializeOutput(result)
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": "Failed to serialize result"})
	}

	return c.JSON(fiber.Map{"result": serializedResult})
}

// DeleteOne handles deleting a single document
func DeleteOne(c *fiber.Ctx) error {
	var doc Document
	if err := c.BodyParser(&doc); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": err.Error()})
	}

	// Deserialize the filter
	deserializedFilter, err := deserializeInput(doc.Filter)
	if err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "Failed to deserialize filter"})
	}

	collection := db.GetCollection(doc.Database, doc.Collection)
	result, err := collection.DeleteOne(context.Background(), deserializedFilter)
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": err.Error()})
	}

	// Serialize the result before returning
	serializedResult, err := serializeOutput(result)
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": "Failed to serialize result"})
	}

	return c.JSON(fiber.Map{"result": serializedResult})
}

// DeleteMany handles deleting multiple documents
func DeleteMany(c *fiber.Ctx) error {
	var doc Document
	if err := c.BodyParser(&doc); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": err.Error()})
	}

	// Deserialize the filter
	deserializedFilter, err := deserializeInput(doc.Filter)
	if err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "Failed to deserialize filter"})
	}

	collection := db.GetCollection(doc.Database, doc.Collection)
	result, err := collection.DeleteMany(context.Background(), deserializedFilter)
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": err.Error()})
	}

	// Serialize the result before returning
	serializedResult, err := serializeOutput(result)
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": "Failed to serialize result"})
	}

	return c.JSON(fiber.Map{"result": serializedResult})
}

// Aggregate handles aggregation pipeline operations
func Aggregate(c *fiber.Ctx) error {
	var doc Document
	if err := c.BodyParser(&doc); err != nil {
		log.Printf("Error parsing request body: %v", err)
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": err.Error()})
	}

	// Deserialize the pipeline
	deserializedPipeline, err := deserializeInput(doc.Pipeline)
	if err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "Failed to deserialize pipeline"})
	}

	collection := db.GetCollection(doc.Database, doc.Collection)

	cursor, err := collection.Aggregate(context.Background(), deserializedPipeline)
	if err != nil {
		log.Printf("Aggregation error: %v", err)
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": err.Error()})
	}
	defer cursor.Close(context.Background())

	var results []bson.M
	if err = cursor.All(context.Background(), &results); err != nil {
		log.Printf("Error reading results: %v", err)
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": err.Error()})
	}

	// Serialize the results before returning
	serializedResults, err := serializeOutput(results)
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": "Failed to serialize results"})
	}

	return c.JSON(fiber.Map{"documents": serializedResults})
}
