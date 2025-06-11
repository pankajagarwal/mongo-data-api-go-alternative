package handlers

import (
	"context"
	"encoding/json"
	"log"

	"mongo-data-api-go-alternative/db"

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
	Upsert     bool                     `json:"upsert"`
	Projection map[string]interface{}   `json:"projection"`
	Sort       map[string]interface{}   `json:"sort"`
	Limit      int64                    `json:"limit"`
	Skip       int64                    `json:"skip"`
	Pipeline   []map[string]interface{} `json:"pipeline"`
}

// Helper function to deserialize incoming data
func deserializeInput(input interface{}) (interface{}, error) {
	// Convert input to JSON string
	jsonData, err := json.Marshal(input)
	if err != nil {
		log.Printf("Failed to marshal input to JSON: %v", err)
		return nil, err
	}

	// Deserialize JSON string to BSON
	var bsonData interface{}
	err = bson.UnmarshalExtJSON(jsonData, false, &bsonData)
	if err != nil {
		log.Printf("Failed to deserialize input: %v", err)
		log.Printf("Input JSON: %s", string(jsonData))
		return nil, err
	}

	return bsonData, nil
}

// Helper function to serialize outgoing data
func serializeOutput(output interface{}) (interface{}, error) {
	// Serialize BSON data to EJSON
	ejsonBytes, err := bson.MarshalExtJSON(output, false, false)
	if err != nil {
		return nil, err
	}

	// Unmarshal the EJSON bytes back into a Go map
	var jsonData interface{}
	if err := json.Unmarshal(ejsonBytes, &jsonData); err != nil {
		return nil, err
	}

	return jsonData, nil
}

// InsertOne handles document insertion
func InsertOne(c *fiber.Ctx) error {
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

	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": err.Error()})
	}

	// Wrap the result in a map to serialize
	wrappedResult := map[string]interface{}{
		"insertedId": result.InsertedID,
	}

	// Serialize the result before returning
	serializedResult, err := serializeOutput(wrappedResult)
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": "Failed to serialize result"})
	}

	return c.JSON(serializedResult)
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

	// Wrap the result in a map to serialize
	wrappedResult := map[string]interface{}{
		"insertedIds": result.InsertedIDs,
	}

	// Serialize the result before returning
	serializedResult, err := serializeOutput(wrappedResult)
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": "Failed to serialize result"})
	}

	return c.JSON(serializedResult)
}

// FindOne handles single document retrieval
func FindOne(c *fiber.Ctx) error {
	var doc Document
	if err := c.BodyParser(&doc); err != nil {
		log.Printf("Error parsing request body: %v", err)
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": err.Error()})
	}

	deserializedFilter, err := deserializeInput(doc.Filter)
	if err != nil {
		log.Printf("Failed to deserialize filter: %v", err)
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "Failed to deserialize filter", "details": err.Error()})
	}

	collection := db.GetCollection(doc.Database, doc.Collection)

	findOptions := options.FindOne()
	if doc.Projection != nil {
		findOptions.SetProjection(doc.Projection)
	}

	var result bson.M
	err = collection.FindOne(context.Background(), deserializedFilter, findOptions).Decode(&result)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			return c.Status(fiber.StatusOK).JSON(fiber.Map{"document": nil})
		}
		log.Printf("Error executing FindOne: %v", err)
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": err.Error()})
	}

	// Wrap and serialize the result
	wrappedResult := map[string]interface{}{
		"document": result,
	}

	serializedResult, err := serializeOutput(wrappedResult)
	if err != nil {
		log.Printf("Failed to serialize result: %v", err)
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": "Failed to serialize result"})
	}

	return c.JSON(serializedResult)
}

// Find handles multiple document retrieval
func Find(c *fiber.Ctx) error {
	var doc Document
	if err := c.BodyParser(&doc); err != nil {
		log.Printf("Error parsing request body: %v", err)
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": err.Error()})
	}

	deserializedFilter, err := deserializeInput(doc.Filter)
	if err != nil {
		log.Printf("Failed to deserialize filter: %v", err)
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "Failed to deserialize filter", "details": err.Error()})
	}

	collection := db.GetCollection(doc.Database, doc.Collection)

	findOptions := options.Find()
	if doc.Projection != nil {
		findOptions.SetProjection(doc.Projection)
	}
	if doc.Sort != nil {
		findOptions.SetSort(doc.Sort)
	}
	if doc.Limit > 0 {
		findOptions.SetLimit(doc.Limit)
	}
	if doc.Skip > 0 {
		findOptions.SetSkip(doc.Skip)
	}

	cursor, err := collection.Find(context.Background(), deserializedFilter, findOptions)
	if err != nil {
		log.Printf("Error executing Find: %v", err)
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": err.Error()})
	}
	defer cursor.Close(context.Background())

	results := make([]bson.M, 0)
	if err := cursor.All(context.Background(), &results); err != nil {
		log.Printf("Error decoding results: %v", err)
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": "Failed to decode results"})
	}

	wrappedResult := map[string]interface{}{
		"documents": results,
	}

	serializedResult, err := serializeOutput(wrappedResult)
	if err != nil {
		log.Printf("Failed to serialize result: %v", err)
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": "Failed to serialize result"})
	}

	return c.JSON(serializedResult)
}

// UpdateOne handles updating a single document
func UpdateOne(c *fiber.Ctx) error {
	var doc Document
	if err := c.BodyParser(&doc); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": err.Error()})
	}

	deserializedFilter, err := deserializeInput(doc.Filter)
	if err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "Failed to deserialize filter"})
	}

	deserializedUpdate, err := deserializeInput(doc.Update)
	if err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "Failed to deserialize update"})
	}

	collection := db.GetCollection(doc.Database, doc.Collection)
	opts := options.Update()
	if doc.Upsert {
		opts.SetUpsert(true)
	}
	result, err := collection.UpdateOne(context.Background(), deserializedFilter, deserializedUpdate, opts)
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": err.Error()})
	}

	wrappedResult := map[string]interface{}{
		"upsertedId":    result.UpsertedID,
		"upsertedCount": result.UpsertedCount,
		"modifiedCount": result.ModifiedCount,
		"matchedCount":  result.MatchedCount,
	}

	serializedResult, err := serializeOutput(wrappedResult)
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": "Failed to serialize result"})
	}

	return c.JSON(serializedResult)
}

// UpdateMany handles updating multiple documents
func UpdateMany(c *fiber.Ctx) error {
	var doc Document
	if err := c.BodyParser(&doc); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": err.Error()})
	}

	deserializedFilter, err := deserializeInput(doc.Filter)
	if err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "Failed to deserialize filter"})
	}

	deserializedUpdate, err := deserializeInput(doc.Update)
	if err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "Failed to deserialize update"})
	}

	collection := db.GetCollection(doc.Database, doc.Collection)
	opts := options.Update()
	if doc.Upsert {
		opts.SetUpsert(true)
	}
	result, err := collection.UpdateMany(context.Background(), deserializedFilter, deserializedUpdate, opts)
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": err.Error()})
	}

	wrappedResult := map[string]interface{}{
		"modifiedCount": result.ModifiedCount,
		"matchedCount":  result.MatchedCount,
	}

	serializedResult, err := serializeOutput(wrappedResult)
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": "Failed to serialize result"})
	}

	return c.JSON(serializedResult)
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

	// Wrap the result in a map to serialize
	wrappedResult := map[string]interface{}{
		"result": result,
	}

	// Serialize the result before returning
	serializedResult, err := serializeOutput(wrappedResult)
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": "Failed to serialize result"})
	}

	return c.JSON(serializedResult)
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

	// Wrap the result in a map to serialize
	wrappedResult := map[string]interface{}{
		"result": result,
	}

	// Serialize the result before returning
	serializedResult, err := serializeOutput(wrappedResult)
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": "Failed to serialize result"})
	}

	return c.JSON(serializedResult)
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
		log.Printf("Failed to deserialize pipeline: %v", err)
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "Failed to deserialize pipeline", "details": err.Error()})
	}

	collection := db.GetCollection(doc.Database, doc.Collection)

	// Execute the aggregation
	cursor, err := collection.Aggregate(context.Background(), deserializedPipeline)
	if err != nil {
		log.Printf("Aggregation error: %v", err)
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": "Aggregation failed", "details": err.Error()})
	}
	defer cursor.Close(context.Background())

	results := make([]bson.M, 0)
	if err = cursor.All(context.Background(), &results); err != nil {
		log.Printf("Error reading aggregation results: %v", err)
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": "Failed to read aggregation results", "details": err.Error()})
	}

	wrappedResults := map[string]interface{}{
		"documents": results,
	}

	// Serialize the results before returning
	serializedResults, err := serializeOutput(wrappedResults)
	if err != nil {
		log.Printf("Failed to serialize results: %v", err)
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": "Failed to serialize results", "details": err.Error()})
	}

	return c.JSON(serializedResults)
}
