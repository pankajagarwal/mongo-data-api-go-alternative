package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"mongo-data-api-go-alternative/db"
	"mongo-data-api-go-alternative/metrics"

	"github.com/gofiber/fiber/v2"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
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
		log.Printf("Failed to marshal input to JSON: %v", err)
		return nil, err
	}

	// Log the JSON string for debugging
	log.Printf("Input JSON for deserialization: %s", string(jsonData))

	// Deserialize JSON string to BSON
	var bsonData interface{} // Use interface{} to handle both bson.D and bson.A
	err = bson.UnmarshalExtJSON(jsonData, true, &bsonData)
	if err != nil {
		log.Printf("Failed to deserialize input: %v", err)
		log.Printf("Input JSON: %s", string(jsonData))
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

func preprocessFilter(filter map[string]interface{}) (map[string]interface{}, error) {
	for key, value := range filter {
		switch v := value.(type) {
		case map[string]interface{}:
			// Check for $oid and $date keys in the nested map
			if oid, ok := v["$oid"]; ok {
				// Convert $oid to primitive.ObjectID
				objectID, err := primitive.ObjectIDFromHex(oid.(string))
				if err != nil {
					return nil, err
				}
				filter[key] = objectID // Replace the entire map with the ObjectID
			} else if date, ok := v["$date"]; ok {
				// Convert $date to primitive.DateTime
				switch dateValue := date.(type) {
				case string:
					parsedDate, err := time.Parse(time.RFC3339, dateValue)
					if err != nil {
						return nil, err
					}
					filter[key] = primitive.NewDateTimeFromTime(parsedDate) // Replace the entire map with the DateTime
				case float64: // Handle timestamp in milliseconds
					filter[key] = primitive.NewDateTimeFromTime(time.UnixMilli(int64(dateValue)))
				default:
					return nil, fmt.Errorf("invalid $date value: %v", date)
				}
			} else {
				// Recursively preprocess nested objects
				processedValue, err := preprocessFilter(v)
				if err != nil {
					return nil, err
				}
				filter[key] = processedValue
			}
		}
	}
	return filter, nil
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

	// Log the raw filter
	log.Printf("Raw Filter: %+v", doc.Filter)

	// Preprocess the filter
	preprocessedFilter, err := preprocessFilter(doc.Filter)
	if err != nil {
		log.Printf("Failed to preprocess filter: %v", err)
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "Failed to preprocess filter", "details": err.Error()})
	}

	// Log the preprocessed filter
	log.Printf("Preprocessed Filter: %+v", preprocessedFilter)

	// Preprocess the projection
	preprocessedProjection, err := preprocessFilter(doc.Projection)
	if err != nil {
		log.Printf("Failed to preprocess projection: %v", err)
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "Failed to preprocess projection", "details": err.Error()})
	}

	// Log the preprocessed projection
	log.Printf("Preprocessed Projection: %+v", preprocessedProjection)

	collection := db.GetCollection(doc.Database, doc.Collection)

	// Execute the FindOne query with filter and projection
	var result bson.M
	err = collection.FindOne(
		context.Background(),
		preprocessedFilter,
		options.FindOne().SetProjection(preprocessedProjection),
	).Decode(&result)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			return c.Status(fiber.StatusNotFound).JSON(fiber.Map{"error": "No document found"})
		}
		log.Printf("Error executing FindOne: %v", err)
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": err.Error()})
	}

	// Wrap the result in a map to serialize
	wrappedResult := map[string]interface{}{
		"document": result,
	}

	// Serialize the result before returning
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

	// Wrap the results in a map to serialize
	wrappedResults := map[string]interface{}{
		"documents": results,
	}

	// Serialize the results before returning
	serializedResults, err := serializeOutput(wrappedResults)
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": "Failed to serialize results"})
	}

	return c.JSON(serializedResults)
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

	// log.Printf("Deserialized Pipeline: %+v", deserializedPipeline)

	collection := db.GetCollection(doc.Database, doc.Collection)

	// Execute the aggregation
	cursor, err := collection.Aggregate(context.Background(), deserializedPipeline)
	if err != nil {
		log.Printf("Aggregation error: %v", err)
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": "Aggregation failed", "details": err.Error()})
	}
	defer cursor.Close(context.Background())

	// Read the results
	var results []bson.M
	if err = cursor.All(context.Background(), &results); err != nil {
		log.Printf("Error reading aggregation results: %v", err)
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": "Failed to read aggregation results", "details": err.Error()})
	}

	// log.Printf("Aggregation Results: %+v", results)

	// Wrap the results in a map to serialize
	wrappedResults := map[string]interface{}{
		"documents": results,
	}

	// Serialize the results before returning
	serializedResults, err := serializeOutput(wrappedResults)
	if err != nil {
		log.Printf("Failed to serialize results: %v", err)
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": "Failed to serialize results", "details": err.Error()})
	}
	// log.Printf("serializedResults: %+v", serializedResults)

	return c.JSON(serializedResults)
}
