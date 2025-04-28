package handlers

import (
	"context"
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

// InsertOne handles document insertion
func InsertOne(c *fiber.Ctx) error {
	start := time.Now()
	var doc Document
	if err := c.BodyParser(&doc); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": err.Error()})
	}

	collection := db.GetCollection(doc.Database, doc.Collection)
	result, err := collection.InsertOne(context.Background(), doc.Document)
	duration := time.Since(start).Seconds()

	// Record MongoDB operation metrics
	metrics.RecordMongoOperation("insertOne", doc.Database, doc.Collection, duration, err)

	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": err.Error()})
	}

	return c.JSON(fiber.Map{"insertedId": result.InsertedID})
}

// FindOne handles single document retrieval
func FindOne(c *fiber.Ctx) error {
	var doc Document
	if err := c.BodyParser(&doc); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": err.Error()})
	}

	collection := db.GetCollection(doc.Database, doc.Collection)
	var result bson.M
	err := collection.FindOne(context.Background(), doc.Filter, options.FindOne().SetProjection(doc.Projection)).Decode(&result)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			return c.Status(fiber.StatusNotFound).JSON(fiber.Map{"error": "No document found"})
		}
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": err.Error()})
	}

	return c.JSON(result)
}

// Find handles multiple document retrieval
func Find(c *fiber.Ctx) error {
	var doc Document
	if err := c.BodyParser(&doc); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": err.Error()})
	}

	collection := db.GetCollection(doc.Database, doc.Collection)
	opts := options.Find().SetProjection(doc.Projection).SetSort(doc.Sort).SetLimit(doc.Limit)
	cursor, err := collection.Find(context.Background(), doc.Filter, opts)
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": err.Error()})
	}
	defer cursor.Close(context.Background())

	var results []bson.M
	if err = cursor.All(context.Background(), &results); err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": err.Error()})
	}

	return c.JSON(results)
}

// UpdateOne handles document updates
func UpdateOne(c *fiber.Ctx) error {
	var doc Document
	if err := c.BodyParser(&doc); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": err.Error()})
	}

	collection := db.GetCollection(doc.Database, doc.Collection)
	result, err := collection.UpdateOne(context.Background(), doc.Filter, doc.Update)
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": err.Error()})
	}

	return c.JSON(fiber.Map{
		"matchedCount":  result.MatchedCount,
		"modifiedCount": result.ModifiedCount,
		"upsertedCount": result.UpsertedCount,
	})
}

// DeleteOne handles document deletion
func DeleteOne(c *fiber.Ctx) error {
	var doc Document
	if err := c.BodyParser(&doc); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": err.Error()})
	}

	collection := db.GetCollection(doc.Database, doc.Collection)
	result, err := collection.DeleteOne(context.Background(), doc.Filter)
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": err.Error()})
	}

	return c.JSON(fiber.Map{"deletedCount": result.DeletedCount})
}

// fix $oid to objectid's and $numberDouble to float64
func fixBsonDoc(input interface{}) (interface{}, error) {
	switch v := input.(type) {
	case []interface{}:
		for i, elem := range v {
			fixedElem, err := fixBsonDoc(elem)
			if err != nil {
				return nil, err
			}
			v[i] = fixedElem
		}
		return v, nil
	case []map[string]interface{}:
		for i, elem := range v {
			fixedElem, err := fixBsonDoc(elem)
			if err != nil {
				return nil, err
			}
			v[i] = fixedElem.(map[string]interface{})
		}
		return v, nil
	case map[string]interface{}:
		if oid, ok := v["$oid"]; ok {
			if oidStr, ok := oid.(string); ok {
				objID, err := primitive.ObjectIDFromHex(oidStr)
				if err != nil {
					return nil, err
				}
				return objID, nil
			}
		}
		for key, val := range v {
			fixedVal, err := fixBsonDoc(val)
			if err != nil {
				return nil, err
			}
			v[key] = fixedVal
		}
		return v, nil
	default:
		return v, nil
	}
}

// Aggregate handles aggregation pipeline operations
func Aggregate(c *fiber.Ctx) error {
	var doc Document
	if err := c.BodyParser(&doc); err != nil {
		log.Printf("Error parsing request body: %v", err)
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": err.Error()})
	}

	log.Printf("Received request for database: %s, collection: %s", doc.Database, doc.Collection)
	log.Printf("Original pipeline: %+v", doc.Pipeline)

	// FIX the pipeline
	fixedPipelineInterface, err := fixBsonDoc(doc.Pipeline)
	if err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": err.Error()})
	}

	// fixedPipelineInterface is type []interface{}, convert it
	fixedMaps := fixedPipelineInterface.([]map[string]interface{})
	fixedPipeline := make([]interface{}, len(fixedMaps))
	for i, m := range fixedMaps {
		fixedPipeline[i] = m
	}

	collection := db.GetCollection(doc.Database, doc.Collection)

	log.Printf("Cleaned pipeline: %+v", fixedPipeline)
	// Pass the modified pipeline to Aggregate
	cursor, err := collection.Aggregate(context.Background(), fixedPipeline)
	if err != nil {
		log.Printf("Aggregation error: %v", err)
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": err.Error()})
	}
	defer cursor.Close(context.Background())

	// Fetch and return the results
	var results []bson.M
	if err = cursor.All(context.Background(), &results); err != nil {
		log.Printf("Error reading results: %v", err)
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": err.Error()})
	}

	log.Printf("Number of results: %d", len(results))
	if len(results) > 0 {
		log.Printf("First result: %+v", results[0])
	}

	return c.JSON(results)
}

// InsertMany handles multiple document insertion
func InsertMany(c *fiber.Ctx) error {
	var doc Document
	if err := c.BodyParser(&doc); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": err.Error()})
	}

	if len(doc.Documents) == 0 {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "No documents provided"})
	}

	collection := db.GetCollection(doc.Database, doc.Collection)

	// Convert []map[string]interface{} to []interface{}
	docs := make([]interface{}, len(doc.Documents))
	for i, d := range doc.Documents {
		docs[i] = d
	}

	result, err := collection.InsertMany(context.Background(), docs)
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": err.Error()})
	}

	return c.JSON(fiber.Map{"insertedIds": result.InsertedIDs})
}

// DeleteMany handles multiple document deletion
func DeleteMany(c *fiber.Ctx) error {
	var doc Document
	if err := c.BodyParser(&doc); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": err.Error()})
	}

	collection := db.GetCollection(doc.Database, doc.Collection)
	result, err := collection.DeleteMany(context.Background(), doc.Filter)
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": err.Error()})
	}

	return c.JSON(fiber.Map{"deletedCount": result.DeletedCount})
}

// UpdateMany handles multiple document updates
func UpdateMany(c *fiber.Ctx) error {
	var doc Document
	if err := c.BodyParser(&doc); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": err.Error()})
	}

	collection := db.GetCollection(doc.Database, doc.Collection)
	result, err := collection.UpdateMany(context.Background(), doc.Filter, doc.Update)
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": err.Error()})
	}

	return c.JSON(fiber.Map{
		"matchedCount":  result.MatchedCount,
		"modifiedCount": result.ModifiedCount,
		"upsertedCount": result.UpsertedCount,
	})
}
