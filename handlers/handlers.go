package handlers

import (
	"context"
	"fmt"
	"log"
	"strconv"
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

func fixBsonValue(value interface{}) (interface{}, error) {
	switch v := value.(type) {
	case map[string]interface{}:
		// Check for special MongoDB extended JSON types
		if len(v) == 1 {
			if oid, ok := v["$oid"]; ok {
				if oidStr, ok := oid.(string); ok {
					objectID, err := primitive.ObjectIDFromHex(oidStr)
					if err != nil {
						return nil, err
					}
					return objectID, nil
				}
			}
			if numberDouble, ok := v["$numberDouble"]; ok {
				if numStr, ok := numberDouble.(string); ok {
					parsed, err := strconv.ParseFloat(numStr, 64)
					if err != nil {
						return nil, err
					}
					return parsed, nil
				}
			}
			if date, ok := v["$date"]; ok {
				if dateStr, ok := date.(string); ok {
					parsedTime, err := time.Parse(time.RFC3339, dateStr)
					if err != nil {
						return nil, err
					}
					return parsedTime, nil
				}
			}
		}
		// Otherwise recursively fix inner maps
		fixedMap := make(map[string]interface{})
		for key, innerVal := range v {
			fixedInnerVal, err := fixBsonValue(innerVal)
			if err != nil {
				return nil, err
			}
			fixedMap[key] = fixedInnerVal
		}
		return fixedMap, nil

	case []interface{}:
		var fixedArr []interface{}
		for _, item := range v {
			fixedItem, err := fixBsonValue(item)
			if err != nil {
				return nil, err
			}
			fixedArr = append(fixedArr, fixedItem)
		}
		return fixedArr, nil

	default:
		return value, nil
	}
}

// fix $oid to objectid's and $numberDouble to float64
func fixBsonDoc(input []interface{}) ([]interface{}, error) {
	var output []interface{}

	for _, stage := range input {
		if stageMap, ok := stage.(map[string]interface{}); ok {
			fixedStage := make(map[string]interface{})

			for key, value := range stageMap {
				fixedValue, err := fixBsonValue(value)
				if err != nil {
					return nil, err
				}
				fixedStage[key] = fixedValue
			}

			output = append(output, fixedStage)
		} else {
			return nil, fmt.Errorf("unexpected stage type: %T", stage)
		}
	}

	return output, nil
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

	// Fix the pipeline directly
	rawPipeline := make([]interface{}, len(doc.Pipeline))
	for i, m := range doc.Pipeline {
		rawPipeline[i] = m
	}

	fixedPipeline, err := fixBsonDoc(rawPipeline)
	if err != nil {
		log.Printf("Error fixing pipeline: %v", err)
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": err.Error()})
	}

	log.Printf("Cleaned pipeline: %+v", fixedPipeline)

	collection := db.GetCollection(doc.Database, doc.Collection)

	cursor, err := collection.Aggregate(context.Background(), fixedPipeline)
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
