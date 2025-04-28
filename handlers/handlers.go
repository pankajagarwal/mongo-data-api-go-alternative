package handlers

import (
	"context"
	"log"
	"time"

	"github.com/gofiber/fiber/v2"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"mongo-data-api-go-alternative/db"
	"mongo-data-api-go-alternative/metrics"
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

// Aggregate handles aggregation pipeline operations
func Aggregate(c *fiber.Ctx) error {
	var doc Document
	if err := c.BodyParser(&doc); err != nil {
		log.Printf("Error parsing request body: %v", err)
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": err.Error()})
	}

	log.Printf("Received request for database: %s, collection: %s", doc.Database, doc.Collection)
	log.Printf("Original pipeline: %+v", doc.Pipeline)

	collection := db.GetCollection(doc.Database, doc.Collection)

	// Convert the pipeline to proper MongoDB format
	var pipeline mongo.Pipeline
	for i, stage := range doc.Pipeline {
		// Convert each stage to bson.D
		stageDoc := bson.D{}
		for key, value := range stage {
			switch key {
			case "$match":
				if match, ok := value.(map[string]interface{}); ok {
					if id, ok := match["_id"].(map[string]interface{}); ok {
						if oid, ok := id["$oid"].(string); ok {
							objectId, err := bson.ObjectIDFromHex(oid)
							if err != nil {
								log.Printf("Error converting ObjectId: %v", err)
								return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": err.Error()})
							}
							stageDoc = append(stageDoc, bson.E{Key: "$match", Value: bson.D{
								{Key: "_id", Value: objectId},
							}})
							log.Printf("Stage %d - Match with ObjectId: %v", i, objectId)
						}
					}
				}
			case "$lookup":
				if lookup, ok := value.(map[string]interface{}); ok {
					stageDoc = append(stageDoc, bson.E{Key: "$lookup", Value: bson.D{
						{Key: "from", Value: lookup["from"]},
						{Key: "localField", Value: lookup["localField"]},
						{Key: "foreignField", Value: lookup["foreignField"]},
						{Key: "as", Value: lookup["as"]},
					}})
					log.Printf("Stage %d - Lookup: from=%s, localField=%s, foreignField=%s, as=%s", 
						i, lookup["from"], lookup["localField"], lookup["foreignField"], lookup["as"])
				}
			case "$project":
				if project, ok := value.(map[string]interface{}); ok {
					projectDoc := bson.D{}
					for field := range project {
						projectDoc = append(projectDoc, bson.E{Key: field, Value: 1})
					}
					stageDoc = append(stageDoc, bson.E{Key: "$project", Value: projectDoc})
					log.Printf("Stage %d - Project: %+v", i, projectDoc)
				}
			}
		}
		if len(stageDoc) > 0 {
			pipeline = append(pipeline, stageDoc)
		}
	}

	// Log the final aggregation pipeline for debugging
	log.Printf("Final MongoDB pipeline: %+v", pipeline)
	log.Printf("Collection: %s", doc.Collection)

	// Pass the modified pipeline to Aggregate
	cursor, err := collection.Aggregate(context.Background(), pipeline)
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
	result, err := collection.InsertMany(context.Background(), doc.Documents)
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