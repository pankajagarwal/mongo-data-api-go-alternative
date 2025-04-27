package main

import (
	"log"
	"os"
	"strconv"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/middleware/logger"
	"mongo-data-api-go-alternative/db"
	"mongo-data-api-go-alternative/handlers"
	"mongo-data-api-go-alternative/metrics"
)

func main() {
	// Connect to MongoDB
	if err := db.Connect(); err != nil {
		log.Fatal("Error connecting to MongoDB:", err)
	}
	defer db.Close()

	// Create Fiber app
	app := fiber.New(fiber.Config{
		ReadTimeout:  time.Second * 10,
		WriteTimeout: time.Second * 10,
	})

	// Add logger middleware
	app.Use(logger.New())

	// API Key Authentication Middleware
	app.Use(func(c *fiber.Ctx) error {
		apiKey := c.Get("apiKey")

		if apiKey != os.Getenv("API_KEY") {
			return c.Status(fiber.StatusForbidden).JSON(fiber.Map{
				"message": "Forbidden: Invalid API Key ",
			})
		}

		// Log request details
		log.Printf("Method: %s, URL: %s, Body: %v, Headers: %v",
			c.Method(),
			c.OriginalURL(),
			c.Body(),
			c.GetReqHeaders(),
		)

		return c.Next()
	})

	// Metrics middleware
	app.Use(func(c *fiber.Ctx) error {
		start := time.Now()
		err := c.Next()
		duration := time.Since(start).Seconds()

		// Record HTTP metrics
		metrics.RecordHTTPRequest(
			c.Method(),
			c.Path(),
			strconv.Itoa(c.Response().StatusCode()),
			duration,
		)

		return err
	})

	// API Routes
	api := app.Group("/api")
	{
		api.Get("/health", func(c *fiber.Ctx) error {
			return c.JSON(fiber.Map{"status": "ok"})
		})

		// MongoDB operations
		api.Post("/insertOne", handlers.InsertOne)
		api.Post("/insertMany", handlers.InsertMany)
		api.Post("/findOne", handlers.FindOne)
		api.Post("/find", handlers.Find)
		api.Post("/updateOne", handlers.UpdateOne)
		api.Post("/updateMany", handlers.UpdateMany)
		api.Post("/deleteOne", handlers.DeleteOne)
		api.Post("/deleteMany", handlers.DeleteMany)
		api.Post("/aggregate", handlers.Aggregate)
	}

	// Metrics endpoint
	app.Get("/metrics", metrics.Handler())

	// Start server
	port := os.Getenv("PORT")
	if port == "" {
		port = "3000"
	}
	log.Fatal(app.Listen(":" + port))
} 