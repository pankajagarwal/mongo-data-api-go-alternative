package main

import (
	"log"
	"os"
	"time"

	"mongo-data-api-go-alternative/db"
	"mongo-data-api-go-alternative/handlers"

	"github.com/gofiber/fiber/v2"
	"github.com/ansrivas/fiberprometheus/v2"
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

	// Add monitor middleware for metrics
	prometheus := fiberprometheus.NewWith("mongo-data-api","mongodataapi","http")
	prometheus.RegisterAt(app, "/metrics")
	prometheus.SetSkipPaths([]string{"/api/health", "/metrics"})
	app.Use(prometheus.Middleware)

	// API Key Authentication Middleware
	app.Use(func(c *fiber.Ctx) error {
		// Skip API key check for health and metrics endpoints
		if c.Path() == "/api/health" || c.Path() == "/metrics" {
			return c.Next()
		}

		apiKey := c.Get("apiKey")

		if apiKey != os.Getenv("API_KEY") {
			return c.Status(fiber.StatusForbidden).JSON(fiber.Map{
				"message": "Forbidden: Invalid API Key ",
			})
		}

		return c.Next()
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

	// Start server
	port := os.Getenv("PORT")
	if port == "" {
		port = "3000"
	}
	log.Fatal(app.Listen(":" + port))
}
