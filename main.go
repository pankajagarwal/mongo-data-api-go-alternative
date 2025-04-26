package main

import (
	"log"
	"os"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/middleware/logger"
	"data-api-alternative-go/db"
	"data-api-alternative-go/handlers"
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
		apiKey := c.Get("X-API-Key")
		apiSecret := c.Get("X-API-Secret")

		if apiKey != os.Getenv("API_KEY") || apiSecret != os.Getenv("API_SECRET") {
			return c.Status(fiber.StatusForbidden).JSON(fiber.Map{
				"message": "Forbidden: Invalid API Key or Secret",
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

	// API Routes
	api := app.Group("/api")
	{
		api.Get("/health", func(c *fiber.Ctx) error {
			return c.JSON(fiber.Map{"status": "ok"})
		})

		// MongoDB operations
		api.Put("/insertOne", handlers.InsertOne)
		api.Post("/insertMany", handlers.InsertMany)
		api.Get("/findOne", handlers.FindOne)
		api.Get("/find", handlers.Find)
		api.Patch("/updateOne", handlers.UpdateOne)
		api.Delete("/deleteOne", handlers.DeleteOne)
		api.Delete("/deleteMany", handlers.DeleteMany)
		api.Post("/aggregate", handlers.Aggregate)
	}

	// Start server
	port := os.Getenv("PORT")
	if port == "" {
		port = "3000"
	}
	log.Fatal(app.Listen(":" + port))
} 