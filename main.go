package main

import (
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	"mongo-data-api-go-alternative/db"
	"mongo-data-api-go-alternative/handlers"
	"mongo-data-api-go-alternative/metrics"

	"github.com/gofiber/fiber/v2"
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
	// app.Use(logger.New())

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

		// Log request details
		// log.Printf("Method: %s, URL: %s, Body: %v, Headers: %v",
		// 	c.Method(),
		// 	c.OriginalURL(),
		// 	c.Body(),
		// 	c.GetReqHeaders(),
		// )

		return c.Next()
	})

	// Metrics middleware with conditional logging
	app.Use(func(c *fiber.Ctx) error {
		start := time.Now()
		err := c.Next()
		duration := time.Since(start).Milliseconds() // Measure duration in milliseconds

		// Log only if the response time exceeds 1000ms or the status code is not 200
		statusCode := c.Response().StatusCode()
		if duration > 100 || statusCode != fiber.StatusOK {
			log.Printf("Method: %s, URL: %s, Status: %d, Duration: %dms, Body: %v, Headers: %v",
				c.Method(),
				c.OriginalURL(),
				statusCode,
				duration,
				string(c.Body()), // Convert body to string for logging
				c.GetReqHeaders(),
			)
		}

		// Get the original path before any modifications
		path := c.OriginalURL()
		if path == "" {
			path = c.Path()
		}

		// Skip metrics for health check and metrics endpoints
		if path == "/api/health" || path == "/metrics" {
			return err
		}

		// Sanitize method and path
		method := strings.ToUpper(strings.TrimSpace(c.Method()))
		path = strings.TrimSpace(path)

		// Only record metrics for valid HTTP methods
		validMethods := map[string]bool{
			"GET":     true,
			"POST":    true,
			"PUT":     true,
			"DELETE":  true,
			"PATCH":   true,
			"HEAD":    true,
			"OPTIONS": true,
		}

		// Only record metrics for valid API routes
		validRoutes := map[string]bool{
			"/api/insertOne":   true,
			"/api/insertMany":  true,
			"/api/findOne":     true,
			"/api/find":        true,
			"/api/updateOne":   true,
			"/api/updateMany":  true,
			"/api/deleteOne":   true,
			"/api/deleteMany":  true,
			"/api/aggregate":   true,
		}

		// Debug log for invalid paths
		if !validRoutes[path] {
			log.Printf("Skipping metrics for invalid path: %s", path)
			return err
		}

		if validMethods[method] && validRoutes[path] {
			// Record HTTP metrics
			metrics.RecordHTTPRequest(
				method,
				path,
				strconv.Itoa(statusCode),
				float64(duration)/1000.0, // Convert duration to seconds for metrics
			)
		}

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
