package main

import (
	"github.com/gin-gonic/gin"
	"fmt"
	"github.com/spf13/viper"
	"github.com/sirupsen/logrus"
	"database/sql"
	"os"
	"path/filepath"
	_ "github.com/lib/pq" // this is required to use the Postgres connector
	"time"
)

var database *sql.DB

func parseConfig() {
	configPath := os.Getenv("CONFIG_PATH")
	dir, filename := filepath.Split(configPath)
	ext := filepath.Ext(filename)
	name := filename[0:len(filename)-len(ext)]

	logrus.Info("Reading configuration")

	viper.AddConfigPath(dir)
	viper.SetConfigName(name)
	err := viper.ReadInConfig()
	if err != nil {
		logrus.WithField("err", err).Fatalln("Unable to read configuration")
	}
}

func getJob(c *gin.Context) {
	var status string
	var email string
	var createDate time.Time
	var execPath string
	var args string
	var driverOutputURI string

	jobID := c.Query("jobid")

	// Check if database connection is open
	if database == nil {
		c.JSON(200, gin.H{
			"errorInfo":  "An error occurred. Please, contact the administrator.",
		})
	}

	// Query the target job
	query := `SELECT Status, Email, CreationTimestamp, ExecutablePath, Arguments, DriverOutputURI
			  FROM Job J, Users U
			  WHERE J.ID = $1 AND J.Author = U.ID;`
	database.QueryRow(query, jobID).Scan(&status, &email, &createDate, &execPath, &args, &driverOutputURI)

	if status != "" {
		c.JSON(200, gin.H{
			"status":  status,
			"user": email,
			"creationTimeStamp": createDate,
			"scriptPath": execPath,
			"args": args,
			"driverOutputURI": driverOutputURI,
		})
	} else {
		c.JSON(200, gin.H{
			"errorInfo":  "The job does not exist",
		})
	}
}

func main() {
	parseConfig()

	connStr := fmt.Sprintf("%s://%s:%s@%s:%s/%s?sslmode=disable",
		viper.GetString("dbType"), viper.GetString("dbUser"),
		viper.GetString("dbPassword"), viper.GetString("dbHost"),
		viper.GetString("dbPort"), viper.GetString("dbName"),
	)

	// Connect to database
	var err error
	logrus.Info("Connecting to persistent storage database")
	database, err = sql.Open(viper.GetString("dbType"), connStr)
	if err != nil {
		logrus.Fatal("Unable to open database connection for persistent storage")
	}

	// Set the router as the default one shipped with Gin
	r := gin.Default()

	r.GET("/api/jobs", getJob)
	r.Run(":8083")
}
