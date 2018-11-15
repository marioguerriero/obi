// Copyright 2018 
// 
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// 
//     http://www.apache.org/licenses/LICENSE-2.0
// 
//     Unless required by applicable law or agreed to in writing, software
//     distributed under the License is distributed on an "AS IS" BASIS,
//     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//     See the License for the specific language governing permissions and
//     limitations under the License.

package main

import (
	"github.com/gin-gonic/gin"
	"fmt"
		"github.com/sirupsen/logrus"
	"database/sql"
	"os"
		_ "github.com/lib/pq" // this is required to use the Postgres connector
	"time"
	"io/ioutil"
)

var database *sql.DB

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
	username, err := ioutil.ReadFile("/etc/db/credentials/username")
	if err != nil {
		logrus.Fatal("Unable to read stolon username")
	}

	password, err := ioutil.ReadFile("/etc/db/credentials/password")
	if err != nil {
		logrus.Fatal("Unable to read stolon password")
	}

	psqlInfo := fmt.Sprintf("host=%s port=%s user=%s "+
		"password=%s dbname=%s sslmode=disable",
		os.Getenv("STOLON_PROXY_DNS_NAME"), os.Getenv("STOLON_PROXY_PORT"), string(username), string(password), "postgres")

	// Connect to database
	logrus.Info("Connecting to persistent storage database")
	database, err = sql.Open("postgres", psqlInfo)
	if err != nil {
		logrus.Fatal("Unable to open database connection for persistent storage")
	}

	// Set the router as the default one shipped with Gin
	r := gin.Default()

	r.GET("/jobs", getJob)
	r.Run(":8083")
}
