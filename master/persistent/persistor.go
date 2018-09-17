package persistent

import (
	"database/sql"
	"errors"
	"fmt"
	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	"obi/master/model"
	"time"

	_ "github.com/lib/pq" // this is required to use the Postgres connector
)

var database *sql.DB

// Record a high level description of a persistent storage record
type Record struct {
	Cluster model.ClusterBase
	Job model.Job
	Timestamp time.Time
}

// CreatePersistentConnection opens connection to the persistent storage database
func CreatePersistentConnection() error {
	// Create connection string
	connStr := fmt.Sprintf("%s://%s:%s@%s:%s/%s",
		viper.GetString("dbType"), viper.GetString("dbUser"),
		viper.GetString("dbPassword"), viper.GetString("dbHost"),
		viper.GetString("dbPort"), viper.GetString("dbName"),
	)

	// Connect to database
	var err error
	logrus.WithField("connection-string", connStr).Info("Connecting to persistent storage database")
	database, err = sql.Open(viper.GetString("dbType"), connStr)
	if err != nil {
		logrus.Fatal("Unable to open database connection for persistent storage")
	}

	return err
}

// Write a record into the persistent storage database
func Write(record *Record) error {
	// Check if database connection is open
	if database == nil {
		return errors.New("database connection is not open")
	}

	// TODO: define what to put in the database

	return nil
}
