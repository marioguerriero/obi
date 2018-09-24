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

	// Initialize tables
	logrus.Info("Running initialization actions for persistent storage")
	err = initTables()

	return err
}

func initTables() error {
	// Create users table
	createUsersTableQuery := "CREATE TABLE IF NOT EXISTS RegisteredUser (ID SERIAL PRIMARY KEY, Email TEXT);"

	_, err := database.Exec(createUsersTableQuery)
	if err != nil {
		return err
	}

	// Create cluster table
	createClusterTableQuery := `CREATE TABLE IF NOT EXISTS Cluster (
		Name VARCHAR(50), 
		Platform VARCHAR(25), 
		Status VARCHAR(20), 
		CreationTimestamp TIMESTAMP, 
		Cost FLOAT,
		LastUpdateTimestamp TIMESTAMP,
		PRIMARY KEY(Name, CreationTimestamp))`

	_, err = database.Exec(createClusterTableQuery)
	if err != nil {
		return err
	}

	// Create job table
	createJobsTableQuery := `CREATE TABLE IF NOT EXISTS Job (
		ID SERIAL PRIMARY KEY, 
		ClusterName VARCHAR(50),
        ClusterCreationTimestamp TIMESTAMP, 
		Status VARCHAR(20), 
		Author INT REFERENCES RegisteredUser(ID), 
		CreationTimestamp TIMESTAMP, 
		LastUpdateTimestamp TIMESTAMP, 
		ExecutablePath TEXT, 
		Type TEXT, 
		Priority INT,
		PredictedDuration INT, 
		FailureProbability Float, 
		Arguments TEXT, 
		PlatformDependentID TEXT,
		FOREIGN KEY (ClusterName, ClusterCreationTimestamp) REFERENCES Cluster(Name, CreationTimestamp)
			ON DELETE CASCADE)`

	_, err = database.Exec(createJobsTableQuery)

	return err
}

// GetPendingJobs returns all the jobs marked as running[
func GetPendingJobs() ([]model.Job, error) {
	return getJobsByState("pending")
}

// GetRunningJobs returns all the jobs marked as running[
func GetRunningJobs() ([]model.Job, error) {
	return getJobsByState("running")
}

// GetJobsByState returns all the jobs marked with a certain state
func getJobsByState(state string) ([]model.Job, error) {
	// Query jobs
	query := `SELECT CreationTimestamp, ExecutablePath, Type, Priority, Status,
			  PredictedDuration, FailureProbability, Arguments FROM Job WHERE Status='$1'`
	rows, err := database.Query(query, state)
	defer rows.Close()
	if err != nil {
		return nil, err
	}

	// Build job objects
	var jobs []model.Job

	for rows.Next() {
		var creationTimestamp time.Time
		var executablePath string
		var jobType string
		var jobTypeCode model.JobType
		var status string
		var statusCode model.JobStatus
		var priority int32
		var predictedDuration int
		var failureProbability float32
		var args string

		err := rows.Scan(&creationTimestamp, &executablePath, &jobType,
			&status, &priority, &predictedDuration, &failureProbability, &args)
		if err != nil {
			return nil, err
		}

		// find out job type
		for k, v := range model.JobTypeNames {
			if jobType == v {
				jobTypeCode = k
			}
		}

		// find out job type
		for k, v := range model.JobStatusNames {
			if status == v {
				statusCode = k
			}
		}

		jobs = append(jobs, model.Job{
			CreationTimestamp:  creationTimestamp,
			ExecutablePath:     executablePath,
			Type:               jobTypeCode,
			Priority:           priority,
			Status: 			statusCode,
			Args:               args,
		})
	}

	return jobs, nil
}

// Write a record into the persistent storage database
func Write(record interface{}) error {
	// Check if database connection is open
	if database == nil {
		return errors.New("database connection is not open")
	}

	logrus.Info("Received persistent storage request")

	// Decide which type of record we are trying to write
	switch record.(type) {
	case *model.Job:
		return writeJob(record.(*model.Job))
	case model.ClusterBaseInterface:
		return writeCluster(record.(model.ClusterBaseInterface))
	default:
		return errors.New("invalid record type")
	}
}

func writeJob(job *model.Job) error {
	logrus.Info("Writing job to persistent storage")

	// If job has no ID set, then a new entry should be created into the database, otherwise update it
	if job.ID == 0 {
		return insertJobQuery(job)
	}

	return updateJobQuery(job)
}

func writeCluster(cluster model.ClusterBaseInterface) error {
	logrus.Info("Writing cluster to persistent storage")

	// If cluster already exists in DB update, otherwise insert
	if rowExists("SELECT * FROM Cluster WHERE Name=$1 AND CreationTimestamp=$2",
			cluster.GetName(), cluster.GetCreationTimestamp()) {
		return updateClusterQuery(cluster)
	}

	return insertClusterQuery(cluster)
}

func insertJobQuery(job *model.Job) error {
	query := `INSERT INTO Job (
				Status, 
				Author, 
				CreationTimestamp, 
				LastUpdateTimestamp, 
				ExecutablePath, 
				Type, 
				Priority,
				PredictedDuration, 
				FailureProbability, 
				Arguments, 
				PlatformDependentID)
			VALUES (
				$1, NULL, $2, CURRENT_TIMESTAMP, $3, $4, $5, $6, $7, $8, $9
			) RETURNING ID`
	stmt, err := database.Prepare(query)
	defer stmt.Close()
	if err != nil {
		return err
	}
	err = stmt.QueryRow(
		model.JobStatusNames[job.Status],
		job.CreationTimestamp,
		job.ExecutablePath,
		model.JobTypeNames[job.Type],
		job.Priority,
		job.PredictedDuration,
		job.FailureProbability,
		job.Args,
		job.PlatformDependentID,
	).Scan(&job.ID)
	if err != nil {
		return err
	}
	return nil
}

func updateJobQuery(job *model.Job) error {
	query := `UPDATE Job SET
				ClusterName = $1,
				ClusterCreationTimestamp = $2,
				Status = $3, 
				CreationTimestamp = $4, 
				LastUpdateTimestamp = CURRENT_TIMESTAMP, 
				ExecutablePath = $5, 
				Type = $6, 
				Priority = $7,
				PredictedDuration = $8, 
				FailureProbability = $9, 
				Arguments = $10, 
				PlatformDependentID = $11
			WHERE Job.ID = $12;`
	stmt, err := database.Prepare(query)
	defer stmt.Close()
	if err != nil {
		return err
	}
	stmt.QueryRow(
		job.Cluster.GetName(),
		job.Cluster.GetCreationTimestamp(),
		model.JobStatusNames[job.Status],
		job.CreationTimestamp,
		job.ExecutablePath,
		model.JobTypeNames[job.Type],
		job.Priority,
		job.PredictedDuration,
		job.FailureProbability,
		job.Args,
		job.PlatformDependentID,
		job.ID,
	)
	return nil
}

func insertClusterQuery(cluster model.ClusterBaseInterface) error {
	query := `INSERT INTO Cluster (
				Name, 
				Platform, 
				Status, 
				CreationTimestamp, 
				Cost,
				LastUpdateTimestamp)
			VALUES (
				$1, $2, $3, $4, 0.0, CURRENT_TIMESTAMP
			)`
	stmt, err := database.Prepare(query)
	defer stmt.Close()
	if err != nil {
		fmt.Println(err)
		return err
	}
	stmt.QueryRow(cluster.GetName(),
		cluster.GetPlatform(),
		model.ClusterStatusNames[cluster.GetStatus()],
		cluster.GetCreationTimestamp(),
	)
	return nil
}

func updateClusterQuery(cluster model.ClusterBaseInterface) error {
	query := `UPDATE Cluster SET
				Platform = $1, 
				Status = $2, 
				Cost = $3,
				LastUpdateTimestamp = CURRENT_TIMESTAMP
			WHERE Cluster.Name = $4 AND Cluster.CreationTimestamp = $5;`
	stmt, err := database.Prepare(query)
	defer stmt.Close()
	if err != nil {
		return err
	}
	stmt.QueryRow(
		cluster.GetPlatform(),
		model.ClusterStatusNames[cluster.GetStatus()],
		cluster.GetCost(),
		cluster.GetName(),
		cluster.GetCreationTimestamp(),
	)
	return nil
}

func rowExists(query string, args ...interface{}) bool {
	var exists bool
	query = fmt.Sprintf("SELECT exists (%s)", query)
	err := database.QueryRow(query, args...).Scan(&exists)
	if err != nil && err != sql.ErrNoRows {
		logrus.WithField("error", err).Error("Error checking if row exists")
	}
	return exists
}
