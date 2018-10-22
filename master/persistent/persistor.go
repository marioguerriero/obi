package persistent

import (
	"database/sql"
	"errors"
	"fmt"
	"github.com/sirupsen/logrus"
		"obi/master/model"
	"time"

	_ "github.com/lib/pq" // this is required to use the Postgres connector
	"os"
	"io/ioutil"
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
	username, err := ioutil.ReadFile("/etc/db/credentials/username")
	if err != nil {
		logrus.Fatal("Unable to read stolon username")
	}

	password, err := ioutil.ReadFile("/etc/db/credentials/password")
	if err != nil {
		logrus.Fatal("Unable to read stolon password")
	}

	// Create connection string
	//connStr := fmt.Sprintf("postgres://%s:%s@%s:%s/postgres?sslmode=disable",
	//	string(username),
	//	string(password), os.Getenv("STOLON_PROXY_DNS_NAME"),
	//	os.Getenv("STOLON_PROXY_PORT"),
	//)

	psqlInfo := fmt.Sprintf("host=%s port=%s user=%s "+
		"password=%s dbname=%s sslmode=disable",
		os.Getenv("STOLON_PROXY_DNS_NAME"), os.Getenv("STOLON_PROXY_PORT"), string(username), string(password), "postgres")

	fmt.Println(psqlInfo)
	// Connect to database
	logrus.Info("Connecting to persistent storage database")
	database, err = sql.Open("postgres", psqlInfo)
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
	createUsersTableQuery := "CREATE TABLE IF NOT EXISTS Users (ID SERIAL PRIMARY KEY, Email TEXT, Password CHAR(60));"

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
		AssignedJobs INT,
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
		Author INT REFERENCES Users(ID), 
		CreationTimestamp TIMESTAMP, 
		LastUpdateTimestamp TIMESTAMP, 
		ExecutablePath TEXT, 
		Type TEXT, 
		Priority INT,
		PredictedDuration INT, 
		FailureProbability Float, 
		Arguments TEXT, 
		PlatformDependentID TEXT,
        DriverOutputURI TEXT,
		FOREIGN KEY (ClusterName, ClusterCreationTimestamp) REFERENCES Cluster(Name, CreationTimestamp)
			ON DELETE CASCADE)`

	_, err = database.Exec(createJobsTableQuery)

	return err
}

func getJobsByStatus(status, cluster string) ([]*model.Job, error) {
	// Check if database connection is open
	if database == nil {
		return nil, errors.New("database connection is not open")
	}

	// Query jobs
	var rows *sql.Rows
	var err error
	if len(cluster) == 0 {
		query := `SELECT ID, CreationTimestamp, ExecutablePath, Type, Status, Priority,
			PredictedDuration, FailureProbability, Arguments, PlatformDependentID 
				FROM Job WHERE Status='$1'`
		rows, err = database.Query(query, status)
	} else {
		query := `SELECT ID, CreationTimestamp, ExecutablePath, Type, Status, Priority,
			PredictedDuration, FailureProbability, Arguments, PlatformDependentID 
				FROM Job WHERE Status='$1' AND ClusterName='$2'`
		rows, err = database.Query(query, status, cluster)
	}
	defer rows.Close()
	if err != nil {
		return nil, err
	}
	return extractJobsFromRows(rows)
}

// GetPendingJobs returns all the jobs marked as running[
func GetPendingJobs() ([]*model.Job, error) {
	// Check if database connection is open
	if database == nil {
		return nil, errors.New("database connection is not open")
	}

	// Query jobs
	query := `SELECT ID, CreationTimestamp, ExecutablePath, Type, Status, Priority,
			PredictedDuration, FailureProbability, Arguments, PlatformDependentID 
				FROM Job WHERE Status='pending'`
	rows, err := database.Query(query)
	defer rows.Close()
	if err != nil {
		return nil, err
	}
	return extractJobsFromRows(rows)
}

// GetRunningJobs returns all the jobs marked as running[
func GetRunningJobs(cluster string) ([]*model.Job, error) {
	// Check if database connection is open
	if database == nil {
		return nil, errors.New("database connection is not open")
	}

	// Query jobs
	query := `SELECT ID, CreationTimestamp, ExecutablePath, Type, Status, Priority,
			PredictedDuration, FailureProbability, Arguments, PlatformDependentID 
				FROM Job WHERE Status='running' AND ClusterName=$1`
	rows, err := database.Query(query, cluster)
	defer rows.Close()
	if err != nil {
		return nil, err
	}
	return extractJobsFromRows(rows)
}

func extractJobsFromRows(rows *sql.Rows) ([]*model.Job, error) {
	var jobs []*model.Job

	for rows.Next() {
		var id int
		var creationTimestamp time.Time
		var executablePath string
		var jobTypeDescription string
		var jobType model.JobType
		var statusDescription string
		var status model.JobStatus
		var priority int32
		var predictedDuration int
		var failureProbability float32
		var args string
		var platformID string

		err := rows.Scan(&id, &creationTimestamp, &executablePath, &jobTypeDescription,
			&statusDescription, &priority, &predictedDuration, &failureProbability, &args,
			&platformID)
		if err != nil {
			return nil, err
		}

		// find out job type
		for k, v := range model.JobTypeNames {
			if jobTypeDescription == v {
				jobType = k
			}
		}

		// find out job type
		for k, v := range model.JobStatusNames {
			if statusDescription == v {
				status = k
			}
		}

		jobs = append(jobs, &model.Job{
			ID:                  id,
			CreationTimestamp:   creationTimestamp,
			ExecutablePath:      executablePath,
			Type:                jobType,
			Priority:            priority,
			Status:              status,
			Args:                args,
			PlatformDependentID: platformID,
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
				PlatformDependentID,
				DriverOutputURI)
			VALUES (
				$1, $2, $3, CURRENT_TIMESTAMP, $4, $5, $6, $7, $8, $9, $10, $11
			) RETURNING ID`
	stmt, err := database.Prepare(query)
	defer stmt.Close()
	if err != nil {
		return err
	}
	err = stmt.QueryRow(
		model.JobStatusNames[job.Status],
		job.Author,
		job.CreationTimestamp,
		job.ExecutablePath,
		model.JobTypeNames[job.Type],
		job.Priority,
		job.PredictedDuration,
		job.FailureProbability,
		job.Args,
		job.PlatformDependentID,
		job.DriverOutputPath,
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
				PlatformDependentID = $11,
				DriverOutputURI = $12
			WHERE Job.ID = $13;`
	stmt, err := database.Prepare(query)
	defer stmt.Close()
	if err != nil {
		return err
	}
	return stmt.QueryRow(
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
		job.DriverOutputPath,
		job.ID,
	).Scan()
}

func insertClusterQuery(cluster model.ClusterBaseInterface) error {
	query := `INSERT INTO Cluster (
				Name, 
				Platform, 
				Status, 
				CreationTimestamp, 
				Cost,
				LastUpdateTimestamp,
				AssignedJobs)
			VALUES (
				$1, $2, $3, $4, 0.0, CURRENT_TIMESTAMP, $5
			)`
	stmt, err := database.Prepare(query)
	defer stmt.Close()
	if err != nil {
		fmt.Println(err)
		return err
	}
	return stmt.QueryRow(cluster.GetName(),
		cluster.GetPlatform(),
		model.ClusterStatusNames[cluster.GetStatus()],
		cluster.GetCreationTimestamp(),
		cluster.GetAllocatedJobSlots(),
	).Scan()
}

// GetRunningDatabaseCreationTimestamp returns the creation timestamp for a given cluster in running state (if any)
func GetRunningDatabaseCreationTimestamp(cluster string) (*time.Time, bool) {
	// Check if database connection is open
	if database == nil {
		return nil, false
	}

	// Check if the cluster exists in database
	if !rowExists(`SELECT CreationTimestamp FROM Cluster WHERE Name=$1 AND Status='running'`, cluster) {
		return nil, false
	}

	// Query cluster
	var ts time.Time

	query := `SELECT CreationTimestamp
				FROM Cluster WHERE Status='running' AND Name=$1`
	err := database.QueryRow(query, cluster).Scan(&ts)
	if err != nil {
		return nil, false
	}
	return &ts, true
}

func updateClusterQuery(cluster model.ClusterBaseInterface) error {
	query := `UPDATE Cluster SET
				Platform = $1, 
				Status = $2, 
				Cost = $3,
				LastUpdateTimestamp = CURRENT_TIMESTAMP,
				AssignedJobs = $4
			WHERE Cluster.Name = $5 AND Cluster.CreationTimestamp = $6;`
	stmt, err := database.Prepare(query)
	defer stmt.Close()
	if err != nil {
		return err
	}
	return stmt.QueryRow(
		cluster.GetPlatform(),
		model.ClusterStatusNames[cluster.GetStatus()],
		cluster.GetCost(),
		cluster.GetAllocatedJobSlots(),
		cluster.GetName(),
		cluster.GetCreationTimestamp(),
	).Scan()
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

// ClusterExists queries the database to see if a certain cluster exists
func ClusterExists(clusterName string) (bool, error) {
	// Check if database connection is open
	if database == nil {
		return false, errors.New("database connection is not open")
	}

	return rowExists(`SELECT * FROM Cluster WHERE Name = $1 AND Status = 'running'`, clusterName), nil
}

// GetUserID given a username and a password, this function returns the corresponding user's ID
func GetUserID(username string, password string) (int, error) {
	var id int
	// Check if database connection is open
	if database == nil {
		return 0, errors.New("database connection is not open")
	}

	query := `SELECT ID FROM Users WHERE Email = $1 AND Password = crypt($2, Password);`
	database.QueryRow(query, username, password).Scan(&id)
	if id == 0 {
		return 0, errors.New("user not authorized")
	}
	return id, nil
}
