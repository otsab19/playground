package main

import (
	"database/sql"
	"encoding/csv"
	"encoding/json"
	"fmt"
	_ "github.com/lib/pq"
	"io/ioutil"
	"log"
	"os"
	"sync"
)

// Config structure to hold configuration details
type Config struct {
	DBConnection  string            `json:"db_connection"`
	CSVFile       string            `json:"csv_file"`
	TableName     string            `json:"table_name"`
	SQLStatement  string            `json:"sql_statement"`
	NoOfWorkers   int               `json:"no_of_workers"`
	ColumnMapping map[string]int    `json:"column_mapping"`
	CustomMapping map[string]string `json:"custom_mapping"`
}

func (c *Config) GetNoOfWorkers() int {
	println(c.NoOfWorkers)
	if c.NoOfWorkers <= 0 {
		return 5
	} else {
		return c.NoOfWorkers
	}
}

func worker(db *sql.DB, records <-chan []interface{}, wg *sync.WaitGroup, sqlStatement string) {
	defer wg.Done()
	for values := range records {
		_, err := db.Exec(sqlStatement, values...)
		if err != nil {
			log.Printf("Failed to insert record: %v", err)
		}
	}
}

func main() {
	// Read configuration from JSON file
	configFile, err := os.Open("config.json")
	if err != nil {
		log.Fatalf("Unable to open config file: %v", err)
	}
	defer configFile.Close()

	// Parse the JSON file
	configBytes, err := ioutil.ReadAll(configFile)
	if err != nil {
		log.Fatalf("Unable to read config file: %v", err)
	}

	var config Config
	err = json.Unmarshal(configBytes, &config)
	if err != nil {
		log.Fatalf("Unable to parse config file: %v", err)
	}

	// Open a connection to the database
	db, err := sql.Open("postgres", config.DBConnection)
	if err != nil {
		log.Fatalf("Unable to connect to the database: %v", err)
	}
	defer db.Close()

	// Open the CSV file
	file, err := os.Open(config.CSVFile)
	if err != nil {
		log.Fatalf("Unable to open CSV file: %v", err)
	}
	defer file.Close()

	// Read the CSV file
	reader := csv.NewReader(file)
	records, err := reader.ReadAll()
	if err != nil {
		log.Fatalf("Unable to read CSV file: %v", err)
	}

	// Create channels and a wait group
	recordChan := make(chan []interface{}, 100) // Buffer size for the channel
	var wg sync.WaitGroup

	// Start worker pool
	numWorkers := config.GetNoOfWorkers()
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go worker(db, recordChan, &wg, config.SQLStatement)
	}

	// Loop through the records and send them to the workers
	for i, record := range records {
		values := make([]interface{}, len(config.ColumnMapping)+len(config.CustomMapping))

		// Add CSV-based values
		for colName, csvIndex := range config.ColumnMapping {
			if csvIndex < len(record) {
				values[csvIndex] = record[csvIndex]
			} else {
				log.Printf("Skipping row %d: missing value for column '%s'", i+1, colName)
				continue
			}
		}

		// Add custom_mapping values
		index := len(config.ColumnMapping)
		for _, value := range config.CustomMapping {
			values[index] = value
			index++
		}

		// Send values to the channel
		recordChan <- values
	}

	// Close the channel and wait for all workers to finish
	close(recordChan)
	wg.Wait()

	fmt.Println("Data insertion complete.")
}
