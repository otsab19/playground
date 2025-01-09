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
)

// Config structure to hold configuration details
type Config struct {
	DBConnection  string            `json:"db_connection"`
	CSVFile       string            `json:"csv_file"`
	TableName     string            `json:"table_name"`
	SQLStatement  string            `json:"sql_statement"`
	ColumnMapping map[string]int    `json:"column_mapping"`
	CustomMapping map[string]string `json:"custom_mapping"` // Updated to correct type
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

	// Loop through the records and insert them into the table
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

		// Execute the insert statement
		_, err := db.Exec(config.SQLStatement, values...)
		if err != nil {
			log.Printf("Failed to insert record on row %d: %v", i+1, err)
		}
	}

	fmt.Println("Data insertion complete.")
}
