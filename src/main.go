package main

import (
	"flag"
	"fmt"

	"github.com/timurgen/cdf-raw-data-exporter/src/infrastructure"
)

func main() {
	var cdfProject = flag.String("cdfProject", "", "CDF project name where data located")
	var dbName = flag.String("dbName", "", "CDF Raw database name where data located")
	var tableName = flag.String("tableName", "", "CDF Raw table name where data located")

	var credentials = flag.String("credentials", "credentials.json", "path to file with Oauth credentials")
	// var outputFormat = flag.String("format", "csv", "Output data format")
	var outputFile = flag.String("output", "out.csv", "Output file name")

	flag.Parse()

	cdfClient, err := infrastructure.FromCredentialsFile(*credentials)
	if err != nil {
		panic(err)
	}

	cdfClient.Project = *cdfProject

	batchIterator, _err := cdfClient.RetrieveRows(*dbName, *tableName)

	csvWriter := infrastructure.NewCsvWriter()
	csvWriter.SetDestination(*outputFile)
	defer csvWriter.Close()

	batchCounter := 0
Loop:
	for {
		select {
		case batch := <-batchIterator:
			batchCounter += 1
			fmt.Print(".")
			if batchCounter%32 == 0 {
				fmt.Println()
			}
			if len(batch) == 0 {
				break Loop
			}
			columns := make([]map[string]interface{}, 0, 3)
			for idx := range batch {
				columns = append(columns, batch[idx].Columns)
			}
			csvWriter.Append(columns)
		case receivedError := <-_err:
			if receivedError.Error() == "EOS" {
				break Loop
			}
			panic(receivedError)
		}
	}

}
