package infrastructure

import (
	"encoding/csv"
	"fmt"
	"os"
)

type CsvWriter struct {
	path          string
	file          *os.File
	headers       []string
	headerWritten bool
	delimeter     string
}

func NewCsvWriter() CsvWriter {
	return CsvWriter{
		delimeter: ",",
	}
}

func (w *CsvWriter) SetDestination(path string) {
	w.path = path
}

func (w *CsvWriter) writeHeader(writer *csv.Writer, mapData map[string]interface{}) []string {
	keys := make([]string, len(mapData))

	i := 0
	for k := range mapData {
		keys[i] = k
		i++
	}
	writer.Write(keys)
	writer.Flush()
	w.headerWritten = true
	return keys
}

func (w *CsvWriter) Append(data []map[string]interface{}) error {
	if w.file == nil {
		file, err := os.OpenFile(w.path, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0755)
		if err != nil {
			return err
		}
		w.file = file
	}
	csvWriter := csv.NewWriter(w.file)

	if !w.headerWritten && len(data) > 0 {
		w.headers = w.writeHeader(csvWriter, data[0])
	}

	for idx := range data {
		mapData := data[idx]

		values := make([]string, len(mapData))

		i := 0
		for k := range w.headers {
			if mapData[w.headers[k]] != nil {
				values[i] = fmt.Sprint(mapData[w.headers[k]])
			} else {
				values[i] = ""
			}

			i++
		}
		csvWriter.Write(values)

	}
	csvWriter.Flush()
	return nil
}

func (w *CsvWriter) Close() error {
	return w.file.Close()
}
