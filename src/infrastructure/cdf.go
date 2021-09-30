package infrastructure

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"

	"golang.org/x/oauth2/clientcredentials"
)

type CdfClient struct {
	TokenUrl          string   `json:"token_url"`
	TokenCliendId     string   `json:"client_id"`
	TokenClientSecret string   `json:"client_secret"`
	TokenScopes       []string `json:"scopes"`
	Audience          string   `json:"audience"`
	Project           string   `jsjon:"cdf_project"`
	BaseUrl           string   `json:"base_url"`
	httpClient        *http.Client
}

type RawRow struct {
	Key           string                 `json:"key"`
	LastUpdatedAt int64                  `json:"lastUpdatedTime"`
	Columns       map[string]interface{} `json:"columns"`
}

type ListRowResponse struct {
	Rows   []RawRow `json:"items"`
	Cursor string   `json:"nextCursor"`
}

func FromCredentialsFile(path string) (result CdfClient, err error) {
	credentialsFile, err := os.Open(path)
	if err != nil {
		return result, err
	}
	defer credentialsFile.Close()

	bytes, err := ioutil.ReadAll(credentialsFile)
	if err != nil {
		return result, err
	}

	err = json.Unmarshal(bytes, &result)
	if err != nil {
		return result, err
	}

	if result.TokenCliendId == "" || result.TokenClientSecret == "" || result.TokenUrl == "" || result.BaseUrl == "" {
		return result, errors.New("invalid credentials file")
	}

	config := clientcredentials.Config{
		ClientID:     result.TokenCliendId,
		ClientSecret: result.TokenClientSecret,
		TokenURL:     result.TokenUrl,
		Scopes:       result.TokenScopes,
	}
	if result.Audience != "" {
		config.EndpointParams = make(url.Values)
		config.EndpointParams.Set("audience", result.Audience)
	}
	result.httpClient = config.Client(context.Background())

	return result, nil
}

const LIMIT = 1000

func (api *CdfClient) RetrieveRows(dbName string, tableName string) (batch chan []RawRow, _err chan error) {
	batch = make(chan []RawRow, LIMIT)

	go func() {
		cdfQueryParams := make(url.Values)
		cursor := ""
		for {
			responseData := ListRowResponse{
				Rows:   []RawRow{},
				Cursor: "",
			}
			if cursor != "" {
				cdfQueryParams.Set("cursor", cursor)
			}
			cdfQueryParams.Set("limit", fmt.Sprintf("%d", LIMIT))

			apiUrl := fmt.Sprintf("%s/api/v1/projects/%s/raw/dbs/%s/tables/%s/rows?%s", api.BaseUrl, api.Project, dbName, tableName, cdfQueryParams.Encode())
			res, err := api.httpClient.Get(apiUrl)
			if err != nil {
				close(batch)
				_err <- err
				break
			}
			if res.StatusCode >= 400 {
				close(batch)
				_err <- fmt.Errorf("got status %d with message: %s", res.StatusCode, res.Status)
				break
			}

			defer res.Body.Close()
			json.NewDecoder(res.Body).Decode(&responseData)

			batch <- responseData.Rows

			cursor = responseData.Cursor
			if cursor == "" || len(responseData.Rows) < LIMIT {
				close(batch)
				_err <- fmt.Errorf("EOS")
				break
			}

		}
	}()
	return batch, _err

}
