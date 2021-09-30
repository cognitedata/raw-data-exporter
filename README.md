# raw-data-exporter
Simple cli tool to export a whole Raw table as a CSV file


## Usage

place file named `credentials.json` with `Oauth` credentials into the same folder as this tool


run `raw-exporter -cdfProject <cdf project name> -dbName <raw db name> -tableName <raw table name>` 

### credentials file format

```json
{
    "client_id": "<Oauth client id>",
    "client_secret": "<Oauth client secret>",
    "token_url": "<Oauth token URL>",
    "audience": "<if uses Auth0>",
    "scopes": ["if", "uses", "scopes"],
    "base_url": "<https://<cluster name | api>.cognitedata.com>"
}
```