# Use SerpAPI

[SerpAPI](https://serpapi.com) is a paid service for SERP download.


## Config

```json
{
  "global": {
    "serpapi": {
      "date": "today 5-y",
      "cat": "0",
      "tz": "120"
    },
    "path": {
      "parent_folder": "s3://sm-google-trend/download/"
    }
  },
  "keywords": [
    {
      "serpapi": {
        "timeframe": "today 5-y",
        "cat": "0",
        "geo": "DE",
        "q": "phone case"
      }
    },
    {
      "serpapi": {
        "timeframe": "today 5-y",
        "cat": "0",
        "geo": "DE",
        "q": "curtain"
      }
    }
  ]
}
```


## Download


```sh
poetry run trendy download-serpapi s3://sm-google-trend/configs/serpapi_config_de.json
```
