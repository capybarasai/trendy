# trendy
Monitor trends (google trend, ...)


## CLI

```sh
poetry run trendy download tests/data/test_config.json
```

### SerpAPI


```sh
poetry run trendy download-serpapi s3://sm-google-trend/configs/serpapi_config_de.json
```


### Manual

Create intermediate folders

```sh
poetry run trendy create-manual-folders s3://sm-google-trend/configs/serpapi_config_de.json /tmp/manual_folders
```

Then manually download trends and place in the corresponding folders. Please keep the original file name (`multiTimeline.csv`)

Upload manually

```sh
poetry run trendy upload-manual s3://sm-google-trend/configs/serpapi_config_de.json /tmp/manual_folders
```
