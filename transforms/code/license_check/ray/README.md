# Code Quality 

Please see the set of
[transform project conventions](../../../README.md)
for details on general project conventions, transform configuration,
testing and IDE set up.

## Summary

This project wraps the [licence check transform](../python/README.md) with a Ray runtime.

## Running

### Launcher Command Line Options 


When running the transform with the TransformLauncher, the following command line arguments are available in addition to the options provided by the python launcher.

  --lc_license_column_name LICENSE_COLUMN_NAME
                        Name of the column holds the data to process
  --lc_allow_no_license
                        allow entries with no associated license (default: false)
  --lc_licenses_file LICENSES_FILE
                        S3 or local path to allowed/denied licenses JSON file
  --lc_deny_licenses    allow all licences except those in licenses_file (default: false)

### Running the samples

To run the samples, use the following `make` targets

* `run-cli-ray-sample` 
* `run-local-ray-sample` 
* `run-s3-ray-sample` 
    * Requires prior invocation of `make minio-start` to load data into local minio for S3 access.

These targets will activate the virtual environment and set up any configuration needed.
Use the `-n` option of `make` to see the detail of what is done to run the sample.

For example, 

```shell
make run-cli-ray-sample
...
```

Then 

```shell
ls output
```
To see results of the transform.
