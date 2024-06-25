# License Check 

Please see the set of
[transform project conventions](../../../README.md#transform-project-conventions)
for details on general project conventions, transform configuration,
testing and IDE set up.

## Summary 

This filter scans the license column of an input dataset and appends the `license_status` column to the dataset.

The type of the license column can be either string or list of strings. For strings, the license name is checked against the list of approved licenses. For list of strings, each license name in the list is checked against the list of approved licenses, and all must be approved.

If the license is approved, the license_status column contains True; otherwise False. 

## Configuration and command line Options

- The optional `lc_license_column` parameter is used to specify the column name in the input dataset that contains the license information. The default column name is license.

- The optional `lc_allow_no_license` option allows any records without a license to be accepted by the filter. If this option is not set, records without a license are rejected.

- The required `lc_licenses_file` options allows a list of licenses to be specified. An S3 or local file path should be supplied (including bucket name, for example: bucket-name/path/to/licenses.json) with the file contents being a JSON list of strings. For example:

[
  'Apache-2.0',
  'MIT'
]

- The optional `lc_deny_licenses` flag is used when `lc_licenses_file` specifies the licenses that will be rejected, with all other licenses being accepted. These parameters do not affect handling of records with no license information, which is dictated by the allow_no_license option.


## Running

### Launched Command Line Options 

When running the transform with the TransformLauncher, the following command line arguments are available in addition to the options provided by the python launcher.

  --lc_license_column_name LICENSE_COLUMN_NAME
                        Name of the column holds the data to process
  --lc_allow_no_license
                        allow entries with no associated license (default: false)
  --lc_licenses_file LICENSES_FILE
                        S3 or local path to allowed/denied licenses JSON file
  --lc_deny_licenses    allow all licences except those in licenses_file (default: false)

### Running the samples

To run the samples, use the following make targets

`run-cli-sample`
`run-local-python-sample` 

These targets will activate the virtual environment and set up any configuration needed. Use the -n option of make to see the detail of what is done to run the sample.

For example,
```
make run-cli-sample

```
...
Then

ls output
To see results of the transform.

