# Licensed under the Apache License, Version 2.0 (the “License”);
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#  http://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an “AS IS” BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

################################################################################


import json
from argparse import ArgumentParser, Namespace

import pyarrow as pa
from data_processing.data_access import DataAccess, DataAccessFactory
from data_processing.transform import AbstractTableTransform, TransformConfiguration
from data_processing.utils import TransformUtils, get_logger
from transformer import AllowLicenseStatusTransformer, DenyLicenseStatusTransformer


logger = get_logger(__name__)

LICENSE_CHECK_PARAMS = "license_check_params"

shortname = "lc"
CLI_PREFIX = f"{shortname}_"
LICENSE_COLUMN_NAME_KEY = f"{CLI_PREFIX}license_column_name"
DENY_LICENSES_KEY = f"{CLI_PREFIX}deny_licenses"
LICENSES_FILE_KEY = f"{CLI_PREFIX}licenses_file"
ALLOW_NO_LICENSE_KEY = f"{CLI_PREFIX}allow_no_license"
LICENSE_COLUMN_DEFAULT = "license"


def _get_supported_licenses(license_file: str, data_access: DataAccess) -> list[str]:
    logger.info(f"Getting supported licenses from file {license_file}")
    licenses_list = None
    try:
        licenses_list_json, _ = data_access.get_file(license_file)
        licenses_list = json.loads(licenses_list_json.decode("utf-8"))
        logger.info(f"Read a list of {len(licenses_list)} licenses.")
    except Exception as e:
        logger.error(f"Failed to read file: {license_file} due to {e}")
    return licenses_list


class LicenseCheckTransform(AbstractTableTransform):
    """It can be used to select the rows/records of data with licenses
    matching those in the approved/deny list.
    """

    def __init__(self, config: dict):
        super().__init__(config)

        try:
            self.license_check = config.get(LICENSE_CHECK_PARAMS)
            self.license_column = self.license_check.get("license_column_name", LICENSE_COLUMN_DEFAULT)
            allow_no_license = self.license_check.get("allow_no_license", False)
            licenses = self.license_check.get("licenses", None)
            if not licenses or not isinstance(licenses, list):
                raise ValueError("license list not found.")
            deny = self.license_check.get("deny", False)
            logger.debug(f"LICENSE_CHECK_PARAMS: {self.license_check}")
        except Exception as e:
            raise ValueError(f"Invalid Argument: cannot create LicenseCheckTransform object: {e}.")

        if not deny:
            self.transformer = AllowLicenseStatusTransformer(
                license_column=self.license_column,
                allow_no_license=allow_no_license,
                licenses=licenses,
            )
        else:
            self.transformer = DenyLicenseStatusTransformer(
                license_column=self.license_column,
                allow_no_license=allow_no_license,
                licenses=licenses,
            )

    def transform(self, table: pa.Table, file_name: str = None) -> tuple[list[pa.Table], dict]:
        """
        Transforms input tables by adding a boolean `license_status` column
        indicating whether the license is approved/denied.
        """
        TransformUtils.validate_columns(table=table, required=[self.license_column])
        new_table = self.transformer.transform(table)
        return [new_table], {}


class LicenseCheckTransformConfiguration(TransformConfiguration):
    def __init__(self):
        super().__init__(name="license_check", transform_class=LicenseCheckTransform)

    def add_input_params(self, parser: ArgumentParser) -> None:
        parser.add_argument(
            f"--{LICENSE_COLUMN_NAME_KEY}",
            required=False,
            type=str,
            default=LICENSE_COLUMN_DEFAULT,
            help="Name of the column holds the data to process",
        )
        parser.add_argument(
            f"--{ALLOW_NO_LICENSE_KEY}",
            required=False,
            action="store_true",
            default=False,
            help="allow entries with no associated license (default: false)",
        )
        parser.add_argument(
            f"--{LICENSES_FILE_KEY}",
            required=True,
            type=str,
            help="S3 or local path to allowed/denied licenses JSON file",
        )
        parser.add_argument(
            f"--{DENY_LICENSES_KEY}",
            required=False,
            action="store_true",
            default=False,
            help="allow all licences except those in licenses_file (default: false)",
        )
        # Create the DataAccessFactor to use CLI args
        self.daf = DataAccessFactory(CLI_PREFIX, False)
        # Add the DataAccessFactory parameters to the transform's configuration parameters.
        self.daf.add_input_params(parser)

    def apply_input_params(self, args: Namespace) -> bool:
        dargs = vars(args)
        data_access = self.daf.create_data_access()
        deny = dargs.get(DENY_LICENSES_KEY, False)
        license_list_file = dargs.get(LICENSES_FILE_KEY)
        # Read licenses from allow-list or deny-list
        licenses = _get_supported_licenses(license_list_file, data_access)
        self.params = {
            LICENSE_CHECK_PARAMS: {
                "license_column_name": dargs.get(LICENSE_COLUMN_NAME_KEY),
                "allow_no_license": dargs.get(ALLOW_NO_LICENSE_KEY),
                "licenses": licenses,
                "deny": deny,
            }
        }
        return True
