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

        self.license_check = config.get(LICENSE_CHECK_PARAMS)
        if not self.license_check:
            raise ValueError(f"Invalid Argument: cannot create LicenseCheckTransform object.")
        logger.debug(f"LICENSE_CHECK_PARAMS: {self.license_check}")
        self.license_column = self.license_check["license_column_name"]
        allow_no_license = self.license_check["allow_no_license"]
        licenses = self.license_check["licenses"]
        deny = self.license_check["deny"]
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
        return [new_table], {"n_files": 1}


class LicenseCheckTransformConfiguration(TransformConfiguration):
    def __init__(self):
        super().__init__(name="license_check", transform_class=LicenseCheckTransform)

    def add_input_params(self, parser: ArgumentParser) -> None:
        parser.add_argument(
            "--lc_license_column_name",
            required=False,
            type=str,
            dest="license_column_name",
            default="contents",
            help="Name of the column holds the data to process",
        )
        parser.add_argument(
            "--lc_allow_no_license",
            required=False,
            dest="allow_no_license",
            action="store_true",
            default=False,
            help="allow entries with no associated license (default: false)",
        )
        parser.add_argument(
            "--lc_licenses_file",
            required=True,
            dest="licenses_file",
            type=str,
            help="S3 or local path to allowed/denied licenses JSON file",
        )
        parser.add_argument(
            "--lc_deny_licenses",
            required=False,
            dest="deny",
            action="store_true",
            default=False,
            help="allow all licences except those in licenses_file (default: false)",
        )

    def apply_input_params(self, args: Namespace) -> bool:
        dargs = vars(args)
        # Read licenses from allow-list or deny-list
        data_access = DataAccessFactory("lc_", False).create_data_access()
        deny = dargs.get("deny")
        license_list_file = dargs.get("licenses_file")
        licenses = _get_supported_licenses(license_list_file, data_access)
        self.params = {
            LICENSE_CHECK_PARAMS: {
                "license_column_name": dargs.get("license_column_name"),
                "allow_no_license": dargs.get("allow_no_license"),
                "licenses": licenses,
                "deny": deny,
            }
        }
        return True
