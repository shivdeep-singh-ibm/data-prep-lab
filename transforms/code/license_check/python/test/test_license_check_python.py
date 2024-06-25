# (C) Copyright IBM Corp. 2024.
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

import os

from data_processing.runtime.pure_python import PythonTransformLauncher
from data_processing.test_support.launch.transform_test import (
    AbstractTransformLauncherTest,
)
from license_check_transform_python import LicenseCheckPythonTransformConfiguration


class TestPythonLicenseCheck(AbstractTransformLauncherTest):
    """
    Extends the super-class to define the test data for the tests defined there.
    The name of this class MUST begin with the word Test so that pytest recognizes it as a test class.
    """

    def get_test_transform_fixtures(self) -> list[tuple]:
        basedir = "../test-data"
        basedir = os.path.abspath(os.path.join(os.path.dirname(__file__), basedir))
        fixtures = []
        launcher = PythonTransformLauncher(LicenseCheckPythonTransformConfiguration())
        license_file_path = os.path.join(basedir, "sample_approved_licenses.json")
        print(license_file_path)
        config = {
            "lc_license_column_name": "license",
            "lc_licenses_file": license_file_path,
        }
        fixtures.append((launcher, config, basedir + "/input", basedir + "/expected"))
        return fixtures
