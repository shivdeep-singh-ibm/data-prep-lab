
# Define the root of the local git clone for the common rules to be able
# know where they are running from.
REPOROOT=../../../..
include $(REPOROOT)/transforms/.make.transforms

TRANSFORM_NAME=license_check
# $(REPOROOT)/.make.versions file contains the versions
DOCKER_IMAGE_VERSION=${LICENSE_CHECK_PYTHON_VERSION}

# Use default rule inherited from makefile.common
clean:: .transforms.clean

# Use default rule inherited from makefile.common
test:: .transforms.python-test

# Use default rule inherited from makefile.common
image:: .transforms.python-image

# Use default rule inherited from makefile.common
venv:: .transforms.python-venv

test-src:: .transforms.test-src

test-image:: .transforms.python-test-image

run-cli-sample:
	$(MAKE) RUN_FILE=$(TRANSFORM_NAME)_transform_python.py \
                RUN_ARGS="--data_local_config \"{ 'input_folder' : '../test-data/input', 'output_folder' : '../test-data/output'}\"  \
		 --lc_license_column_name license							\
    	 --lc_licenses_file  ../test-data/sample_approved_licenses.json" 					\
		.transforms.run-src-file

run-local-python-sample:
	#$(MAKE) RUN_FILE=dpk_license_check_python/$(TRANSFORM_NAME)_local_pure.py .transforms.run-src-file
	$(MAKE) RUN_FILE=$(TRANSFORM_NAME)_local_python.py .transforms.run-src-file

build:: build-dist image

publish:: publish-dist publish-image

publish-image:: .transforms.publish-image-python

setup:: .transforms.setup

# distribution versions is the same as image version.
set-versions:
	$(MAKE) TRANSFORM_PYTHON_VERSION=${CODE_QUALITY_PYTHON_VERSION} TOML_VERSION=$(DOCKER_IMAGE_VERSION) .transforms.set-versions 
        
build-dist:: set-versions .defaults.build-dist 

publish-dist:: .defaults.publish-dist

setup:: .transforms.setup

run-local-python-sample: .transforms.run-local-python-sample

load-image:: .transforms.load-image
