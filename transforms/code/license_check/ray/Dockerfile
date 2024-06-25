FROM docker.io/rayproject/ray:2.9.3-py310

# install pytest
RUN pip install --no-cache-dir pytest

# Copy and install data processing libraries 
# These are expected to be placed in the docker context before this is run (see the make image).
COPY --chown=ray:users data-processing-lib-python/ data-processing-lib-python/
RUN cd data-processing-lib-python && pip install --no-cache-dir -e .
COPY --chown=ray:users data-processing-lib-ray/ data-processing-lib-ray/ 
RUN cd data-processing-lib-ray    && pip install --no-cache-dir -e .

COPY --chown=ray:users python-transform lc_python
RUN cd lc_python && pip install .


# copy source data
COPY src/license_check_transform_ray.py .
COPY src/license_check_local_ray.py local/


COPY --chown=ray:users src/ ray_tf/src/
COPY --chown=ray:users README.md ray_tf/
COPY --chown=ray:users pyproject.toml ray_tf/pyproject.toml 
RUN cd ray_tf && pip install .


# copy test
COPY test/ test/
COPY test-data/ test-data/
ENV PATH $PATH:/home/dpk/.local/bin/

# Set environment
ENV PYTHONPATH /home/ray

# Put these at the end since they seem to upset the docker cache.
ARG BUILD_DATE
ARG GIT_COMMIT
LABEL build-date=$BUILD_DATE
LABEL git-commit=$GIT_COMMIT

