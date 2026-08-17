FROM public.ecr.aws/lambda/python:3.14.2026.08.14.13@sha256:ae83d2a00c0378ae8838f8258c86c1063ae0a8ebb6e90ac67e5d74a2f1089803

# Loading Variables from Github Actions environment
ARG PIP_INDEX_URL

# Copy function code
COPY "tre_judgment_packer.py" ${LAMBDA_TASK_ROOT}

# Install the function's dependencies using file requirements.txt
COPY requirements.txt ./requirements.txt
RUN pip3 install --no-cache-dir --requirement requirements.txt --target "${LAMBDA_TASK_ROOT}"

RUN pip install --no-cache-dir legacy-cgi==2.6.2
# Run as non-root user
USER nobody

# Set the CMD to your handler (could also be done as a parameter override outside of the Dockerfile)
CMD [ "tre_judgment_packer.handler" ]
