FROM public.ecr.aws/lambda/python:3.15.2026.11.13.18@sha256:4fbad1f61c74026f42a6b071ce3e103f2b419b8f73e724ef622cacc85e9f67d6

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
