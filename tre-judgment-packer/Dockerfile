FROM public.ecr.aws/lambda/python:3.14.2026.10.08.12@sha256:208b9b8e1a2277d4f8e0dbe3073f846d47919300a69584864f9f8f9c112ad5b4

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
