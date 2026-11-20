<<<<<<< Updated upstream
FROM public.ecr.aws/lambda/python:3.15.2026.11.19.13@sha256:daedfd8cfccf4cf3e64b2f1b650f50f5bcf5500b4cc3fe355030575d3f41672c
=======
FROM public.ecr.aws/lambda/python:3.14.2026.11.19.13-x86_64@sha256:98477693a99ed67b0176b20428d9400506465ace75856020a1e94241c014d3e4
>>>>>>> Stashed changes

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
