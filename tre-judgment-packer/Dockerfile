FROM public.ecr.aws/lambda/python:3.13.2026.04.17.16@sha256:31814562fc6d222dd84bf659ae2f0a1bb5ac17b7d217e9c734c7612e326e7559

# Loading Variables from Github Actions environment
ARG PIP_INDEX_URL

# Copy function code
COPY "tre_judgment_packer.py" ${LAMBDA_TASK_ROOT}

# Install the function's dependencies using file requirements.txt
COPY requirements.txt ./requirements.txt
RUN dnf upgrade
RUN dnf clean all --enablerepo=\*
RUN pip3 install --requirement requirements.txt --target "${LAMBDA_TASK_ROOT}"

RUN pip install --no-cache-dir legacy-cgi==2.6.2
# Set the CMD to your handler (could also be done as a parameter override outside of the Dockerfile)
CMD [ "tre_judgment_packer.handler" ]
