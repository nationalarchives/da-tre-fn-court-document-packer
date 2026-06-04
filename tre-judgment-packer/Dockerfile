FROM public.ecr.aws/lambda/python:3.13.2026.06.04.00@sha256:899f05ee2ac63bf38b2a42053981dba678cba9866ed7ec9de6c70a3e7646f1d7

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
