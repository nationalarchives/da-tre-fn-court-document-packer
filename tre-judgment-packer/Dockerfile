FROM public.ecr.aws/lambda/python:3.13.2026.04.27.23@sha256:6716d749d53c5c1bc113770f647911c4db27fc947cf5a32ea78227314debe343

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
