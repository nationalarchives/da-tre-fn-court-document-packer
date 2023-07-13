FROM public.ecr.aws/lambda/python:3.8

ARG PIP_INDEX_URL

# Copy function code
COPY "tre_judgment_packer.py" ${LAMBDA_TASK_ROOT}

# Install the function's dependencies using file requirements.txt
COPY requirements.txt ./requirements.txt

RUN export PIP_VERBOSE=1 && \
    echo "${PIP_INDEX_URL}" >> /var/log/pip_index_url.log && \
    yum -y update && \
    yum clean all && \
    pip3 install --requirement requirements.txt --target "${LAMBDA_TASK_ROOT}" > /var/log/pip_install.log 2>&1


# Set the CMD to your handler (could also be done as a parameter override outside of the Dockerfile)
CMD [ "tre_judgment_packer.handler" ]
