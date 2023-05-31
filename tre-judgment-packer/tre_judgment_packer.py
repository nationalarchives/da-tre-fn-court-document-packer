import logging
import boto3
from botocore.exceptions import ClientError
import s3_lib.object_lib
from s3_lib import tar_lib
from s3_lib import common_lib
from datetime import datetime

# Set global logging options; AWS environment may override this though
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Env vars

ENVIRONMENT = common_lib.get_env_var(
    "TRE_ENVIRONMENT", must_exist=True, must_have_value=True
)
OUT_BUCKET = common_lib.get_env_var(
    "TRE_S3_COURT_DOCUMENT_PACKED_OUT_BUCKET", must_exist=True, must_have_value=True
)
PROCESS = common_lib.get_env_var(
    "TRE_PROCESS_NAME", must_exist=True, must_have_value=True
)
PRODUCER = common_lib.get_env_var(
    "TRE_SYSTEM_NAME", must_exist=True, must_have_value=True
)

# Message in Vars

KEY_S3_OBJECT_ROOT = "s3FolderName"
ORIGINATOR = "originator"
STATUS = "status"
PARAMETERS = "parameters"
EXECUTION_ID = "executionId"
PARENT_EXECUTION_ID = "parentExecutionId"
PARSED_JUDGMENT_FILE_PATH = "s3FolderName"
PROPERTIES = "properties"
REFERENCE = "reference"
SOURCE_BUCKET_NAME = "s3Bucket"


def handler(event, context):
    """
    Given the presence of a parsed judgment, this lambda zips up the contents of all files and generates a
    one time link in s3 to be sent onto the caselaw team for consumption.
    """

    logger.info(f'handler start: event="{event}"')

    # Grabbing parameters from source event message.
    s3_source_bucket = event[PARAMETERS][SOURCE_BUCKET_NAME]
    reference = event[PARAMETERS][REFERENCE]
    parsed_judgment_file_path = event[PARAMETERS][KEY_S3_OBJECT_ROOT]
    execution_id = event[PROPERTIES][EXECUTION_ID]
    parent_execution_id = event[PROPERTIES][PARENT_EXECUTION_ID]
    originator = event[PARAMETERS][ORIGINATOR]
    status = event[PARAMETERS][STATUS]

    # produce timestamp for message
    timestamp = datetime.now().isoformat() + 'Z'

    try:

        logger.info(
            f"Packing files of ref: {reference} found at {parsed_judgment_file_path} in "
            f"the {s3_source_bucket} and bundling to s3 bucket : {OUT_BUCKET}"
        )

        # Create a list of items to be zipped

        files_to_zip = s3_lib.object_lib.s3_ls(
            bucket_name=s3_source_bucket, object_filter=f"{parsed_judgment_file_path}"
        )

        # construct file path
        s3_key_file_path = f"{reference}/{execution_id}/"
        # create a name for the package based on reference
        packed_judgment_file_name = f"{reference}.tar.gz"
        # s3 object key with name
        s3_file_path_with_file_name = f"{s3_key_file_path}{packed_judgment_file_name}"

        # tar all the objects up into the package.

        logger.info(
            f"packing {len(files_to_zip)} files in package {packed_judgment_file_name}"
        )

        s3_lib.tar_lib.s3_objects_to_s3_tar_gz_file(
            s3_bucket_in=s3_source_bucket,
            s3_object_names=files_to_zip,
            tar_gz_object=s3_file_path_with_file_name,
            s3_bucket_out=OUT_BUCKET,
        )

        event_output_success = {
            "properties": {
                "messageType": "uk.gov.nationalarchives.tre.messages.courtdocumentpackage.available.CourtDocumentPackageAvailable",
                "timestamp": timestamp,
                "function": PROCESS,
                "producer": PRODUCER,
                "executionId": execution_id,
                "parentExecutionId": parent_execution_id,
            },
            "parameters": {
                "status": status,
                "reference": reference,
                "originator": originator,
                "s3Bucket": OUT_BUCKET,
                "s3Key": s3_file_path_with_file_name,
                "metadataFilePath": "/metadata.json",
                "metadataFileType": "Json",
            },
        }

        logger.info(f"event_output_success:\n%s\n", event_output_success)
        return event_output_success

    except ValueError as e:
        logging.error("handler error: %s", str(e))

        # generic error message

        event_output_error = {
            "properties": {
                "messageType": "uk.gov.nationalarchives.tre.messages.Error",
                "timestamp": timestamp,
                "function": PROCESS,
                "producer": PRODUCER,
                "executionId": execution_id,
                "parentExecutionId": parent_execution_id,
            },
            "parameters": {
                "reference": reference,
                "originator": originator,
                "errors": str(e),
            },
        }

        logger.info(f"event_output_error:\n%s\n", event_output_error)
        return event_output_error
