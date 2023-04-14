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
    "TRE_S3_JUDGMENT_OUT_BUCKET", must_exist=True, must_have_value=True
)
PROCESS = common_lib.get_env_var(
    "TRE_PROCESS_NAME", must_exist=True, must_have_value=True
)
PRODUCER = common_lib.get_env_var(
    "TRE_SYSTEM_NAME", must_exist=True, must_have_value=True
)
URL_EXPIRY = common_lib.get_env_var(
    "TRE_PRESIGNED_URL_EXPIRY", must_exist=True, must_have_value=True
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

# Message out Vars

KEY_S3_FOLDER_URL = "bundleFileURI"
META_DATA_FILE_PATH = "metadataFilePath"
META_DATA_FILE_TYPE = "metadataFileType"

# Lambda vars

EVENT_NAME_INPUT = "tre-judgment-parsed"
EVENT_NAME_OUTPUT_OK = "tre-judgment-package-available"
EVENT_NAME_OUTPUT_ERROR = "packed-judgment-error"


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

        # Generate pre-signed URL
        s3 = boto3.client("s3")

        try:
            s3_presigned_link = s3.generate_presigned_url(
                "get_object",
                Params={
                    "Bucket": OUT_BUCKET,
                    "Key": packed_judgment_file_name,
                },
                ExpiresIn=URL_EXPIRY,
            )
            logger.info(
                f"packed file link generated for {packed_judgment_file_name} in {s3_key_file_path} folder in {OUT_BUCKET} bucket"
            )
        except ClientError as e:
            logging.error(e)
            return None

        # event output not currently externally validated. This should be changed ASAP.

        event_output_success = {
            "properties": {
                "messageType": "uk.gov.nationalarchives.tre.messages.judgmentpackage.available.JudgmentPackageAvailable",
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
                "bundleFileURI": s3_presigned_link,
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
