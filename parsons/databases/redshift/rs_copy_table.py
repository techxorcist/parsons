import logging
import os
import time

import boto3
from botocore.client import ClientError

from parsons.aws.s3 import S3

logger = logging.getLogger(__name__)

S3_TEMP_KEY_PREFIX = "Parsons_RedshiftCopyTable"


class RedshiftCopyTable(object):
    def __init__(
        self,
        aws_access_key_id=None,
        aws_secret_access_key=None,
        aws_session_token=None,
        s3_temp_bucket=None,
        iam_role=None,
        role_arn=None,
        use_env_token=True,
    ):
        """
        Initialize RedshiftCopyTable with credential and configuration options.

        `Args:`
            aws_access_key_id: str
                AWS access key ID. Optional if using environment variables or role assumption.
            aws_secret_access_key: str
                AWS secret access key. Optional if using environment variables or role assumption.
            aws_session_token: str
                AWS session token for temporary credentials. Optional.
            s3_temp_bucket: str
                S3 bucket for temporary file storage during copy operations.
            iam_role: str
                AWS IAM Role ARN for Redshift service-side role assumption (deprecated).
                Use role_arn for client-side assumption instead.
            role_arn: str
                AWS IAM Role ARN for client-side STS role assumption.
            use_env_token: bool
                Whether to use AWS_SESSION_TOKEN environment variable. Defaults to True.
        """
        # Explicit credential parameters
        self._aws_access_key_id = aws_access_key_id
        self._aws_secret_access_key = aws_secret_access_key
        self._aws_session_token = aws_session_token
        self._s3_temp_bucket = s3_temp_bucket
        self._iam_role = iam_role
        self._role_arn = role_arn
        self.use_env_token = use_env_token

        # Initialize cached credentials and STS client
        self._assumed_credentials = None
        self._sts_client = None

    def _get_attribute_with_fallback(self, attr_name, private_attr_name=None):
        """Get attribute from explicit value."""
        private_attr_name = private_attr_name or f"_{attr_name}"
        # Return the private attribute value, or None if not set
        return getattr(self, private_attr_name, None)

    @property
    def aws_access_key_id(self):
        """Get AWS access key ID from explicit value or parent class."""
        return self._get_attribute_with_fallback("aws_access_key_id")

    @property
    def aws_secret_access_key(self):
        """Get AWS secret access key from explicit value or parent class."""
        return self._get_attribute_with_fallback("aws_secret_access_key")

    @property
    def iam_role(self):
        """Get IAM role from explicit value or parent class (deprecated)."""
        return self._get_attribute_with_fallback("iam_role")

    @property
    def role_arn(self):
        """Get role ARN for STS assumption."""
        return self._get_attribute_with_fallback("role_arn")

    @property
    def s3_temp_bucket(self):
        """Get S3 temp bucket from explicit value or parent class."""
        return self._get_attribute_with_fallback("s3_temp_bucket")

    @property
    def s3_temp_bucket_prefix(self):
        """Get S3 temp bucket prefix from parent class if available."""
        return self._get_attribute_with_fallback("s3_temp_bucket_prefix")

    def assume_role(self, role_arn=None, session_name=None):
        """
        Assume an AWS IAM role using STS and cache the credentials.

        `Args:`
            role_arn: str
                The ARN of the role to assume. If not provided, uses self.role_arn.
            session_name: str
                Optional session name for the assumed role session.

        `Returns:`
            dict
                Dictionary containing temporary credentials (AccessKeyId, SecretAccessKey, SessionToken).
        """
        role_arn = role_arn or self.role_arn
        if not role_arn:
            raise ValueError("No role ARN provided for assumption")

        # Create STS client if not exists
        if not self._sts_client:
            self._sts_client = boto3.client("sts")

        # Generate session name if not provided
        if not session_name:
            session_name = f"parsons-redshift-{int(time.time())}"

        try:
            logger.debug(f"Assuming role: {role_arn}")
            response = self._sts_client.assume_role(
                RoleArn=role_arn,
                RoleSessionName=session_name,
                DurationSeconds=3600,  # 1 hour default
            )

            credentials = response["Credentials"]
            self._assumed_credentials = {
                "access_key": credentials["AccessKeyId"],
                "secret_key": credentials["SecretAccessKey"],
                "session_token": credentials["SessionToken"],
                "expiration": credentials["Expiration"],
            }

            logger.debug("Role assumption successful")
            return self._assumed_credentials

        except ClientError as e:
            error_message = f"""Failed to assume role {role_arn}
            This may be due to insufficient permissions or incorrect role ARN.
            Ensure the current credentials have sts:AssumeRole permission for this role."""
            logger.error(error_message)
            raise e

    def _get_sts_credentials(self):
        """Get credentials from STS role assumption."""
        if not self._assumed_credentials:
            self.assume_role()
        return self._assumed_credentials

    def _get_standard_credentials(self, aws_access_key_id, aws_secret_access_key):
        """Get credentials using standard resolution (existing logic)."""
        aws_session_token = None

        if aws_access_key_id and aws_secret_access_key:
            # When we have credentials, check for session token in environment if enabled
            if self.use_env_token:
                aws_session_token = self._aws_session_token or os.getenv("AWS_SESSION_TOKEN")

        elif self.aws_access_key_id and self.aws_secret_access_key:
            aws_access_key_id = self.aws_access_key_id
            aws_secret_access_key = self.aws_secret_access_key
            # Check for session token in environment if use_env_token is enabled
            if self.use_env_token:
                aws_session_token = self._aws_session_token or os.getenv("AWS_SESSION_TOKEN")

        elif "AWS_ACCESS_KEY_ID" in os.environ and "AWS_SECRET_ACCESS_KEY" in os.environ:
            aws_access_key_id = os.environ["AWS_ACCESS_KEY_ID"]
            aws_secret_access_key = os.environ["AWS_SECRET_ACCESS_KEY"]
            # Check for session token in environment if use_env_token is enabled
            if self.use_env_token:
                aws_session_token = self._aws_session_token or os.getenv("AWS_SESSION_TOKEN")

        else:
            s3 = S3(use_env_token=self.use_env_token)
            creds = s3.aws.session.get_credentials()
            aws_access_key_id = creds.access_key
            aws_secret_access_key = creds.secret_key
            # Extract session token from S3 session credentials if available
            aws_session_token = creds.token

        return aws_access_key_id, aws_secret_access_key, aws_session_token

    def copy_statement(
        self,
        table_name,
        bucket,
        key,
        manifest=False,
        data_type="csv",
        csv_delimiter=",",
        max_errors=0,
        statupdate=None,
        compupdate=None,
        ignoreheader=1,
        acceptanydate=True,
        dateformat="auto",
        timeformat="auto",
        emptyasnull=True,
        blanksasnull=True,
        nullas=None,
        acceptinvchars=True,
        truncatecolumns=False,
        specifycols=None,
        aws_access_key_id=None,
        aws_secret_access_key=None,
        compression=None,
        bucket_region=None,
        json_option="auto",
    ):
        logger.info(f"Data type is {data_type}")
        # Source / Destination
        source = f"s3://{bucket}/{key}"

        # Add column list for mapping or if there are fewer columns on source file
        col_list = f"({', '.join(specifycols)})" if specifycols is not None else ""

        sql = f"copy {table_name}{col_list} \nfrom '{source}' \n"

        # Generate credentials
        sql += self.get_creds(aws_access_key_id, aws_secret_access_key)

        # Other options
        if manifest:
            sql += "manifest \n"
        if bucket_region:
            sql += f"region '{bucket_region}'\n"
            logger.info("Copying data from S3 bucket %s in region %s", bucket, bucket_region)
        sql += f"maxerror {max_errors} \n"

        # Redshift has some default behavior when statupdate is left out
        # vs when it is explicitly set as on or off.
        if statupdate is not None:
            if statupdate:
                sql += "statupdate on\n"
            else:
                sql += "statupdate off\n"

        # Redshift has some default behavior when compupdate is left out
        # vs when it is explicitly set as on or off.
        if compupdate is not None:
            if compupdate:
                sql += "compupdate on\n"
            else:
                sql += "compupdate off\n"

        if ignoreheader:
            sql += f"ignoreheader {ignoreheader} \n"
        if acceptanydate:
            sql += "acceptanydate \n"
        sql += f"dateformat '{dateformat}' \n"
        sql += f"timeformat '{timeformat}' \n"
        if emptyasnull:
            sql += "emptyasnull \n"
        if blanksasnull:
            sql += "blanksasnull \n"
        if nullas:
            sql += f"null as {nullas}"
        if acceptinvchars:
            sql += "acceptinvchars \n"
        if truncatecolumns:
            sql += "truncatecolumns \n"

        # Data Type
        if data_type == "csv":
            sql += f"csv delimiter '{csv_delimiter}' \n"
        elif data_type == "json":
            sql += f"json '{json_option}' \n"
        else:
            raise TypeError("Invalid data type specified.")

        if compression == "gzip":
            sql += "gzip \n"

        sql += ";"

        return sql

    def get_creds(self, aws_access_key_id, aws_secret_access_key):
        """
        Get credentials string for Redshift COPY command with unified IAM support.

        `Args:`
            aws_access_key_id: str
                AWS access key ID (can be None for role-based authentication)
            aws_secret_access_key: str
                AWS secret access key (can be None for role-based authentication)

        `Returns:`
            str
                Formatted credentials string for Redshift COPY command
        """
        # Priority 1: Client-side STS role assumption (NEW)
        if self.role_arn:
            sts_creds = self._get_sts_credentials()
            cred_parts = [
                f"aws_access_key_id={sts_creds['access_key']}",
                f"aws_secret_access_key={sts_creds['secret_key']}",
                f"token={sts_creds['session_token']}",
            ]
            credentials_string = ";".join(cred_parts)
            return f"credentials '{credentials_string}'\n"

        # Priority 2: Redshift service-side role assumption (EXISTING - deprecated)
        elif self.iam_role:
            logger.warning(
                "Using iam_role for service-side role assumption is deprecated. "
                "Consider using role_arn for client-side assumption instead."
            )
            return f"credentials 'aws_iam_role={self.iam_role}'\n"

        # Priority 3: Standard credential resolution (EXISTING with session token support)
        else:
            aws_access_key_id, aws_secret_access_key, aws_session_token = (
                self._get_standard_credentials(aws_access_key_id, aws_secret_access_key)
            )

            # Build credentials string with session token if available
            cred_parts = [
                f"aws_access_key_id={aws_access_key_id}",
                f"aws_secret_access_key={aws_secret_access_key}",
            ]

            if aws_session_token:
                cred_parts.append(f"token={aws_session_token}")

            credentials_string = ";".join(cred_parts)
            return f"credentials '{credentials_string}'\n"

    def temp_s3_copy(
        self,
        tbl,
        aws_access_key_id=None,
        aws_secret_access_key=None,
        csv_encoding="utf-8",
    ):
        if not self.s3_temp_bucket:
            raise KeyError(
                (
                    "Missing S3_TEMP_BUCKET, needed for transferring data to Redshift. "
                    "Must be specified as env vars or kwargs"
                )
            )

        # Coalesce S3 Key arguments
        aws_access_key_id = aws_access_key_id or self.aws_access_key_id
        aws_secret_access_key = aws_secret_access_key or self.aws_secret_access_key

        self.s3 = S3(
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            use_env_token=self.use_env_token,
        )

        hashed_name = hash(time.time())
        key = f"{S3_TEMP_KEY_PREFIX}/{hashed_name}.csv.gz"
        if self.s3_temp_bucket_prefix:
            key = self.s3_temp_bucket_prefix + "/" + key

        # Convert table to compressed CSV file, to optimize the transfers to S3 and to
        # Redshift.
        local_path = tbl.to_csv(temp_file_compression="gzip", encoding=csv_encoding)
        # Copy table to bucket
        self.s3.put_file(self.s3_temp_bucket, key, local_path)

        return key

    def temp_s3_delete(self, key):
        if key:
            self.s3.remove_file(self.s3_temp_bucket, key)
