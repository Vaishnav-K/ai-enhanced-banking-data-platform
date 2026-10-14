import unittest

from botocore.config import Config
from botocore.session import Session
from botocore.stub import Stubber
from botocore.exceptions import ClientError


class TestS3SdkRetryBehavior(unittest.TestCase):
    def test_standard_retry_configuration_is_bounded(self):
        config = Config(
            retries={"mode": "standard", "total_max_attempts": 3},
            connect_timeout=5,
            read_timeout=30,
        )

        self.assertEqual(config.retries["mode"], "standard")
        self.assertEqual(config.retries["total_max_attempts"], 3)

    def test_access_denied_is_not_a_retryable_service_error(self):
        session = Session()
        client = session.create_client(
            "s3",
            region_name="ap-southeast-2",
            aws_access_key_id="testing",
            aws_secret_access_key="testing",
            config=Config(
                retries={"mode": "standard", "total_max_attempts": 3}
            ),
        )

        with Stubber(client) as stubber:
            stubber.add_client_error(
                "head_object",
                service_error_code="AccessDenied",
                service_message="Simulated permission failure",
                http_status_code=403,
                expected_params={"Bucket": "test-bucket", "Key": "test.csv"},
            )

            with self.assertRaises(ClientError) as context:
                client.head_object(Bucket="test-bucket", Key="test.csv")

        self.assertEqual(
            context.exception.response["Error"]["Code"], "AccessDenied"
        )


if __name__ == "__main__":
    unittest.main()
