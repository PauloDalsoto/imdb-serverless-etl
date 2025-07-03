import json
import uuid
from .utils import with_retries

class SQSService:
    def __init__(self, sqs_client, queue_url, max_retries, base_delay, logger):
        self.sqs = sqs_client
        self.queue_url = queue_url
        self.max_retries = max_retries
        self.base_delay = base_delay
        self.logger = logger

    def _send_single(self, message_body, message_group_id, deduplication_id):
        self.sqs.send_message(
            QueueUrl=self.queue_url,
            MessageBody=message_body,
            MessageGroupId=message_group_id,
            MessageDeduplicationId=deduplication_id
        )
        return True

    def send_batch(self, batch, is_final_batch=False):
        message_body = json.dumps({
            "movies": batch,
            "is_final_batch": is_final_batch
        })

        return with_retries(
            self.logger,
            self.max_retries,
            self.base_delay,
            self._send_single,
            f"Sending batch with {len(batch)} movies to SQS",
            message_body,
            "movies-group",
            str(uuid.uuid4())
        )
