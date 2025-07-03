import json
from .utils import with_retries

class SecretsService:
    def __init__(self, client, max_retries, base_delay, logger):
        self.client = client
        self.max_retries = max_retries
        self.base_delay = base_delay
        self.logger = logger

    def get_secret_string(self, secret_name):
        response = with_retries(
            self.logger,
            self.max_retries,
            self.base_delay,
            self.client.get_secret_value,
            f"Retrieving secret: {secret_name}",
            SecretId=secret_name
        )
        return response.get("SecretString")

    def get_omdb_api_key(self, secret_name):
        secret_string = self.get_secret_string(secret_name)
        if not secret_string:
            raise ValueError("OMDb API secret is empty.")

        try:
            secret_json = json.loads(secret_string)
            return secret_json.get("omdbapi_key")
        except json.JSONDecodeError:
            raise ValueError("Secret string is not valid JSON.")
