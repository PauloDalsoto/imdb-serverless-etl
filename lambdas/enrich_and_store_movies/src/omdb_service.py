import requests
from .utils import with_retries

class OMDBService:
    def __init__(self, base_url, max_retries, base_delay, logger):
        self.base_url = base_url
        self.max_retries = max_retries
        self.base_delay = base_delay
        self.logger = logger

    def _get(self, url):
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        return response.json()

    def fetch_movie_data(self, imdb_id, api_key):
        if not self.base_url:
            raise ValueError("OMDB_URL not set.")

        url = f"{self.base_url}/?apikey={api_key}&i={imdb_id}"

        try:
            data = with_retries(
                self.logger,
                self.max_retries,
                self.base_delay,
                self._get,
                f"Calling OMDb API for {imdb_id}",
                url
            )
            if data.get("Response") == "False":
                self.logger.warning(f"OMDb API returned error for {imdb_id}: {data.get('Error')}")
                return None
            return data
        except Exception as e:
            self.logger.error(f"OMDb API failed for {imdb_id}: {e}")
            return None
