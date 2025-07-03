import requests
from .utils import with_retries

class IMDBService:
    def __init__(self, url, max_retries, base_delay, logger):
        self.url = url
        self.max_retries = max_retries
        self.base_delay = base_delay
        self.logger = logger

    def _fetch(self, url):
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        return response.json()

    def fetch_movie_data(self):
        return with_retries(
            self.logger,
            self.max_retries,
            self.base_delay,
            self._fetch,
            f"Fetching data from: {self.url}",
            self.url
        )

    def get_top_rated_movies(self, items, top_n):
        try:
            valid_movies = []
            for m in items:
                if 'rank' in m and m['rank'] not in ('', 'N/A'):
                    try:
                        m['rank'] = int(m['rank'])
                        valid_movies.append(m)
                    except ValueError:
                        self.logger.warning(f"Skipping movie with invalid rank: {m.get('id', 'N/A')}")
                        continue

            sorted_movies = sorted(valid_movies, key=lambda m: m['rank'])
            return sorted_movies[:top_n]

        except Exception as e:
            self.logger.error(f"Error sorting movies: {e}")
            return []
