import requests
import logging
from datetime import datetime, timezone
from retrying import retry
import time

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

API_URL = 'http://localhost:3123/animals/v1'
BATCH_SIZE = 100
MAX_RETRY_ATTEMPTS = 5

# Retry configuration with exponential backoff for handling server errors
def retry_if_server_error(exception):
    """Return True if we should retry (in this case when it's an HTTP 5xx error)."""
    return isinstance(exception,
                      requests.exceptions.RequestException) and exception.response is not None and 500 <= exception.response.status_code < 600

class AnimalETL:
    def __init__(self, api_url, batch_size, max_retry_attempts):
        self.api_url = api_url
        self.batch_size = batch_size
        self.max_retry_attempts = max_retry_attempts

    @retry(retry_on_exception=retry_if_server_error, wait_exponential_multiplier=1000, wait_exponential_max=16000,
           stop_max_attempt_number=MAX_RETRY_ATTEMPTS)
    def get_animals(self, page):
        """Fetch animals from the paginated API."""
        response = requests.get(f'{self.api_url}/animals', params={'page': page})
        response.raise_for_status()
        return response.json()

    def transform_animal(self, animal):
        """Transform the 'friends' and 'born_at' fields of an animal."""
        if 'friends' in animal and animal['friends']:
            animal['friends'] = animal['friends'].split(',')
        else:
            animal['friends'] = []
        
        if 'born_at' in animal and animal['born_at'] is not None:
            try:
                animal['born_at'] = datetime.fromtimestamp(animal['born_at'] / 1000, tz=timezone.utc).isoformat()
            except Exception as e:
                logging.error(f"Error converting 'born_at' for animal {animal['id']}: {e}")
                animal['born_at'] = None

        return animal

    @retry(retry_on_exception=retry_if_server_error, wait_exponential_multiplier=1000, wait_exponential_max=16000,
           stop_max_attempt_number=MAX_RETRY_ATTEMPTS)
    def post_animals(self, batch):
        """Post a batch of animals to the home endpoint."""
        logging.debug(f"Posting batch of animals: {batch}")
        
        for animal in batch:
            if not isinstance(animal.get('friends'), list):
                logging.error(f"Validation Error: 'friends' should be a list. Found: {animal.get('friends')}")
                return
            if animal.get('born_at') is not None and not isinstance(animal['born_at'], str):
                logging.error(f"Validation Error: 'born_at' should be a string or None. Found: {animal['born_at']}")
                return

        response = requests.post(f'{self.api_url}/home', json=batch)
        response.raise_for_status()
        return response.json()

    def fetch_all_animals(self):
        """Fetch and return all animals from the API."""
        animals = []
        page = 1
        while True:
            try:
                start_time = time.time()
                result = self.get_animals(page)
                logging.debug(f'API Response for page {page}: {result}')

                if 'items' not in result:
                    logging.error(f"Key 'items' not found in the response: {result}")
                    break

                if not result['items']:
                    break

                animals.extend(result['items'])
                logging.info(f'Fetched {len(result["items"])} animals from page {page}')
                page += 1

                elapsed_time = time.time() - start_time
                if elapsed_time > 5:
                    logging.warning(
                        f"Server pause detected, response took {elapsed_time:.2f} seconds. Pausing for 5 seconds.")
                    time.sleep(5)

            except requests.exceptions.RequestException as e:
                logging.error(f'Error fetching animals: {e}')
                break
        return animals

    def run_etl(self):
        """Run the complete ETL process for animals data."""
        # Step 1: Fetch all animals
        logging.info("Fetching all animal details...")
        animals = self.fetch_all_animals()

        if not animals:
            logging.error("No animals data fetched. Exiting...")
            return

        # Step 2: Transform animal data
        logging.info("Transforming animal data...")
        transformed_animals = [self.transform_animal(animal) for animal in animals]

        # Step 3: Post animals in batches of 100
        logging.info("Posting animal data in batches...")
        for i in range(0, len(transformed_animals), self.batch_size):
            batch = transformed_animals[i:i + self.batch_size]
            try:
                self.post_animals(batch)
                logging.info(f"Successfully posted batch {i // self.batch_size + 1}")
            except requests.exceptions.RequestException as e:
                logging.error(f'Error posting animals: {e}')


if __name__ == '__main__':
    animal_etl = AnimalETL(api_url=API_URL, batch_size=BATCH_SIZE, max_retry_attempts=MAX_RETRY_ATTEMPTS)
    animal_etl.run_etl()