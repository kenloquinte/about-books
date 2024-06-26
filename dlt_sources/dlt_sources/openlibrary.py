import dlt
from dlt.sources.helpers import requests
import time

@dlt.source
def openlibrary_source(api_secret_key=dlt.secrets.value):
    @dlt.resource(write_disposition="replace")
    def openlibrary_books():
        headers = _create_auth_headers(api_secret_key)
        
        subjects = ["adventure", "romance", "fantasy", "horror", "lgbtq"]

        for subject in subjects:
            url = f'https://openlibrary.org/subjects/{subject}.json'
            response = requests.get(url, headers=headers)
            response.raise_for_status()
            for work in response.json()['works']:
                work['genre'] = subject
                yield work

    @dlt.transformer(data_from=openlibrary_books, primary_key="key", write_disposition="replace") #primary_key does not seem to work, check
    def openlibrary_author(work_item):
        headers = _create_auth_headers(api_secret_key)
        
        for author in work_item['authors']:
            author_key = author['key']
            url = f'https://openlibrary.org{author_key}.json'
            response = requests.get(url, headers=headers)
            response.raise_for_status()
            author_details = response.json()
            author_details['book_key'] = work_item['key'] 
            time.sleep(3)
            yield author_details


    return openlibrary_books, openlibrary_author


def _create_auth_headers(api_secret_key):
    """Constructs Bearer type authorization header which is the most common authorization method"""
    headers = {"Authorization": f"Bearer {api_secret_key}"}
    return headers
