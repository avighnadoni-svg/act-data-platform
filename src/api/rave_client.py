import os
import time
import requests

from dotenv import load_dotenv

load_dotenv()

BASE_URL = os.getenv("RAVE_API_BASE_URL")
TOKEN = os.getenv("RAVE_CODESPACE_TOKEN")


def get_api_data(endpoint: str, params: dict | None = None):

    if not BASE_URL:
        raise ValueError("RAVE_API_BASE_URL is missing")

    if not TOKEN:
        raise ValueError("RAVE_CODESPACE_TOKEN is missing")

    url = f"{BASE_URL.rstrip('/')}/{endpoint.lstrip('/')}"

    headers = {
        "X-Github-Token": TOKEN
    }

    max_retries = 3

    for attempt in range(1, max_retries + 1):

        try:
            print(f"Calling: {url}")

            response = requests.get(
                url,
                headers=headers,
                params=params,
                timeout=30
            )

            # Rate limit
            if response.status_code == 429:
                wait_time = int(
                    response.headers.get("Retry-After", 2)
                )

                print(f"Rate limited. Waiting {wait_time} seconds...")
                time.sleep(wait_time)
                continue

            response.raise_for_status()

            content_type = response.headers.get(
                "content-type",
                ""
            )

            if "application/json" not in content_type:
                raise ValueError(
                    f"Expected JSON but received {content_type}"
                )

            data = response.json()

            print(f"Records received: {len(data)}")

            return data

        except requests.RequestException as error:

            print(
                f"Attempt {attempt}/{max_retries} failed: {error}"
            )

            if attempt == max_retries:
                raise

            time.sleep(2 ** attempt)