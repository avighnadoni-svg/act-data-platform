from config.endpoints import ENDPOINTS
from src.api.rave_client import get_api_data
from src.aws.s3_client import upload_json_as_csv_to_s3


def extract_all():

    s3_locations = []

    for entity_name, endpoint in ENDPOINTS.items():

        print(f"\nProcessing: {entity_name}")

        # 1. Call API
        data = get_api_data(endpoint)

        print(f"Records received: {len(data)}")

        # 2. JSON -> CSV -> S3
        s3_path = upload_json_as_csv_to_s3(
            data=data,
            entity_name=entity_name
        )

        if s3_path:
            s3_locations.append(s3_path)

    return s3_locations


if __name__ == "__main__":

    locations = extract_all()

    print("\nFiles uploaded:")

    for location in locations:
        print(location)