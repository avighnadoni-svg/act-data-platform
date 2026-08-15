from pathlib import Path

import pandas as pd


TEMP_DIR = Path("data/temp")


def json_to_csv(
    data: list[dict],
    entity_name: str
) -> str:

    if not data:
        print(f"No data received for {entity_name}")
        return ""

    TEMP_DIR.mkdir(
        parents=True,
        exist_ok=True
    )

    file_path = TEMP_DIR / f"{entity_name}.csv"

    df = pd.DataFrame(data)

    df.to_csv(
        file_path,
        index=False
    )

    print(
        f"{entity_name}: "
        f"{len(df)} records written to {file_path}"
    )

    return str(file_path)