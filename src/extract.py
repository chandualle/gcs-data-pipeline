from pathlib import Path

import requests

from config.pipeline_config import RAW_DATA_PATH, SOURCE_URL


def download_source() -> Path:
    """Download the source Parquet file if it isn't already present."""
    destination = Path(RAW_DATA_PATH)
    destination.parent.mkdir(parents=True, exist_ok=True)

    if destination.exists() and destination.stat().st_size > 0:
        print(f"Source already exists: {destination}")
        return destination

    temporary_path = destination.with_suffix(
        destination.suffix + ".part"
    )

    try:
        with requests.get(
            SOURCE_URL,
            stream=True,
            timeout=(15, 120),
        ) as response:
            response.raise_for_status()

            with temporary_path.open("wb") as output:
                for chunk in response.iter_content(
                    chunk_size=1024 * 1024
                ):
                    if chunk:
                        output.write(chunk)

        temporary_path.replace(destination)
        print(f"Downloaded source to {destination}")
        return destination

    finally:
        temporary_path.unlink(missing_ok=True)


if __name__ == "__main__":
    download_source()