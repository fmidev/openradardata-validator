import json
from pathlib import Path
import jsonschema
import os


current_filedir = Path(__file__).parent.resolve()

schema_dir = current_filedir / "schemas"


def main(filename: Path, schema_file: Path):
    if schema_file is None:
        schema_file = schema_dir / "openradardata-spec.json"

    print(f"Schema: {schema_file}")
    if os.path.exists(filename):
        with open(schema_file, encoding="utf-8") as file:
            try:
                schema = json.load(file)
            except json.decoder.JSONDecodeError as e:
                raise ValueError(f"Invalid shema json: {filename}") from e
        with open(filename, encoding="utf-8") as file:
            try:
                data = json.load(file)
            except json.decoder.JSONDecodeError as e:
                raise ValueError(f"Invalid json: {filename}") from e

        print(f"Read msg: {filename}")
        for msg in data:
            jsonschema.validate(instance=msg, schema=schema)
            start_datetime_str = ""
            if "datetime" in msg["properties"]:
                start_datetime_str = msg["properties"]["datetime"]
            else:
                if "start_datetime" in msg["properties"]:
                    start_datetime_str = msg["properties"]["start_datetime"]
            print(
                "Validation OK: {0} {1}\t{2}\t{3}".format(
                    start_datetime_str,
                    msg["properties"]["platform"],
                    msg["properties"]["level"],
                    msg["properties"]["content"]["standard_name"],
                )
            )
    else:
        raise FileNotFoundError(f"File does not exist: {filename}")
