import json
import jsonschema
import os
import sys

if __name__ == "__main__":

    schema_list = []
    odr_validator_dir = os.getenv("ORD_VALIDATOR_DIR", ".")
    schema_dir = os.getenv("ORD_SCHEMA_DIR", odr_validator_dir + "/schemas")
    # print("Add validator schemas")
    schema_list.append(schema_dir + "/openradardata-spec.json")

    if len(sys.argv) > 1:
        for i, s in enumerate(sys.argv):
            if i > 0:
                schema_list.append(s)
        print("Schemas: {0}".format(schema_list))
        for i, file_name in enumerate(schema_list):
            if os.path.exists(file_name):
                # print(file_name)
                with open(file_name) as file:
                    try:
                        data = json.load(file)
                    except json.decoder.JSONDecodeError:
                        print("Invalid json: {0}".format(file_name))
                        exit(10)
                if i == 0:
                    print("Read Openradar schema: {0}".format(file_name))
                    schema = data
                else:
                    print("Read msg: {0}".format(file_name))
                    for msg in data:
                        jsonschema.validate(instance=msg, schema=schema)
                        start_datetime_str = ""
                        if "datetime" in msg["properties"]:
                            start_datetime_str = msg["properties"]["datetime"]
                        else:
                            if "start_datetime" in msg["properties"]:
                                start_datetime_str = msg["properties"]["start_datetime"]
                        print("Validation OK: {0} {1}\t{2}\t{3}".format(start_datetime_str, msg["properties"]["platform"], msg["properties"]["level"], msg["properties"]["content"]["standard_name"]))
            else:
                print("File not exists: {0}".format(file_name))
                exit(1)
    else:
        print("Validate json input")
        print("Usage:   python3 ./ord_validator.py [schema_file ] msg_file")
        print("Example: python3 ./ord_validator.py ./examples/odim/T_PAZA43_C_LPMG_20241008051005.h5_ordmsg.json")

    exit(0)
