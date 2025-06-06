import json
import jsonschema
import os
import sys

if __name__ == "__main__":

    schema_list = []
    odr_validator_dir = os.getenv("ORD_VALIDATOR_DIR", "")
    schema_dir = os.getenv("ORD_SCHEMA_DIR", odr_validator_dir + "/schemas")
    # print("Add validator schemas")
    schema_list.append(schema_dir + "/openradardata-spec.json")
    schema_list.append(schema_dir + "/openradardata-radar_meta-spec.json")

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
                    if i == 1:
                        print("Read Meta schema: {0}".format(file_name))
                        meta_schema = data
                    else:
                        print("Read msg: {0}".format(file_name))
                        for msg in data:
                            jsonschema.validate(instance=msg, schema=schema)
                            print("Validation OK: {0} {1}\t{2}\t{3}".format(msg["properties"]["datetime"], msg["properties"]["platform"], msg["properties"]["level"], msg["properties"]["content"]["standard_name"]))
                            # print("Meta: {0}".format(msg["properties"]["radar_meta"]))
                            msg_str = (msg["properties"]["radar_meta"]).replace("'", '"')
                            meta = json.loads(msg_str)
                            jsonschema.validate(instance=meta, schema=meta_schema)
                            print("Meta Validation OK")
            else:
                print("File not exists: {0}".format(file_name))
                exit(1)
    else:
        print("Validate json input")
        print("Usage:   python3 ./ord_validator.py [schema_file radar_meta_schema_file] msg_file")
        print("Example: python3 ./ord_validator.py ./schemas/openradardata-spec.json ./schemas/openradardata-radar_meta-spec.json ./examples/odim/T_PAZA43_C_LPMG_20241008051005.h5_ordmsg.json")

    exit(0)
