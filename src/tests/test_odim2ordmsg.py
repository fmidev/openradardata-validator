import json
import os
import pytest
import pathlib

from openradardata_validator.odim2ordmsg import odim2mqtt

current_filedir = pathlib.Path(__file__).parent.resolve()


@pytest.mark.parametrize(
    ["filename"],
    [
        (file,)
        for file in os.listdir(current_filedir / "data/odim")
        if file.endswith(".h5") or file.endswith(".hdf")
    ],
)
def test_odim2mqtt(filename: str):
    msg = odim2mqtt(current_filedir / "data/odim" / filename)
    first_msg = True
    result = ["["]
    for m in msg:
        if first_msg:
            first_msg = False
        else:
            result.append(",")
        result.append(json.dumps(m, indent=2))
    result.append("]")
    output_text = "\n".join(result) + "\n"
    with open(current_filedir / "data/odim" / f"{filename}.json") as reference_file:
        assert reference_file.read() == output_text
