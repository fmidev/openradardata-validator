import json
import os
import pytest
from pathlib import Path

from openradardata_validator import odim2ordmsg

current_filedir = Path(__file__).parent.resolve()


@pytest.mark.parametrize(
    ["filename"],
    [
        (file,)
        for file in os.listdir(current_filedir / "data/odim")
        if file.endswith(".h5") or file.endswith(".hdf")
    ],
)
def test_odim2mqtt(filename: str):
    output_text = odim2ordmsg.main(current_filedir / "data/odim" / filename)
    with open(
        current_filedir / "data/odim" / f"{filename}.json", encoding="utf-8"
    ) as reference_file:
        assert reference_file.read() == output_text
