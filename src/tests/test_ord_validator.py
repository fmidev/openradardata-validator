import os
from pathlib import Path

import pytest

from openradardata_validator.ord_validator import validate_ord_json

current_filedir = Path(__file__).parent.resolve()


@pytest.mark.parametrize(
    ["filename"],
    [
        (file,)
        for file in os.listdir(current_filedir / "data/odim")
        if file.endswith(".h5.json") or file.endswith(".hdf.json")
    ],
)
def test_odim2mqtt(filename: str) -> None:
    validate_ord_json(current_filedir / "data/odim" / filename)
