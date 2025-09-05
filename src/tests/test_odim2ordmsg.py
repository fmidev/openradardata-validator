import os
from pathlib import Path

import pytest

from openradardata_validator.odim2ordmsg import create_json_from_odim


current_filedir = Path(__file__).parent.resolve()


@pytest.mark.parametrize(
    ["filename"],
    [
        (file,)
        for file in os.listdir(current_filedir / "data/odim")
        if file.endswith(".h5") or file.endswith(".hdf")
    ],
)
def test_create_json_from_odim(filename: str) -> None:
    output_text = create_json_from_odim(
        current_filedir / "data/odim" / filename, "https://placeholder.url"
    )
    with open(
        current_filedir / "data/odim" / f"{filename}.json", encoding="utf-8"
    ) as reference_file:
        assert reference_file.read() == output_text
