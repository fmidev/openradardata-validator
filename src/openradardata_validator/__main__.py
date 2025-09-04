import argparse
from enum import Enum
from pathlib import Path

from openradardata_validator.odim2ordmsg import create_json_from_odim
from openradardata_validator.ord_validator import validate_ord_json


class StartScript(Enum):
    ODIM_TO_ORD_MSG = "odim2ordmsg"
    ORD_VALIDATOR = "ord_validator"


def parse_cli_arguments() -> argparse.Namespace:
    cli_arguments_parser = argparse.ArgumentParser(
        prog="openradardata_validator", description="OpenRadarData Validator"
    )

    cli_arguments_parser.add_argument(
        dest="start_script",
        nargs=1,
        type=StartScript,
        help="Choose script to start, currently supported are: odim2ordmsg, ord_validator",
    )
    cli_arguments_parser.add_argument(
        dest="schema",
        nargs="?",
        type=Path,
        help="Choose schema for usage in script",
    )
    cli_arguments_parser.add_argument(
        dest="filename",
        nargs=1,
        type=Path,
        help="Choose filename to apply script to",
    )
    known_args, _ = cli_arguments_parser.parse_known_args()

    return known_args


if __name__ == "__main__":
    cli_arguments = parse_cli_arguments()
    match cli_arguments.start_script[0]:
        case StartScript.ODIM_TO_ORD_MSG:
            print(
                create_json_from_odim(cli_arguments.filename[0], cli_arguments.schema)
            )
        case StartScript.ORD_VALIDATOR:
            validate_ord_json(cli_arguments.filename[0], cli_arguments.schema)
