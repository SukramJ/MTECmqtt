"""
A tool that enables to query MTEC Modbus API and export the data in various ways.

(c) 2024 by Christian Rödel
"""

from __future__ import annotations

import argparse
import logging
import sys

from mtecmqtt.config import CONFIG, REGISTER_GROUPS
from mtecmqtt.const import UTF8, Config, Register
from mtecmqtt.modbus_client import MTECModbusClient

_LOGGER = logging.getLogger(__name__)


def parse_options() -> argparse.Namespace:
    """Parse the options."""
    groups = sorted(REGISTER_GROUPS)
    groups.append("all")

    parser = argparse.ArgumentParser(
        description="MTEC Modbus data export tool. Allows to read and export Modbus registers from an MTEC inverter.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "-g",
        "--group",
        choices=groups,
        default="all",
        help="Group of registers you want to export",
    )
    parser.add_argument(
        "-r", "--registers", help="Comma separated list of registers which shall be retrieved"
    )
    parser.add_argument("-c", "--csv", action="store_true", help="Export as CSV")
    parser.add_argument("-f", "--file", help="Write data to <FILE> instead of stdout")
    parser.add_argument(
        "-a",
        "--append",
        action="store_true",
        help="Use as modifier in combination with --file argument to append data to file instead of replacing it",
    )
    return parser.parse_args()


def main() -> None:
    """Start the mqtt export."""
    args = parse_options()
    api = MTECModbusClient()
    _LOGGER.info("Reading data...")

    # redirect stdout to file (if defined as command line parameter)
    if args.file:
        try:
            _LOGGER.info("Writing output to '%s'", {args.file})
            if args.csv:
                _LOGGER.info("CSV format selected")
            if args.append:
                _LOGGER.info("Append mode selected")
                f_mode = "a"
            else:
                f_mode = "w"
            original_stdout = sys.stdout
            with open(file=args.file, mode=f_mode, encoding=UTF8) as file:
                sys.stdout = file
        except Exception:
            _LOGGER.info("ERROR - Unable to open output file '%s'", args.file)
            sys.exit(1)

    registers = None
    if args.group and args.group != "all":
        registers = sorted(api.get_register_list(args.group))

    if args.registers:
        registers = []
        reg_str = args.registers.split(",")
        for addr in reg_str:
            registers.append(addr.strip())

            # Do the export
    api.connect(
        ip_addr=CONFIG[Config.MODBUS_IP],
        port=CONFIG[Config.MODBUS_PORT],
        slave=CONFIG[Config.MODBUS_SLAVE],
    )
    data = api.read_modbus_data(registers=registers)
    api.disconnect()

    if data:
        for register, item in data.items():
            if args.csv:
                line = f"{register};{item[Register.NAME]};{item[Register.VALUE]};{item[Register.UNIT]}"
            else:
                line = f"- {register}: {item[Register.NAME]} {item[Register.VALUE]} {item[Register.UNIT]}"
            _LOGGER.info(line)

    # cleanup
    if args.file:
        sys.stdout.close()
        sys.stdout = original_stdout
    _LOGGER.info("Data completed")


if __name__ == "__main__":
    main()
