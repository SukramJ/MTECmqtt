"""
This is a test utility for MTEC Modbus API.
(c) 2023 by Christian Rödel
"""

from __future__ import annotations

import logging

from mtecmqtt.config import cfg, register_groups, register_map
from mtecmqtt.MTECModbusClient import MTECModbusClient

_LOGGER = logging.getLogger(__name__)


# -------------------------------
def read_register(api) -> None:
    """Read register."""
    _LOGGER.info("-------------------------------------")
    register = input("Register: ")
    data = api.read_modbus_data(registers=[register])
    if data:
        item = data.get(register)
        _LOGGER.info(
            "Register %s (%s): %s %s", register, item["name"], item["value"], item["unit"]
        )


# -------------------------------
def read_register_group(api) -> None:
    """Read register group."""
    _LOGGER.info("-------------------------------------")
    line = "Groups: "
    for g in sorted(register_groups):
        line += g + ", "
    _LOGGER.info(line + "all")

    group = input("Register group (or RETURN for all): ")
    if group == "" or group == "all":
        registers = None
    else:
        registers = api.get_register_list(group)
        if not registers:
            return

    _LOGGER.info("Reading...")
    data = api.read_modbus_data(registers=registers)
    if data:
        for register, item in data.items():
            _LOGGER.info(
                "- {}: {:50s} {} {}".format(register, item["name"], item["value"], item["unit"])
            )


# -------------------------------
def write_register(api) -> None:
    """Write register."""
    _LOGGER.info("-------------------------------------")
    _LOGGER.info("Current settings of writable registers:")
    _LOGGER.info("Reg   Name                           Value  Unit")
    _LOGGER.info("----- ------------------------------ ------ ----")
    register_map_sorted = dict(sorted(register_map.items()))
    for register, item in register_map_sorted.items():
        if item["writable"]:
            data = api.read_modbus_data(registers=[register])
            value = ""
            if data:
                value = data[register]["value"]
            unit = item["unit"] if item["unit"] else ""
            _LOGGER.info(
                "{:5s} {:30s} {:6s} {:4s} ".format(register, item["name"], str(value), unit)
            )

    _LOGGER.info("")
    register = input("Register: ")
    value = input("Value: ")

    _LOGGER.info("WARNING: Be careful when writing registers to your Inverter!")
    yn = input(f"Do you really want to set register {register} to '{value}'? (y/N)")
    if yn == "y" or yn == "Y":
        ret = api.write_register(register=register, value=value)
        if ret == True:
            _LOGGER.info("New value successfully set")
        else:
            _LOGGER.info("Writing failed")
    else:
        _LOGGER.info("Write aborted by user")

    # -------------------------------


def list_register_config(api) -> None:
    """List register config."""
    _LOGGER.info("-------------------------------------")
    _LOGGER.info(
        "Reg   MQTT Parameter                 Unit Mode Group           Name                   "
    )
    _LOGGER.info(
        "----- ------------------------------ ---- ---- --------------- -----------------------"
    )
    register_map_sorted = dict(sorted(register_map.items()))
    for register, item in register_map_sorted.items():
        if (
            not register.isnumeric()
        ):  # non-numeric registers are deemed to be calculated pseudo-registers
            register = ""
        mqtt = item["mqtt"] if item["mqtt"] else ""
        unit = item["unit"] if item["unit"] else ""
        group = item["group"] if item["group"] else ""
        mode = "RW" if item["writable"] else "R"
        _LOGGER.info(
            "{:5s} {:30s} {:4s} {:4s} {:15s} {}".format(
                register, mqtt, unit, mode, group, item["name"]
            )
        )


# -------------------------------
def list_register_config_by_groups(api):
    """List register config by groups."""
    for group in register_groups:
        _LOGGER.info("-------------------------------------")
        _LOGGER.info("Group %s:", group)
        _LOGGER.info("")
        _LOGGER.info("Reg   MQTT Parameter                 Unit Mode Name                   ")
        _LOGGER.info("----- ------------------------------ ---- ---- -----------------------")
        register_map_sorted = dict(sorted(register_map.items()))
        for register, item in register_map_sorted.items():
            if item["group"] == group:
                if (
                    not register.isnumeric()
                ):  # non-nu1meric registers are deemed to be calculated pseudo-registers
                    register = ""
                mqtt = item["mqtt"] if item["mqtt"] else ""
                unit = item["unit"] if item["unit"] else ""
                mode = "RW" if item["writable"] else "R"
                _LOGGER.info(
                    "{:5s} {:30s} {:4s} {:4s} {}".format(register, mqtt, unit, mode, item["name"])
                )
        _LOGGER.info("")


# -------------------------------
def main():
    """Main function."""
    api = MTECModbusClient()
    api.connect(ip_addr=cfg["MODBUS_IP"], port=cfg["MODBUS_PORT"], slave=cfg["MODBUS_SLAVE"])

    while True:
        _LOGGER.info("=====================================")
        _LOGGER.info("Menu:")
        _LOGGER.info("  1: List all known registers")
        _LOGGER.info("  2: List register configuration by groups")
        _LOGGER.info("  3: Read register group from Inverter")
        _LOGGER.info("  4: Read single register from Inverter")
        _LOGGER.info("  5: Write register to Inverter")
        _LOGGER.info("  x: Exit")
        opt = input("Please select: ")
        if opt == "1":
            list_register_config(api)
        elif opt == "2":
            list_register_config_by_groups(api)
        if opt == "3":
            read_register_group(api)
        elif opt == "4":
            read_register(api)
        elif opt == "5":
            write_register(api)
        elif opt in ("x", "X"):
            break

    api.disconnect()
    _LOGGER.info("Bye!")


# -------------------------------
if __name__ == "__main__":
    main()
