"""
A test utility for MTEC Modbus API.

(c) 2023 by Christian Rödel
"""

from __future__ import annotations

import logging

from mtecmqtt import modbus_client
from mtecmqtt.config import REGISTER_GROUPS, REGISTER_MAP
from mtecmqtt.const import Register, RegisterGroup

_LOGGER = logging.getLogger(__name__)


def read_register(api: modbus_client.MTECModbusClient) -> None:
    """Read register."""
    _LOGGER.info("-------------------------------------")
    register = input("Register: ")
    if (data := api.read_modbus_data(registers=[register])) and (item := data.get(register)):
        _LOGGER.info(
            "Register %s (%s): %s %s",
            register,
            item[Register.NAME],
            item[Register.VALUE],
            item[Register.UNIT],
        )


def read_register_group(api: modbus_client.MTECModbusClient) -> None:
    """Read register group."""
    _LOGGER.info("-------------------------------------")
    line = "Groups: "
    for g in sorted(REGISTER_GROUPS):
        line += g + ", "
    _LOGGER.info("%s %s", line, "all")

    if (group := input("Register group (or RETURN for all): ")) in ("", "all"):
        registers = None
    elif not api.get_register_list(group=RegisterGroup(group)):
        return

    _LOGGER.info("Reading...")
    if data := api.read_modbus_data(registers=registers):
        for register, item in data.items():
            _LOGGER.info(
                "- %s;: %s; %s; %s;",
                register,
                item[Register.NAME],
                item[Register.VALUE],
                item[Register.UNIT],
            )


def write_register(api: modbus_client.MTECModbusClient) -> None:
    """Write register."""
    _LOGGER.info("-------------------------------------")
    _LOGGER.info("Current settings of writable registers:")
    _LOGGER.info("Reg   Name                           Value  Unit")
    _LOGGER.info("----- ------------------------------ ------ ----")
    register_map_sorted = dict(sorted(REGISTER_MAP.items()))
    for register, item in register_map_sorted.items():
        if item[Register.WRITABLE]:
            data = api.read_modbus_data(registers=[register])
            value = ""
            if data:
                value = data[register][Register.VALUE]
            unit = item[Register.UNIT] if item[Register.UNIT] else ""
            _LOGGER.info("%s; %s; %s; %s", register, item[Register.NAME], str(value), unit)

    _LOGGER.info("")
    register = input("Register: ")
    value = input("Value: ")

    _LOGGER.info("WARNING: Be careful when writing registers to your Inverter!")
    yn = input(f"Do you really want to set register {register} to '{value}'? (y/N)")
    if yn in ("y", "Y"):
        if api.write_register(register=register, value=value):
            _LOGGER.info("New value successfully set")
        else:
            _LOGGER.info("Writing failed")
    else:
        _LOGGER.info("Write aborted by user")


def list_register_config(api: modbus_client.MTECModbusClient) -> None:
    """List register config."""
    _LOGGER.info("-------------------------------------")
    _LOGGER.info(
        "Reg   MQTT Parameter                 Unit Mode Group           Name                   "
    )
    _LOGGER.info(
        "----- ------------------------------ ---- ---- --------------- -----------------------"
    )
    register_map_sorted = dict(sorted(REGISTER_MAP.items()))
    for register, item in register_map_sorted.items():
        if (
            not register.isnumeric()
        ):  # non-numeric registers are deemed to be calculated pseudo-registers
            register = ""
        mqtt = item[Register.MQTT] if item[Register.MQTT] else ""
        unit = item[Register.UNIT] if item[Register.UNIT] else ""
        group = item[Register.GROUP] if item[Register.GROUP] else ""
        mode = "RW" if item[Register.WRITABLE] else "R"
        _LOGGER.info(
            "%s; %s; %s; %s; %s; %s", register, mqtt, unit, mode, group, item[Register.NAME]
        )


def list_register_config_by_groups(api: modbus_client.MTECModbusClient) -> None:
    """List register config by groups."""
    for group in REGISTER_GROUPS:
        _LOGGER.info("-------------------------------------")
        _LOGGER.info("Group %s:", group)
        _LOGGER.info("")
        _LOGGER.info("Reg   MQTT Parameter                 Unit Mode Name                   ")
        _LOGGER.info("----- ------------------------------ ---- ---- -----------------------")
        register_map_sorted = dict(sorted(REGISTER_MAP.items()))
        for register, item in register_map_sorted.items():
            if item[Register.GROUP] == group:
                if (
                    not register.isnumeric()
                ):  # non-nu1meric registers are deemed to be calculated pseudo-registers
                    register = ""
                mqtt = item[Register.MQTT] if item[Register.MQTT] else ""
                unit = item[Register.UNIT] if item[Register.UNIT] else ""
                mode = "RW" if item[Register.WRITABLE] else "R"
                _LOGGER.info(
                    "%s; %s; %s; %s; %s; %s",
                    group,
                    register,
                    mqtt,
                    unit,
                    mode,
                    item[Register.NAME],
                )
        _LOGGER.info("")
