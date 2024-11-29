"""
Read YAML config files.

(c) 2024 by Christian Rödel
"""

from __future__ import annotations

import logging
import os
import socket
import sys
from typing import Any, Final, cast

import yaml

from mtecmqtt.const import UTF8, Register

_LOGGER = logging.getLogger(__name__)

CONFIG_FILE: Final = "config.yaml"
CONFIG_PATH: Final = "mtecmqtt"
CONFIG_ROOT: Final = ".config"
CONFIG_TEMPLATE: Final = "config-template.yaml"


# Create new config file
def create_config_file() -> bool:
    """Read the config file."""
    _LOGGER.info("Creating %s", CONFIG_FILE)

    # Resolve hostname
    try:
        ip_addr = socket.gethostbyname("espressif")
        _LOGGER.info("Found espressif server: %s", ip_addr)
    except OSError:
        _LOGGER.info("Couldn't find espressif server")
        ip_addr = input("Please enter IP address of espressif server: ")

    opt = input("Enable HomeAssistant support? (y/N): ")
    hass_cfg = "HASS_ENABLE : True" if opt.lower() == "y" else "HASS_ENABLE : False"

    # Read template
    try:
        BASE_DIR = os.path.dirname(__file__)  # Base installation directory
        templ_fname = os.path.join(BASE_DIR, CONFIG_TEMPLATE)
        with open(file=templ_fname, encoding=UTF8) as file:
            data = file.read()
    except Exception as ex:
        _LOGGER.info("ERROR - Couldn't read '%s': %s", CONFIG_TEMPLATE, ex)
        return False

    # Customize
    data = data.replace("HASS_ENABLE : False", hass_cfg)
    data = data.replace("MODBUS_IP : espressif", 'MODBUS_IP : "' + ip_addr + '"')

    # Write customized config
    # Usually something like ~/.config/mtecmqtt/config.yaml resp. 'C:\\Users\\xxxx\\AppData\\Roaming'
    if cfg_path := os.environ.get("XDG_CONFIG_HOME") or os.environ.get("APPDATA"):
        cfg_fname = os.path.join(cfg_path, CONFIG_PATH, CONFIG_FILE)
    else:
        cfg_fname = os.path.join(
            os.path.expanduser("~"), CONFIG_ROOT, CONFIG_PATH, CONFIG_FILE
        )  # ~/.config/mtecmqtt/config.yaml

    try:
        os.makedirs(os.path.dirname(cfg_fname), exist_ok=True)
        with open(file=cfg_fname, mode="w", encoding=UTF8) as file:
            file.write(data)
    except Exception as ex:
        _LOGGER.error("ERROR - Couldn't write %s: %s", cfg_fname, ex)
        return False

    _LOGGER.info("Successfully created %s", cfg_fname)
    return True


def init_config() -> dict[str, Any]:
    """Read configuration from YAML file."""
    # Look in different locations for config.yaml file
    conf_files: list[str] = []
    conf_files.append(os.path.join(os.getcwd(), CONFIG_FILE))  # CWD/config.yaml
    # Usually something like ~/.config/mtecmqtt/config.yaml resp. 'C:\\Users\\xxxx\\AppData\\Roaming'
    if cfg_path := os.environ.get("XDG_CONFIG_HOME") or os.environ.get("APPDATA"):
        conf_files.append(os.path.join(cfg_path, CONFIG_PATH, CONFIG_FILE))
    else:
        conf_files.append(
            os.path.join(os.path.expanduser("~"), CONFIG_ROOT, CONFIG_PATH, CONFIG_FILE)
        )

    config: dict[str, Any] = {}
    for fname_conf in conf_files:
        try:
            with open(file=fname_conf, encoding=UTF8) as f_conf:
                config = cast(dict[str, Any], yaml.safe_load(f_conf))
                _LOGGER.info("Using config YAML file: %s", fname_conf)
                break
        except OSError as err:
            _LOGGER.debug("Couldn't open config YAML file: %s", str(err))
        except yaml.YAMLError as err:
            _LOGGER.debug("Couldn't read config YAML file %s : %s", fname_conf, str(err))

    return config


def init_register_map() -> tuple[dict[str, dict[str, Any]], list[str]]:
    """Read inverter registers and their mapping from YAML file."""
    BASE_DIR = os.path.dirname(__file__)  # Base installation directory
    try:
        fname_regs = os.path.join(BASE_DIR, "registers.yaml")
        with open(fname_regs, encoding=UTF8) as f_regs:
            r_map = cast(dict[str, dict[str, Any]], yaml.safe_load(f_regs))
    except OSError as err:
        _LOGGER.fatal("Couldn't open registers YAML file: %s", str(err))
        sys.exit(1)
    except yaml.YAMLError as err:
        _LOGGER.fatal("Couldn't read config YAML file %s: %s", fname_regs, str(err))
        sys.exit(1)

    # Syntax checks
    reg_map: dict[str, dict[str, Any]] = {}
    p_mandatory: list[str] = [Register.NAME]

    # param, default
    p_optional: dict[str, Any] = {
        Register.LENGTH: None,
        Register.TYPE: None,
        Register.UNIT: "",
        Register.SCALE: 1,
        Register.WRITABLE: False,
        Register.MQTT: None,
        Register.GROUP: None,
    }
    reg_groups: list[str] = []

    error = False
    for key, val in r_map.items():
        # Check for mandatory parameters
        for p in p_mandatory:
            if not val.get(p):
                _LOGGER.warning(
                    "Skipping invalid register config: %s. Missing mandatory parameter: %s.",
                    key,
                    p,
                )
                error = True
                break

        if not error:  # All mandatory parameters found
            item = val.copy()
            # Check optional parameters and add defaults, if not found
            for param, default in p_optional.items():
                if param not in item:
                    item[param] = default
            reg_map[key] = item  # Append to reg_map

            if (group := item[Register.GROUP]) and group not in reg_groups:
                reg_groups.append(group)  # Append to group list
    return reg_map, reg_groups


logging.basicConfig(level=logging.INFO, format="[%(levelname)s] %(filename)s: %(message)s")
if not (CONFIG := init_config()):
    if create_config_file():  # Create a new config
        if not (CONFIG := init_config()):
            _LOGGER.fatal("Couldn't open config YAML file")
            sys.exit(1)
    else:
        _LOGGER.fatal("Couldn't create config YAML file")
        sys.exit(1)

REGISTER_MAP, REGISTER_GROUPS = init_register_map()
