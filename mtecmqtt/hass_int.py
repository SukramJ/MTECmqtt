"""
Auto discovery for home assistant.

(c) 2024 by Christian Rödel
"""

from __future__ import annotations

import json
import logging
from typing import Any

from mtecmqtt import config, const, mqtt

_LOGGER = logging.getLogger(__name__)


class HassIntegration:
    """HA integration."""

    # Custom automations
    buttons: list[str] = [
        # name                        unique_id                   payload_press
        #    [ "Set general mode",         "MTEC_load_battery_btn",    "load_battery_from_grid" ],
    ]

    def __init__(self) -> None:
        """Init hass integration."""
        self._serial_no: str | None = None
        self.is_initialized = False
        self._devices_array: list[tuple[str, Any]] = []
        self._device_info: dict[str, Any] = {}

    def initialize(self, serial_no: str) -> None:
        """Initialize."""
        self._serial_no = serial_no
        self._device_info = {
            const.HA_IDENTIFIERS: [self._serial_no],
            const.HA_NAME: "MTEC Energybutler",
            const.HA_MANUFACTURER: "MTEC",
            const.HA_MODEL: "Energybutler",
            const.HA_VIA_DEVICE: "MTECmqtt",
        }
        self._devices_array.clear()
        self._build_devices_array()
        self._build_automation_array()
        self.send_discovery_info()
        self.is_initialized = True

    def send_discovery_info(self) -> None:
        """Send discovery info."""
        _LOGGER.info("Sending home assistant discovery info")
        for device in self._devices_array:
            mqtt.mqtt_publish(topic=device[0], payload=device[1])

    def send_unregister_info(self) -> None:
        """Send unregister info."""
        _LOGGER.info("Sending info to unregister from home assistant")
        for device in self._devices_array:
            mqtt.mqtt_publish(topic=device[0], payload="")

    def _build_automation_array(self) -> None:
        # Buttons
        for item in self.buttons:
            data_item = {
                const.HA_NAME: item[0],
                const.HA_UNIQUE_ID: item[1],
                const.HA_PAYLOAD_PRESS: item[2],
                const.HA_COMMAND_TOPIC: f"MTEC/{self._serial_no}/automations/command",
                const.HA_DEVICE: self._device_info,
            }
            topic = f"{config.CONFIG[const.CFG_HASS_BASE_TOPIC]}/button/{item[1]}/config"
            self._devices_array.append((topic, json.dumps(data_item)))

    def _build_devices_array(self) -> None:
        """Build discovery data for devices."""
        for item in config.REGISTER_MAP.values():
            # Do registration if there is a "hass_" config entry
            do_hass_registration = False
            for key in item:
                if "hass_" in key:
                    do_hass_registration = True
                    break

            if item[const.REG_GROUP] and do_hass_registration:
                component_type = item.get("hass_component_type", "sensor")
                if component_type == "sensor":
                    self._append_sensor(item)
                if component_type == "binary_sensor":
                    self._append_binary_sensor(item)

    def _append_sensor(self, item: dict[str, Any]) -> None:
        data_item = {
            const.HA_NAME: item[const.REG_NAME],
            const.HA_UNIQUE_ID: "MTEC_" + item[const.REG_MQTT],
            const.HA_UNIT_OF_MEASUREMENT: item[const.REG_UNIT],
            const.HA_STATE_TOPIC: f"MTEC/{self._serial_no}/{item[const.REG_GROUP]}/{item[const.REG_MQTT]}",
            const.HA_DEVICE: self._device_info,
        }
        if hass_device_class := item.get(const.REG_DEVICE_CLASS):
            data_item[const.HA_DEVICE_CLASS] = hass_device_class
        if hass_value_template := item.get(const.REG_VALUE_TEMPLATE):
            data_item[const.HA_VALUE_TEMPLATE] = hass_value_template
        if hass_state_class := item.get(const.REG_STATE_CLASS):
            data_item[const.HA_STATE_CLASS] = hass_state_class

        topic = (
            f"{config.CONFIG[const.CFG_HASS_BASE_TOPIC]}/sensor/MTEC_{item[const.REG_MQTT]}/config"
        )
        self._devices_array.append((topic, json.dumps(data_item)))

    def _append_binary_sensor(self, item: dict[str, Any]) -> None:
        data_item = {
            const.HA_NAME: item[const.REG_NAME],
            const.HA_UNIQUE_ID: f"MTEC_{item[const.REG_MQTT]}",
            const.HA_STATE_TOPIC: f"MTEC/{ self._serial_no}/{item[const.REG_GROUP]}/{item[const.REG_MQTT]}",
            const.HA_DEVICE: self._device_info,
        }

        if hass_device_class := item.get(const.REG_DEVICE_CLASS):
            data_item[const.HA_DEVICE_CLASS] = hass_device_class
        if hass_payload_on := item.get(const.REG_PAYLOAD_ON):
            data_item[const.HA_PAYLOAD_ON] = hass_payload_on
        if hass_payload_off := item.get(const.REG_PAYLOAD_OFF):
            data_item[const.HA_PAYLOAD_OFF] = hass_payload_off

        topic = f"{config.CONFIG[const.CFG_HASS_BASE_TOPIC]}/binary_sensor/MTEC_{item[const.REG_MQTT]}/config"
        self._devices_array.append((topic, json.dumps(data_item)))
