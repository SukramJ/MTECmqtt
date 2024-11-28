"""
Auto discovery for home assistant.

(c) 2024 by Christian Rödel
"""

from __future__ import annotations

import json
import logging
from typing import Any

from mtecmqtt import config, mqtt
from mtecmqtt.const import GROUP, MQTT, NAME, UNIT

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
            "identifiers": [self._serial_no],
            "name": "MTEC Energybutler",
            "manufacturer": "MTEC",
            "model": "Energybutler",
            "via_device": "MTECmqtt",
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
                "name": item[0],
                "unique_id": item[1],
                "payload_press": item[2],
                "command_topic": f"MTEC/{self._serial_no}/automations/command",
                "device": self._device_info,
            }
            topic = f"{config.CONFIG['HASS_BASE_TOPIC']}/button/{item[1]}/config"
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

            if item[GROUP] and do_hass_registration:
                component_type = item.get("hass_component_type", "sensor")
                if component_type == "sensor":
                    self._append_sensor(item)
                if component_type == "binary_sensor":
                    self._append_binary_sensor(item)

    def _append_sensor(self, item: dict[str, Any]) -> None:
        data_item = {
            "name": item[NAME],
            "unique_id": "MTEC_" + item[MQTT],
            "unit_of_measurement": item[UNIT],
            "state_topic": f"MTEC/{self._serial_no}/{item[GROUP]}/{item[MQTT]}",
            "device": self._device_info,
        }
        if item.get("hass_device_class"):
            data_item["device_class"] = item["hass_device_class"]
        if item.get("hass_value_template"):
            data_item["value_template"] = item["hass_value_template"]
        if item.get("hass_state_class"):
            data_item["state_class"] = item["hass_state_class"]

        topic = f"{config.CONFIG['HASS_BASE_TOPIC']}/sensor/MTEC_{item[MQTT]}/config"
        self._devices_array.append((topic, json.dumps(data_item)))

    def _append_binary_sensor(self, item: dict[str, Any]) -> None:
        data_item = {
            "name": item[NAME],
            "unique_id": f"MTEC_{item[MQTT]}",
            "state_topic": f"MTEC/{ self._serial_no}/{item[GROUP]}/{item[MQTT]}",
            "device": self._device_info,
        }
        if item.get("hass_device_class"):
            data_item["device_class"] = item["hass_device_class"]
        if item.get("hass_payload_on"):
            data_item["payload_on"] = item["hass_payload_on"]
        if item.get("hass_payload_off"):
            data_item["payload_off"] = item["hass_payload_off"]

        topic = f"{config.CONFIG['HASS_BASE_TOPIC']}/binary_sensor/MTEC_{item[MQTT]}/config"
        self._devices_array.append((topic, json.dumps(data_item)))
