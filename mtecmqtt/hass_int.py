"""
Auto discovery for home assistant.

(c) 2024 by Christian Rödel
"""

from __future__ import annotations

import json
import logging
from typing import Any, Final

from mtecmqtt import mqtt_client
from mtecmqtt.const import HA, Register

_LOGGER: Final = logging.getLogger(__name__)


class HassIntegration:
    """HA integration."""

    # Custom automations
    buttons: list[str] = [
        # name                        unique_id                   payload_press
        #    [ "Set general mode",         "MTEC_load_battery_btn",    "load_battery_from_grid" ],
    ]

    def __init__(self, hass_base_topic: str, register_map: dict[str, dict[str, Any]]) -> None:
        """Init hass integration."""
        self._hass_base_topic: Final = hass_base_topic
        self._register_map: Final = register_map
        self._mqtt: mqtt_client.MqttClient = None  # type: ignore[assignment]
        self._serial_no: str | None = None
        self._is_initialized = False
        self._devices_array: Final[list[tuple[str, Any]]] = []
        self._device_info: dict[str, Any] = {}

    @property
    def is_initialized(self) -> bool:
        """Return True if hass integration initialized."""
        return self._is_initialized

    def initialize(self, mqtt: mqtt_client.MqttClient, serial_no: str) -> None:
        """Initialize."""
        self._mqtt = mqtt
        self._serial_no = serial_no
        self._device_info = {
            HA.IDENTIFIERS: [self._serial_no],
            HA.NAME: "MTEC Energybutler",
            HA.MANUFACTURER: "MTEC",
            HA.MODEL: "Energybutler",
            HA.VIA_DEVICE: "MTECmqtt",
        }
        self._devices_array.clear()
        self._build_devices_array()
        self._build_automation_array()
        self.send_discovery_info()
        self._is_initialized = True

    def send_discovery_info(self) -> None:
        """Send discovery info."""
        _LOGGER.info("Sending home assistant discovery info")
        for device in self._devices_array:
            self._mqtt.publish(topic=device[0], payload=device[1], retain=True)

    def send_unregister_info(self) -> None:
        """Send unregister info."""
        _LOGGER.info("Sending info to unregister from home assistant")
        for device in self._devices_array:
            self._mqtt.publish(topic=device[0], payload="")

    def _build_automation_array(self) -> None:
        # Buttons
        for item in self.buttons:
            data_item = {
                HA.NAME: item[0],
                HA.UNIQUE_ID: item[1],
                HA.PAYLOAD_PRESS: item[2],
                HA.COMMAND_TOPIC: f"MTEC/{self._serial_no}/automations/command",
                HA.DEVICE: self._device_info,
            }
            topic = f"{self._hass_base_topic}/button/{item[1]}/config"
            self._devices_array.append((topic, json.dumps(data_item)))

    def _build_devices_array(self) -> None:
        """Build discovery data for devices."""
        for item in self._register_map.values():
            # Do registration if there is a "hass_" config entry
            do_hass_registration = False
            for key in item:
                if "hass_" in key:
                    do_hass_registration = True
                    break

            if item[Register.GROUP] and do_hass_registration:
                component_type = item.get("hass_component_type", "sensor")
                if component_type == "sensor":
                    self._append_sensor(item)
                if component_type == "binary_sensor":
                    self._append_binary_sensor(item)

    def _append_sensor(self, item: dict[str, Any]) -> None:
        data_item = {
            HA.NAME: item[Register.NAME],
            HA.UNIQUE_ID: "MTEC_" + item[Register.MQTT],
            HA.UNIT_OF_MEASUREMENT: item[Register.UNIT],
            HA.STATE_TOPIC: f"MTEC/{self._serial_no}/{item[Register.GROUP]}/{item[Register.MQTT]}",
            HA.DEVICE: self._device_info,
        }
        if hass_device_class := item.get(Register.DEVICE_CLASS):
            data_item[HA.DEVICE_CLASS] = hass_device_class
        if hass_value_template := item.get(Register.VALUE_TEMPLATE):
            data_item[HA.VALUE_TEMPLATE] = hass_value_template
        if hass_state_class := item.get(Register.STATE_CLASS):
            data_item[HA.STATE_CLASS] = hass_state_class

        topic = f"{self._hass_base_topic}/sensor/MTEC_{item[Register.MQTT]}/config"
        self._devices_array.append((topic, json.dumps(data_item)))

    def _append_binary_sensor(self, item: dict[str, Any]) -> None:
        data_item = {
            HA.NAME: item[Register.NAME],
            HA.UNIQUE_ID: f"MTEC_{item[Register.MQTT]}",
            HA.STATE_TOPIC: f"MTEC/{ self._serial_no}/{item[Register.GROUP]}/{item[Register.MQTT]}",
            HA.DEVICE: self._device_info,
        }

        if hass_device_class := item.get(Register.DEVICE_CLASS):
            data_item[HA.DEVICE_CLASS] = hass_device_class
        if hass_payload_on := item.get(Register.PAYLOAD_ON):
            data_item[HA.PAYLOAD_ON] = hass_payload_on
        if hass_payload_off := item.get(Register.PAYLOAD_OFF):
            data_item[HA.PAYLOAD_OFF] = hass_payload_off

        topic = f"{self._hass_base_topic}/binary_sensor/MTEC_{item[Register.MQTT]}/config"
        self._devices_array.append((topic, json.dumps(data_item)))
