"""
MQTT client base implementation.

(c) 2024 by Christian Rödel
"""

from __future__ import annotations

import logging
import time
from typing import Any, Final

from paho.mqtt import client as paho

from mtecmqtt import hass_int
from mtecmqtt.const import CLIENT_ID, UTF8, Config
from mtecmqtt.exceptions import MtecException

_LOGGER: Final = logging.getLogger(__name__)


class MqttClient:
    """Client for mqtt."""

    def __init__(
        self, config: dict[str, Any], hass: hass_int.HassIntegration | None = None
    ) -> None:
        """Init the mqtt client."""
        self._hass = hass
        self._username: Final[str] = config[Config.MQTT_LOGIN]
        self._password: Final[str] = config[Config.MQTT_PASSWORD]
        self._auth: Final[dict[str, str]] = {
            "username": self._username,
            "password": self._password,
        }
        self._hostname: Final[str] = config[Config.MQTT_SERVER]
        self._port: Final[int] = config[Config.MQTT_PORT]
        self._hass_base_topic: Final[str] = config[Config.HASS_BASE_TOPIC]
        self._hass_birth_gracetime: Final[int] = config.get(Config.HASS_BIRTH_GRACETIME, 15)
        self._client = self._start()

    def on_mqtt_connect(self, *args: Any) -> None:
        """Handle mqtt connect."""
        _LOGGER.info("Connected to MQTT broker")

    def on_mqtt_message(
        self,
        client: paho.Client,
        userdata: Any,
        message: paho.MQTTMessage,
    ) -> None:
        """Handle received message."""
        try:
            msg = message.payload.decode(UTF8)
            # topic = message.topic.split("/")
            if msg == "online" and self._hass is not None:
                gracetime = self._hass_birth_gracetime
                _LOGGER.info(
                    "Received HASS online message. Sending discovery info in %i sec", gracetime
                )
                time.sleep(
                    gracetime
                )  # dirty workaround: hass requires some grace period for being ready to receive discovery info
                self._hass.send_discovery_info()
        except Exception as e:
            _LOGGER.warning("Error while handling MQTT message: %s", str(e))

    def _start(self) -> paho.Client:
        """Start the MQTT client."""
        try:
            client = paho.Client(client_id=CLIENT_ID)
            client.username_pw_set(username=self._username, password=self._password)
            client.connect(host=self._hostname, port=self._port)

            if self._hass:
                client.subscribe(topic=self._hass_base_topic + "/status")
            client.on_connect = self.on_mqtt_connect
            client.on_message = self.on_mqtt_message
            client.loop_start()
            _LOGGER.info("MQTT server started")
        except Exception as ex:
            msg = "Couldn't start MQTT: {str(e)}"
            _LOGGER.warning(msg)
            raise MtecException(msg) from ex
        else:
            return client

    def stop(self) -> None:
        """Stop the MQTT client."""
        try:
            self._client.loop_stop()
            _LOGGER.info("MQTT server stopped")
        except Exception as e:
            _LOGGER.warning("Couldn't stop MQTT: %s", str(e))

    def publish(self, topic: str, payload: str, retain: bool = False) -> None:
        """Publish mqtt message."""
        _LOGGER.debug("- %s: %s", topic, str(payload))
        try:
            self._client.publish(topic=topic, payload=payload, retain=retain)
        except Exception as e:
            _LOGGER.error("Couldn't send MQTT command: %s", str(e))
