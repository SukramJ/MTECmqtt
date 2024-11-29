"""
MQTT client base implementation.

(c) 2024 by Christian Rödel
"""

from __future__ import annotations

import logging
import time
from typing import Any

from paho.mqtt import client as mqtt_client, publish

from mtecmqtt import hass_int
from mtecmqtt.config import CONFIG
from mtecmqtt.const import CLIENT_ID, UTF8, Config

_LOGGER = logging.getLogger(__name__)


def on_mqtt_connect(*args: Any) -> None:
    """Handle mqtt connect."""
    _LOGGER.info("Connected to MQTT broker")


def on_mqtt_message(
    mqttclient: mqtt_client.Client,
    userdata: hass_int.HassIntegration,
    message: mqtt_client.MQTTMessage,
) -> None:
    """Handle received message."""
    try:
        msg = message.payload.decode(UTF8)
        # topic = message.topic.split("/")
        if msg == "online" and userdata:
            gracetime = CONFIG.get(Config.HASS_BIRTH_GRACETIME, 15)
            _LOGGER.info(
                "Received HASS online message. Sending discovery info in %i sec", gracetime
            )
            time.sleep(
                gracetime
            )  # dirty workaround: hass requires some grace period for being ready to receive discovery info
            userdata.send_discovery_info()
    except Exception as e:
        _LOGGER.warning("Error while handling MQTT message: %s", str(e))


def mqtt_start(hass: hass_int.HassIntegration | None = None) -> mqtt_client.Client | None:
    """Start the MQTT client."""
    try:
        client = mqtt_client.Client()
        client.user_data_set(userdata=hass)  # register home automation instance

        client.username_pw_set(CONFIG[Config.MQTT_LOGIN], CONFIG[Config.MQTT_PASSWORD])
        client.connect(CONFIG[Config.MQTT_SERVER], CONFIG[Config.MQTT_PORT], keepalive=60)

        if hass:
            client.subscribe(topic=CONFIG[Config.HASS_BASE_TOPIC] + "/status")
        client.on_connect = on_mqtt_connect
        client.on_message = on_mqtt_message
        client.loop_start()
        _LOGGER.info("MQTT server started")
    except Exception as e:
        _LOGGER.warning("Couldn't start MQTT: %s", str(e))
        return None
    else:
        return client


def mqtt_stop(client: mqtt_client.Client) -> None:
    """Stop the MQTT client."""
    try:
        client.loop_stop()
        _LOGGER.info("MQTT server stopped")
    except Exception as e:
        _LOGGER.warning("Couldn't stop MQTT: %s", str(e))


def mqtt_publish(topic: str, payload: str, retain: bool = False) -> None:
    """Publish mqtt message."""
    if CONFIG[Config.MQTT_DISABLE]:  # Don't do anything - just logg
        _LOGGER.info("- %s: %s", topic, str(payload))
    else:
        auth = {
            "username": CONFIG[Config.MQTT_LOGIN],
            "password": CONFIG[Config.MQTT_PASSWORD],
        }
        _LOGGER.debug("- %s: %s", topic, str(payload))
        try:
            publish.single(
                topic=topic,
                client_id=CLIENT_ID,
                payload=payload,
                hostname=CONFIG[Config.MQTT_SERVER],
                port=CONFIG[Config.MQTT_PORT],
                auth=auth,  # type: ignore[arg-type]
                retain=retain,
            )
        except Exception as e:
            _LOGGER.error("Couldn't send MQTT command: %s", str(e))
