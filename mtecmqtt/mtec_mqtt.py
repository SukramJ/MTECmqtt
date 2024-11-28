"""
MQTT server for M-TEC Energybutler reading modbus data.

(c) 2024 by Christian Rödel
"""

from __future__ import annotations

from datetime import datetime, timedelta
import logging
import signal
import time
from typing import Any

from mtecmqtt import hass_int, modbus_client
from mtecmqtt.config import CONFIG, REGISTER_MAP
from mtecmqtt.const import (
    CFG_DEBUG,
    CFG_HASS_ENABLE,
    CFG_MODBUS_IP,
    CFG_MODBUS_PORT,
    CFG_MODBUS_SLAVE,
    CFG_MQTT_FLOAT_FORMAT,
    CFG_REFRESH_CONFIG,
    CFG_REFRESH_DAY,
    CFG_REFRESH_NOW,
    CFG_REFRESH_TOTAL,
    MQTT,
    VALUE,
)
from mtecmqtt.mqtt import mqtt_publish, mqtt_start, mqtt_stop

_LOGGER = logging.getLogger(__name__)

PVDATA_TYPE = dict[str, dict[str, Any] | int | float | str | bool]
run_status = False


def signal_handler(signal_number: int, frame: Any) -> None:
    """Signal shutdown."""
    global run_status  # noqa: PLW0603  # pylint: disable=global-statement
    _LOGGER.warning("Received Signal %s. Graceful shutdown initiated.", signal_number)
    run_status = False


def read_mtec_data(api: modbus_client.MTECModbusClient, group: str) -> PVDATA_TYPE:
    """Read data from MTEC modbus."""
    _LOGGER.info("Reading registers for group: %s", group)
    registers = api.get_register_list(group=group)
    now = datetime.now()
    data = api.read_modbus_data(registers=registers)
    pvdata: PVDATA_TYPE = {}
    try:  # assign all data
        for register in registers:
            item = REGISTER_MAP[register]
            if item[MQTT]:
                if register.isnumeric():
                    pvdata[item[MQTT]] = data[register]
                else:  # non-numeric registers are deemed to be calculated pseudo-registers
                    if register == "consumption":
                        pvdata[item[MQTT]] = (
                            data["11016"][VALUE] - data["11000"][VALUE]
                        )  # power consumption
                    elif register == "consumption-day":
                        pvdata[item[MQTT]] = (
                            data["31005"][VALUE]
                            + data["31001"][VALUE]
                            + data["31004"][VALUE]
                            - data["31000"][VALUE]
                            - data["31003"][VALUE]
                        )  # power consumption
                    elif register == "autarky-day":
                        pvdata[item[MQTT]] = (
                            100 * (1 - (data["31001"][VALUE] / pvdata["consumption_day"]))
                            if isinstance(pvdata["consumption_day"], float | int)
                            and float(pvdata["consumption_day"]) > 0
                            else 0
                        )
                    elif register == "ownconsumption-day":
                        pvdata[item[MQTT]] = (
                            100 * (1 - data["31000"][VALUE] / data["31005"][VALUE])
                            if data["31005"][VALUE] > 0
                            else 0
                        )
                    elif register == "consumption-total":
                        pvdata[item[MQTT]] = (
                            data["31112"][VALUE]
                            + data["31104"][VALUE]
                            + data["31110"][VALUE]
                            - data["31102"][VALUE]
                            - data["31108"][VALUE]
                        )  # power consumption
                    elif register == "autarky-total":
                        pvdata[item[MQTT]] = (
                            100 * (1 - (data["31104"][VALUE] / pvdata["consumption_total"]))
                            if isinstance(pvdata["consumption_total"], float | int)
                            and float(pvdata["consumption_total"]) > 0
                            else 0
                        )
                    elif register == "ownconsumption-total":
                        pvdata[item[MQTT]] = (
                            100 * (1 - data["31102"][VALUE] / data["31112"][VALUE])
                            if data["31112"][VALUE] > 0
                            else 0
                        )
                    elif register == "api-date":
                        pvdata[item[MQTT]] = now.strftime(
                            "%Y-%m-%d %H:%M:%S"
                        )  # Local time of this server
                    else:
                        _LOGGER.warning("Unknown calculated pseudo-register: %s", register)

                    if (
                        (value := pvdata[item[MQTT]])
                        and isinstance(value, float)
                        and float(value) < 0
                    ):  # Avoid to report negative values, which might occur in some edge cases
                        pvdata[item[MQTT]] = 0

    except Exception as e:
        _LOGGER.warning("Retrieved Modbus data is incomplete: %s", str(e))
        return {}
    return pvdata


def write_to_mqtt(pvdata: PVDATA_TYPE, base_topic: str) -> None:
    """Write data to MQTT."""
    for param, data in pvdata.items():
        topic = base_topic + param
        if isinstance(data, dict):
            value = data[VALUE]
            if isinstance(value, float):
                payload = CONFIG[CFG_MQTT_FLOAT_FORMAT].format(value)
            elif isinstance(value, bool):
                payload = f"{value:d}"
            else:
                payload = value
        elif isinstance(data, float):
            payload = CONFIG[CFG_MQTT_FLOAT_FORMAT].format(data)
        elif isinstance(data, bool):
            payload = f"{data:d}"
        else:
            payload = data
        mqtt_publish(topic=topic, payload=payload)


# ==========================================
def main() -> None:
    """Stat mtec mqtt."""
    global run_status  # noqa: PLW0603  # pylint: disable=global-statement
    run_status = True

    # Initialization
    signal.signal(signalnum=signal.SIGTERM, handler=signal_handler)
    signal.signal(signalnum=signal.SIGINT, handler=signal_handler)
    if CONFIG[CFG_DEBUG] is True:
        logging.getLogger().setLevel(level=logging.DEBUG)
    _LOGGER.info("Starting")

    next_read_config = datetime.now()
    next_read_day = datetime.now()
    next_read_total = datetime.now()
    now_ext_idx = 0
    topic_base = None

    hass = hass_int.HassIntegration() if CONFIG[CFG_HASS_ENABLE] else None

    mqttclient = mqtt_start(hass=hass)
    api = modbus_client.MTECModbusClient()
    api.connect(
        ip_addr=CONFIG[CFG_MODBUS_IP], port=CONFIG[CFG_MODBUS_PORT], slave=CONFIG[CFG_MODBUS_SLAVE]
    )

    # Initialize
    pv_config = None
    while not pv_config:
        if not (pv_config := read_mtec_data(api=api, group="config")):
            _LOGGER.warning("Can't retrieve initial config - retry in 10 s")
            time.sleep(10)

    topic_base = f"{CONFIG['MQTT_TOPIC']}/{pv_config['serial_no'][VALUE]}/"  # type: ignore[unreachable]
    if hass and not hass.is_initialized:
        hass.initialize(serial_no=pv_config["serial_no"][VALUE])

    # Main loop - exit on signal only
    while run_status:
        now = datetime.now()

        # Now base
        if pvdata := read_mtec_data(api=api, group="now-base"):
            write_to_mqtt(pvdata=pvdata, base_topic=f"{topic_base}now-base/")

        # Now extended - read groups in a round robin - one per loop
        if now_ext_idx == 0 and (pvdata := read_mtec_data(api=api, group="now-grid")):
            write_to_mqtt(pvdata=pvdata, base_topic=f"{topic_base}now-grid/")
        elif now_ext_idx == 1 and (pvdata := read_mtec_data(api=api, group="now-inverter")):
            write_to_mqtt(pvdata=pvdata, base_topic=f"{topic_base}now-inverter/")
        elif now_ext_idx == 2 and (pvdata := read_mtec_data(api=api, group="now-backup")):
            write_to_mqtt(pvdata=pvdata, base_topic=f"{topic_base}now-backup/")
        elif now_ext_idx == 3 and (pvdata := read_mtec_data(api=api, group="now-battery")):
            write_to_mqtt(pvdata=pvdata, base_topic=f"{topic_base}now-battery/")
        elif now_ext_idx == 4 and (pvdata := read_mtec_data(api=api, group="now-pv")):
            write_to_mqtt(pvdata=pvdata, base_topic=f"{topic_base}now-pv/")

        if now_ext_idx >= 4:
            now_ext_idx = 0
        else:
            now_ext_idx += 1

        # Day
        if next_read_day <= now and (pvdata := read_mtec_data(api=api, group="day")):
            write_to_mqtt(pvdata=pvdata, base_topic=f"{topic_base}day/")
            next_read_day = datetime.now() + timedelta(seconds=CONFIG[CFG_REFRESH_DAY])

        # Total
        if next_read_total <= now and (pvdata := read_mtec_data(api=api, group="total")):
            write_to_mqtt(pvdata=pvdata, base_topic=f"{topic_base}total/")
            next_read_total = datetime.now() + timedelta(seconds=CONFIG[CFG_REFRESH_TOTAL])

        # Config
        if next_read_config <= now and (pvdata := read_mtec_data(api=api, group="config")):
            write_to_mqtt(pvdata=pvdata, base_topic=f"{topic_base}config/")
            next_read_config = datetime.now() + timedelta(seconds=CONFIG[CFG_REFRESH_CONFIG])

        _LOGGER.debug("Sleep %ss", CONFIG[CFG_REFRESH_NOW])
        time.sleep(CONFIG[CFG_REFRESH_NOW])

    # clean up
    if hass:
        hass.send_unregister_info()
    api.disconnect()
    mqtt_stop(client=mqttclient)
    _LOGGER.info("Exiting")


if __name__ == "__main__":
    main()
