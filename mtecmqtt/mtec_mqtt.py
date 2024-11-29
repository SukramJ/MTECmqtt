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
from mtecmqtt.const import SECONDARY_REGISTER_GROUPS, Config, Register, RegisterGroup
from mtecmqtt.mqtt import mqtt_publish, mqtt_start, mqtt_stop

_LOGGER = logging.getLogger(__name__)

PVDATA_TYPE = dict[str, dict[str, Any] | int | float | str | bool]
run_status = False


def signal_handler(signal_number: int, frame: Any) -> None:
    """Signal shutdown."""
    global run_status  # noqa: PLW0603  # pylint: disable=global-statement
    _LOGGER.warning("Received Signal %s. Graceful shutdown initiated.", signal_number)
    run_status = False


def read_mtec_data(api: modbus_client.MTECModbusClient, group: RegisterGroup) -> PVDATA_TYPE:
    """Read data from MTEC modbus."""
    _LOGGER.info("Reading registers for group: %s", group)
    registers = api.get_register_list(group=group)
    now = datetime.now()
    data = api.read_modbus_data(registers=registers)
    pvdata: PVDATA_TYPE = {}
    try:  # assign all data
        for register in registers:
            item = REGISTER_MAP[register]
            if item[Register.MQTT]:
                if register.isnumeric():
                    pvdata[item[Register.MQTT]] = data[register]
                else:  # non-numeric registers are deemed to be calculated pseudo-registers
                    if register == "consumption":
                        pvdata[item[Register.MQTT]] = (
                            data["11016"][Register.VALUE] - data["11000"][Register.VALUE]
                        )  # power consumption
                    elif register == "consumption-day":
                        pvdata[item[Register.MQTT]] = (
                            data["31005"][Register.VALUE]
                            + data["31001"][Register.VALUE]
                            + data["31004"][Register.VALUE]
                            - data["31000"][Register.VALUE]
                            - data["31003"][Register.VALUE]
                        )  # power consumption
                    elif register == "autarky-day":
                        pvdata[item[Register.MQTT]] = (
                            100 * (1 - (data["31001"][Register.VALUE] / pvdata["consumption_day"]))
                            if isinstance(pvdata["consumption_day"], float | int)
                            and float(pvdata["consumption_day"]) > 0
                            else 0
                        )
                    elif register == "ownconsumption-day":
                        pvdata[item[Register.MQTT]] = (
                            100
                            * (1 - data["31000"][Register.VALUE] / data["31005"][Register.VALUE])
                            if data["31005"][Register.VALUE] > 0
                            else 0
                        )
                    elif register == "consumption-total":
                        pvdata[item[Register.MQTT]] = (
                            data["31112"][Register.VALUE]
                            + data["31104"][Register.VALUE]
                            + data["31110"][Register.VALUE]
                            - data["31102"][Register.VALUE]
                            - data["31108"][Register.VALUE]
                        )  # power consumption
                    elif register == "autarky-total":
                        pvdata[item[Register.MQTT]] = (
                            100
                            * (1 - (data["31104"][Register.VALUE] / pvdata["consumption_total"]))
                            if isinstance(pvdata["consumption_total"], float | int)
                            and float(pvdata["consumption_total"]) > 0
                            else 0
                        )
                    elif register == "ownconsumption-total":
                        pvdata[item[Register.MQTT]] = (
                            100
                            * (1 - data["31102"][Register.VALUE] / data["31112"][Register.VALUE])
                            if data["31112"][Register.VALUE] > 0
                            else 0
                        )
                    elif register == "api-date":
                        pvdata[item[Register.MQTT]] = now.strftime(
                            "%Y-%m-%d %H:%M:%S"
                        )  # Local time of this server
                    else:
                        _LOGGER.warning("Unknown calculated pseudo-register: %s", register)

                    if (
                        (value := pvdata[item[Register.MQTT]])
                        and isinstance(value, float)
                        and float(value) < 0
                    ):  # Avoid to report negative values, which might occur in some edge cases
                        pvdata[item[Register.MQTT]] = 0

    except Exception as e:
        _LOGGER.warning("Retrieved Modbus data is incomplete: %s", str(e))
        return {}
    return pvdata


def write_to_mqtt(pvdata: PVDATA_TYPE, topic_base: str, group: RegisterGroup) -> None:
    """Write data to MQTT."""
    for param, data in pvdata.items():
        topic = f"{topic_base}/{group}/{param}"
        if isinstance(data, dict):
            value = data[Register.VALUE]
            if isinstance(value, float):
                payload = CONFIG[Config.MQTT_FLOAT_FORMAT].format(value)
            elif isinstance(value, bool):
                payload = f"{value:d}"
            else:
                payload = value
        elif isinstance(data, float):
            payload = CONFIG[Config.MQTT_FLOAT_FORMAT].format(data)
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
    if CONFIG[Config.DEBUG] is True:
        logging.getLogger().setLevel(level=logging.DEBUG)
    _LOGGER.info("Starting")

    next_read_config = datetime.now()
    next_read_day = datetime.now()
    next_read_total = datetime.now()
    now_ext_idx = 0
    topic_base = None

    hass = hass_int.HassIntegration() if CONFIG[Config.HASS_ENABLE] else None

    mqttclient = mqtt_start(hass=hass)
    api = modbus_client.MTECModbusClient()
    api.connect(
        ip_addr=CONFIG[Config.MODBUS_IP],
        port=CONFIG[Config.MODBUS_PORT],
        slave=CONFIG[Config.MODBUS_SLAVE],
    )

    # Initialize
    pv_config = None
    while not pv_config:
        if not (pv_config := read_mtec_data(api=api, group=RegisterGroup.CONFIG)):
            _LOGGER.warning("Can't retrieve initial config - retry in 10 s")
            time.sleep(10)

    topic_base = f"{CONFIG[Config.MQTT_TOPIC]}/{pv_config[Register.SERIAL_NO][Register.VALUE]}"  # type: ignore[unreachable]
    if hass and not hass.is_initialized:
        hass.initialize(serial_no=pv_config[Register.SERIAL_NO][Register.VALUE])

    # Main loop - exit on signal only
    while run_status:
        now = datetime.now()

        # Now base
        if pvdata := read_mtec_data(api=api, group=RegisterGroup.BASE):
            write_to_mqtt(pvdata=pvdata, topic_base=topic_base, group=RegisterGroup.BASE)

        # Now extended - read groups in a round robin - one per loop
        if (group := SECONDARY_REGISTER_GROUPS.get(now_ext_idx)) and (
            pvdata := read_mtec_data(api=api, group=group)
        ):
            write_to_mqtt(pvdata=pvdata, topic_base=topic_base, group=group)

        if now_ext_idx >= 4:
            now_ext_idx = 0
        else:
            now_ext_idx += 1

        # Day
        if next_read_day <= now and (pvdata := read_mtec_data(api=api, group=RegisterGroup.DAY)):
            write_to_mqtt(pvdata=pvdata, topic_base=topic_base, group=RegisterGroup.DAY)
            next_read_day = datetime.now() + timedelta(seconds=CONFIG[Config.REFRESH_DAY])

        # Total
        if next_read_total <= now and (
            pvdata := read_mtec_data(api=api, group=RegisterGroup.TOTAL)
        ):
            write_to_mqtt(pvdata=pvdata, topic_base=topic_base, group=RegisterGroup.TOTAL)
            next_read_total = datetime.now() + timedelta(seconds=CONFIG[Config.REFRESH_TOTAL])

        # Config
        if next_read_config <= now and (
            pvdata := read_mtec_data(api=api, group=RegisterGroup.CONFIG)
        ):
            write_to_mqtt(pvdata=pvdata, topic_base=topic_base, group=RegisterGroup.CONFIG)
            next_read_config = datetime.now() + timedelta(seconds=CONFIG[Config.REFRESH_CONFIG])

        _LOGGER.debug("Sleep %ss", CONFIG[Config.REFRESH_NOW])
        time.sleep(CONFIG[Config.REFRESH_NOW])

    # clean up
    #if hass:
    #    hass.send_unregister_info()
    api.disconnect()
    mqtt_stop(client=mqttclient)
    _LOGGER.info("Exiting")


if __name__ == "__main__":
    main()
