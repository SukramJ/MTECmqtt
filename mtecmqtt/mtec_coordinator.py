"""
MQTT server for M-TEC Energybutler reading modbus data.

(c) 2024 by Christian Rödel
"""

from __future__ import annotations

from datetime import datetime, timedelta
import logging
import signal
import time
from typing import Any, Final

from mtecmqtt import hass_int, modbus_client, mqtt_client
from mtecmqtt.config import init_config, init_register_map
from mtecmqtt.const import SECONDARY_REGISTER_GROUPS, Config, Register, RegisterGroup

_LOGGER = logging.getLogger(__name__)

PVDATA_TYPE = dict[str, dict[str, Any] | int | float | str | bool]
run_status = False


def signal_handler(signal_number: int, _: Any) -> None:
    """Signal shutdown."""
    global run_status  # noqa: PLW0603  # pylint: disable=global-statement
    _LOGGER.warning("Received Signal %s. Graceful shutdown initiated.", signal_number)
    run_status = False


class MtecCoordinator:
    """MTEC MQTT Coordinator."""

    def __init__(self) -> None:
        """Initialize the coordinator."""
        config = init_config()
        self._register_map, register_groups = init_register_map()
        self._hass: Final = (
            hass_int.HassIntegration(
                hass_base_topic=config[Config.HASS_BASE_TOPIC], register_map=self._register_map
            )
            if config[Config.HASS_ENABLE]
            else None
        )
        self._mqtt_client: Final = mqtt_client.MqttClient(config=config, hass=self._hass)
        self._modbus_client: Final = modbus_client.MTECModbusClient(
            config=config,
            register_map=self._register_map,
            register_groups=register_groups,
        )

        self._mqtt_float_format: Final[str] = config[Config.MQTT_FLOAT_FORMAT]
        self._mqtt_refresh_config: Final[int] = config[Config.REFRESH_CONFIG]
        self._mqtt_refresh_day: Final[int] = config[Config.REFRESH_DAY]
        self._mqtt_refresh_now: Final[int] = config[Config.REFRESH_NOW]
        self._mqtt_refresh_total: Final[int] = config[Config.REFRESH_TOTAL]
        self._mqtt_topic: Final[str] = config[Config.MQTT_TOPIC]

        if config[Config.DEBUG] is True:
            logging.getLogger().setLevel(level=logging.DEBUG)
        _LOGGER.info("Starting")

    def stop(self) -> None:
        """Stop the coordinator."""
        # clean up
        # if self._hass:
        #    hass.send_unregister_info()
        self._modbus_client.disconnect()
        self._mqtt_client.stop()
        _LOGGER.info("Exiting")

    def run(self) -> None:
        """Run the coordinator."""
        next_read_config = datetime.now()
        next_read_day = datetime.now()
        next_read_total = datetime.now()
        now_ext_idx = 0

        self._modbus_client.connect()

        # Initialize
        pv_config = None
        while not pv_config:
            if not (pv_config := self.read_mtec_data(group=RegisterGroup.CONFIG)):
                _LOGGER.warning("Can't retrieve initial config - retry in 10 s")
                time.sleep(10)

        topic_base = (  # type: ignore[unreachable]
            f"{self._mqtt_topic}/{pv_config[Register.SERIAL_NO][Register.VALUE]}"
        )
        if self._hass and not self._hass.is_initialized:
            self._hass.initialize(
                mqtt=self._mqtt_client, serial_no=pv_config[Register.SERIAL_NO][Register.VALUE]
            )

        # Main loop - exit on signal only
        while run_status:
            now = datetime.now()

            # Now base
            if pvdata := self.read_mtec_data(group=RegisterGroup.BASE):
                self.write_to_mqtt(pvdata=pvdata, topic_base=topic_base, group=RegisterGroup.BASE)

            # Now extended - read groups in a round-robin - one per loop
            if (group := SECONDARY_REGISTER_GROUPS.get(now_ext_idx)) and (
                pvdata := self.read_mtec_data(group=group)
            ):
                self.write_to_mqtt(pvdata=pvdata, topic_base=topic_base, group=group)

            if now_ext_idx >= 4:
                now_ext_idx = 0
            else:
                now_ext_idx += 1

            # Day
            if next_read_day <= now and (pvdata := self.read_mtec_data(group=RegisterGroup.DAY)):
                self.write_to_mqtt(pvdata=pvdata, topic_base=topic_base, group=RegisterGroup.DAY)
                next_read_day = datetime.now() + timedelta(seconds=self._mqtt_refresh_day)

            # Total
            if next_read_total <= now and (
                pvdata := self.read_mtec_data(group=RegisterGroup.TOTAL)
            ):
                self.write_to_mqtt(pvdata=pvdata, topic_base=topic_base, group=RegisterGroup.TOTAL)
                next_read_total = datetime.now() + timedelta(seconds=self._mqtt_refresh_total)

            # Config
            if next_read_config <= now and (
                pvdata := self.read_mtec_data(group=RegisterGroup.CONFIG)
            ):
                self.write_to_mqtt(
                    pvdata=pvdata, topic_base=topic_base, group=RegisterGroup.CONFIG
                )
                next_read_config = datetime.now() + timedelta(seconds=self._mqtt_refresh_config)

            refresh_now_interval = self._mqtt_refresh_now
            _LOGGER.debug("Sleep %ss", refresh_now_interval)
            time.sleep(refresh_now_interval)

    def read_mtec_data(self, group: RegisterGroup) -> PVDATA_TYPE:
        """Read data from MTEC modbus."""
        _LOGGER.info("Reading registers for group: %s", group)
        registers = self._modbus_client.get_register_list(group=group)
        now = datetime.now()
        data = self._modbus_client.read_modbus_data(registers=registers)
        pvdata: PVDATA_TYPE = {}
        try:  # assign all data
            for register in registers:
                item = self._register_map[register]
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
                                100
                                * (1 - (data["31001"][Register.VALUE] / pvdata["consumption_day"]))
                                if isinstance(pvdata["consumption_day"], float | int)
                                and float(pvdata["consumption_day"]) > 0
                                else 0
                            )
                        elif register == "ownconsumption-day":
                            pvdata[item[Register.MQTT]] = (
                                100
                                * (
                                    1
                                    - data["31000"][Register.VALUE] / data["31005"][Register.VALUE]
                                )
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
                                * (
                                    1
                                    - (data["31104"][Register.VALUE] / pvdata["consumption_total"])
                                )
                                if isinstance(pvdata["consumption_total"], float | int)
                                and float(pvdata["consumption_total"]) > 0
                                else 0
                            )
                        elif register == "ownconsumption-total":
                            pvdata[item[Register.MQTT]] = (
                                100
                                * (
                                    1
                                    - data["31102"][Register.VALUE] / data["31112"][Register.VALUE]
                                )
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

    def write_to_mqtt(self, pvdata: PVDATA_TYPE, topic_base: str, group: RegisterGroup) -> None:
        """Write data to MQTT."""
        for param, data in pvdata.items():
            topic = f"{topic_base}/{group}/{param}"
            if isinstance(data, dict):
                value = data[Register.VALUE]
                if isinstance(value, float):
                    payload = self._mqtt_float_format.format(value)
                elif isinstance(value, bool):
                    payload = f"{value:d}"
                else:
                    payload = value
            elif isinstance(data, float):
                payload = self._mqtt_float_format.format(data)
            elif isinstance(data, bool):
                payload = f"{data:d}"
            else:
                payload = str(data)
            self._mqtt_client.publish(topic=topic, payload=payload)


# ==========================================
def main() -> None:
    """Stat mtec mqtt."""
    global run_status  # noqa: PLW0603  # pylint: disable=global-statement
    run_status = True

    # Initialization
    signal.signal(signalnum=signal.SIGTERM, handler=signal_handler)
    signal.signal(signalnum=signal.SIGINT, handler=signal_handler)

    coordinator = MtecCoordinator()
    coordinator.run()
    coordinator.stop()


if __name__ == "__main__":
    main()
