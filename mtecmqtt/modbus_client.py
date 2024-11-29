"""
Modbus API for M-TEC Energybutler.

(c) 2023 by Christian Rödel
"""

from __future__ import annotations

import logging
from typing import Any, Final, cast

from pymodbus.client import ModbusTcpClient
from pymodbus.constants import Endian
from pymodbus.exceptions import ModbusException
from pymodbus.framer import FramerType
from pymodbus.payload import BinaryPayloadDecoder
from pymodbus.pdu.register_read_message import ReadHoldingRegistersResponse

from mtecmqtt.const import DEFAULT_FRAMER, Config, Register, RegisterGroup

_LOGGER = logging.getLogger(__name__)


class MTECModbusClient:
    """Modbus API for MTEC Energy Butler."""

    def __init__(
        self,
        config: dict[str, Any],
        register_map: dict[str, dict[str, Any]],
        register_groups: list[str],
    ) -> None:
        """Init the modbus client."""
        self._register_map: Final = register_map
        self._register_groups: Final = register_groups
        self._modbus_client: ModbusTcpClient = None  # type: ignore[assignment]
        self._cluster_cache: Final[dict[str, list[dict[str, Any]]]] = {}

        self._modbus_framer: Final[str] = config.get(Config.MODBUS_FRAMER, DEFAULT_FRAMER)
        self._modbus_host: Final[str] = config[Config.MODBUS_IP]
        self._modbus_port: Final[int] = config[Config.MODBUS_PORT]
        self._modbus_retries: Final[int] = config[Config.MODBUS_RETRIES]
        self._modbus_slave: Final[int] = config[Config.MODBUS_SLAVE]
        self._modbus_timeout: Final[int] = config[Config.MODBUS_TIMEOUT]
        _LOGGER.debug("Modbus client initialized")

    def __del__(self) -> None:
        """Cleanup the modbus client."""
        self.disconnect()

    @property
    def register_groups(self) -> list[str]:
        """Return the register groups."""
        return self._register_groups

    @property
    def register_map(self) -> dict[str, dict[str, Any]]:
        """Return the register map."""
        return self._register_map

    def connect(self) -> bool:
        """Connect to modbus server."""
        _LOGGER.debug(
            "Connecting to server %s:%i (framer=%s)",
            self._modbus_host,
            self._modbus_port,
            self._modbus_framer,
        )
        self._modbus_client = ModbusTcpClient(
            host=self._modbus_host,
            port=self._modbus_port,
            framer=FramerType(self._modbus_framer),
            timeout=self._modbus_timeout,
            retries=self._modbus_retries,
        )

        if self._modbus_client.connect():  # type: ignore[no-untyped-call]
            _LOGGER.debug(
                "Successfully connected to server %s:%i", self._modbus_host, self._modbus_port
            )
            return True
        _LOGGER.error("Couldn't connect to server %s:%i", self._modbus_host, self._modbus_port)
        return False

    def disconnect(self) -> None:
        """Disconnect from Modbus server."""
        if self._modbus_client and self._modbus_client.is_socket_open():
            self._modbus_client.close()  # type: ignore[no-untyped-call]
            _LOGGER.debug("Successfully disconnected from server")

    def get_register_list(self, group: RegisterGroup) -> list[str]:
        """Get a list of all registers which belong to a given group."""
        registers: list[str] = []
        for register, item in self._register_map.items():
            if item[Register.GROUP] == group:
                registers.append(register)

        if len(registers) == 0:
            _LOGGER.error("Unknown or empty register group: %s", group)
            return []
        return registers

    def read_modbus_data(self, registers: list[str] | None = None) -> dict[str, dict[str, Any]]:
        """
        Read modbus data.

        This is the main API function. It either fetches all registers or a list of given registers.
        """
        data: dict[str, dict[str, Any]] = {}
        _LOGGER.debug("Retrieving data...")

        if registers is None:  # Create a list of all (numeric) registers
            registers = []
            for register in self._register_map:
                if (
                    register.isnumeric()
                ):  # non-numeric registers are deemed to be calculated pseudo-registers
                    registers.append(register)

        cluster_list = self._get_register_clusters(registers=registers)
        for reg_cluster in cluster_list:
            offset = 0
            _LOGGER.debug(
                "Fetching data for cluster start %s, length %s, items %s",
                reg_cluster["start"],
                reg_cluster[Register.LENGTH],
                len(reg_cluster["items"]),
            )
            if rawdata := self._read_registers(
                register=reg_cluster["start"], length=reg_cluster[Register.LENGTH]
            ):
                for item in reg_cluster["items"]:
                    if item.get(Register.TYPE):  # type==None means dummy
                        register = str(reg_cluster["start"] + offset)
                        if data_decoded := _decode_rawdata(
                            rawdata=rawdata, offset=offset, item=item
                        ):
                            data.update({register: data_decoded})
                        else:
                            _LOGGER.error("Decoding error while decoding register %s", register)
                    offset += item[Register.LENGTH]

        _LOGGER.debug("Data retrieval completed")
        return data

    def write_register(self, register: str, value: Any) -> bool:
        """Write a value to a register."""
        # Lookup register
        if not (item := self._register_map.get(str(register), None)):
            _LOGGER.error("Can't write unknown register: %s", register)
            return False
        if item.get(Register.WRITABLE, False) is False:
            _LOGGER.error("Can't write register which is marked read-only: %s", register)
            return False

        # check value
        try:
            if isinstance(value, str):
                value = float(value) if "." in value else int(value)
        except Exception:
            _LOGGER.error("Invalid numeric value: %s", value)
            return False

        # adjust scale
        if item[Register.SCALE] > 1:
            value *= item[Register.SCALE]

        try:
            result = self._modbus_client.write_register(
                address=int(register), value=int(value), slave=self._modbus_slave
            )
        except Exception as ex:
            _LOGGER.error("Exception while writing register %s to pymodbus: %s", register, ex)
            return False

        if result.isError():
            _LOGGER.error("Error while writing register %s to pymodbus", register)
            return False
        return True

    def _get_register_clusters(self, registers: list[str]) -> list[dict[str, Any]]:
        """Cluster registers in order to optimize modbus traffic."""
        # Cache clusters to avoid unnecessary overhead
        # use stringified version of list as index
        if (idx := str(registers)) not in self._cluster_cache:
            self._cluster_cache[idx] = self._create_register_clusters(registers=registers)
        return self._cluster_cache[idx]

    def _create_register_clusters(self, registers: list[str]) -> list[dict[str, Any]]:
        """Create clusters."""
        cluster: dict[str, Any] = {"start": 0, Register.LENGTH: 0, "items": []}
        cluster_list: list[dict[str, Any]] = []

        for register in sorted(registers):
            if register.isnumeric():  # ignore non-numeric pseudo registers
                if item := self._register_map.get(register):
                    if (
                        int(register) > cluster["start"] + cluster[Register.LENGTH]
                    ):  # there is a gap
                        if cluster["start"] > 0:  # except for first cluster
                            cluster_list.append(cluster)
                        cluster = {"start": int(register), Register.LENGTH: 0, "items": []}
                    cluster[Register.LENGTH] += item[Register.LENGTH]
                    cluster["items"].append(item)
                else:
                    _LOGGER.warning("Unknown register: %s - skipped.", register)

        if cluster["start"] > 0:  # append last cluster
            cluster_list.append(cluster)

        return cluster_list

    def _read_registers(self, register: str, length: int) -> ReadHoldingRegistersResponse | None:
        """Do the actual reading from modbus."""
        try:
            result: ReadHoldingRegistersResponse = cast(
                ReadHoldingRegistersResponse,
                self._modbus_client.read_holding_registers(
                    address=int(register), count=length, slave=self._modbus_slave
                ),
            )
        except ModbusException as ex:
            _LOGGER.error(
                "Exception while reading register %s, length %s from pymodbus: %s",
                register,
                length,
                ex,
            )
            return None
        if result.isError():
            _LOGGER.error(
                "Error while reading register %s, length %s from pymodbus", register, length
            )
            return None
        if len(result.registers) != length:
            _LOGGER.error(
                "Error while reading register %s from pymodbus: Requested length %s, received %i",
                register,
                length,
                len(result.registers),
            )
            return None
        return result


def _decode_rawdata(
    rawdata: ReadHoldingRegistersResponse, offset: int, item: dict[str, Any]
) -> dict[str, Any]:
    """Decode the result from rawdata, starting at offset."""
    try:
        val = None
        start = rawdata.registers[offset:]
        decoder = BinaryPayloadDecoder.fromRegisters(  # type: ignore[no-untyped-call]
            registers=start, byteorder=Endian.BIG
        )
        item_type = str(item[Register.TYPE])
        item_length = int(item[Register.LENGTH])
        if item_type == "U16":
            val = decoder.decode_16bit_uint()
        elif item_type == "I16":
            val = decoder.decode_16bit_int()
        elif item_type == "U32":
            val = decoder.decode_32bit_uint()
        elif item_type == "I32":
            val = decoder.decode_32bit_int()
        elif item_type == "BYTE":
            if item_length == 1:
                val = f"{decoder.decode_8bit_uint():02d} {decoder.decode_8bit_uint():02d}"
            elif item_length == 2:
                val = f"{decoder.decode_8bit_uint():02d} {decoder.decode_8bit_uint():02d}  {decoder.decode_8bit_uint():02d} {decoder.decode_8bit_uint():02d}"
            elif item_length == 4:
                val = f"{decoder.decode_8bit_uint():02d} {decoder.decode_8bit_uint():02d} {decoder.decode_8bit_uint():02d} {decoder.decode_8bit_uint():02d}  {decoder.decode_8bit_uint():02d} {decoder.decode_8bit_uint():02d} {decoder.decode_8bit_uint():02d} {decoder.decode_8bit_uint():02d}"
        elif item_type == "BIT":
            if item_length == 1:
                val = f"{decoder.decode_8bit_uint():08b}"
            if item_length == 2:
                val = f"{decoder.decode_8bit_uint():08b} {decoder.decode_8bit_uint():08b}"
        elif item_type == "DAT":
            val = f"{decoder.decode_8bit_uint():02d}-{decoder.decode_8bit_uint():02d}-{decoder.decode_8bit_uint():02d} {decoder.decode_8bit_uint():02d}:{decoder.decode_8bit_uint():02d}:{decoder.decode_8bit_uint():02d}"
        elif item_type == "STR":
            val = decoder.decode_string(item_length * 2).decode()
        else:
            _LOGGER.error("Unknown type %s to decode", item_type)
            return {}

        item_scale = int(item[Register.SCALE])
        if val and item_scale > 1:
            val /= item_scale
        return {
            Register.NAME: item[Register.NAME],
            Register.VALUE: val,
            Register.UNIT: item[Register.UNIT],
        }
    except Exception as ex:
        _LOGGER.error("Exception while decoding data: %s", ex)
        return {}
