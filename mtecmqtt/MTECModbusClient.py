"""
Modbus API for M-TEC Energybutler.

(c) 2023 by Christian Rödel
"""

from __future__ import annotations

import logging
from typing import Any

from pymodbus.client import ModbusTcpClient
from pymodbus.constants import Endian
from pymodbus.framer import FramerRTU
from pymodbus.payload import BinaryPayloadDecoder

from mtecmqtt.config import cfg, register_map

_LOGGER = logging.getLogger(__name__)


class MTECModbusClient:
    """Modbus API for MTEC Energy Butler."""

    def __init__(self):
        """Init the modbus client."""
        self._modbus_client: ModbusTcpClient | None = None
        self._slave = 0
        self._cluster_cache = {}
        _LOGGER.debug("API initialized")

    def __del__(self):
        """Cleanup the modbus client."""
        self.disconnect()

    def connect(self, ip_addr: str, port: int, slave: int) -> bool:
        """Connect to modbus server."""
        self._slave = slave

        framer = cfg.get("MODBUS_FRAMER", "rtu")
        _LOGGER.debug("Connecting to server %s:%i (framer=%s)", ip_addr, port, framer)
        self._modbus_client = ModbusTcpClient(
            host=ip_addr,
            port=port,
            framer=FramerRTU(framer),
            timeout=cfg["MODBUS_TIMEOUT"],
            retries=cfg["MODBUS_RETRIES"],
            retry_on_empty=True,
        )

        if self._modbus_client.connect():
            _LOGGER.debug("Successfully connected to server %s:%i", ip_addr, port)
            return True
        _LOGGER.error("Couldn't connect to server %s:%i", ip_addr, port)
        return False

    def disconnect(self) -> None:
        """Disconnect from Modbus server."""
        if self._modbus_client and self._modbus_client.is_socket_open():
            self._modbus_client.close()
            _LOGGER.debug("Successfully disconnected from server")

    def get_register_list(self, group) -> list[int] | None:
        """Get a list of all registers which belong to a given group."""
        registers = []
        for register, item in register_map.items():
            if item["group"] == group:
                registers.append(register)

        if len(registers) == 0:
            _LOGGER.error("Unknown or empty register group: %s", group)
            return None
        return registers

    def read_modbus_data(self, registers=None) -> dict[int, Any]:
        """
        Read modbus data.

        This is the main API function. It either fetches all registers or a list of given registers.
        """
        data: dict[str, Any] = {}
        _LOGGER.debug("Retrieving data...")

        if registers is None:  # Create a list of all (numeric) registers
            registers = list[int]
            for register in register_map:
                if (
                    register.isnumeric()
                ):  # non-numeric registers are deemed to be calculated pseudo-registers
                    registers.append(register)

        cluster_list = self._get_register_clusters(registers)
        for reg_cluster in cluster_list:
            offset = 0
            _LOGGER.debug(
                "Fetching data for cluster start %s, length %s, items %s",
                reg_cluster["start"],
                reg_cluster["length"],
                len(reg_cluster["items"]),
            )
            if rawdata := self._read_registers(reg_cluster["start"], reg_cluster["length"]):
                for item in reg_cluster["items"]:
                    if item.get("type"):  # type==None means dummy
                        if data_decoded := self._decode_rawdata(rawdata, offset, item):
                            register = str(reg_cluster["start"] + offset)
                            data.update({register: data_decoded})
                        else:
                            _LOGGER.error("Decoding error while decoding register %s", register)
                    offset += item["length"]

        _LOGGER.debug("Data retrieval completed")
        return data

    def write_register(self, register, value) -> bool:
        """Write a value to a register."""
        # Lookup register
        if not (item := register_map.get(str(register), None)):
            _LOGGER.error("Can't write unknown register: %s", register)
            return False
        if item.get("writable", False) is False:
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
        if item["scale"] > 1:
            value *= item["scale"]

        try:
            result = self._modbus_client.write_register(
                address=int(register), value=int(value), slave=self._slave
            )
        except Exception as ex:
            _LOGGER.error("Exception while writing register %s to pymodbus: %s", register, ex)
            return False

        if result.isError():
            _LOGGER.error("Error while writing register %s to pymodbus", register)
            return False
        return True

    def _get_register_clusters(self, registers):
        """Cluster registers in order to optimize modbus traffic."""
        # Cache clusters to avoid unnecessary overhead
        # use stringified version of list as index
        if (idx := str(registers)) not in self._cluster_cache:
            self._cluster_cache[idx] = self._create_register_clusters(registers)
        return self._cluster_cache[idx]

    def _create_register_clusters(self, registers):
        """Create clusters."""
        cluster = {"start": 0, "length": 0, "items": []}
        cluster_list = []

        for register in sorted(registers):
            if register.isnumeric():  # ignore non-numeric pseudo registers
                if item := register_map.get(register):
                    if int(register) > cluster["start"] + cluster["length"]:  # there is a gap
                        if cluster["start"] > 0:  # except for first cluster
                            cluster_list.append(cluster)
                        cluster = {"start": int(register), "length": 0, "items": []}
                    cluster["length"] += item["length"]
                    cluster["items"].append(item)
                else:
                    _LOGGER.warning("Unknown register: %s - skipped.", register)

        if cluster["start"] > 0:  # append last cluster
            cluster_list.append(cluster)

        return cluster_list

    def _read_registers(self, register, length):
        """Do the actual reading from modbus."""
        try:
            result = self._modbus_client.read_holding_registers(
                address=int(register), count=length, slave=self._slave
            )
        except Exception as ex:
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

    def _decode_rawdata(self, rawdata, offset, item):
        """Decode the result from rawdata, starting at offset."""
        try:
            val = None
            start = rawdata.registers[offset:]
            decoder = BinaryPayloadDecoder.fromRegisters(
                registers=start, byteorder=Endian.BIG, wordorder=Endian.BIG
            )
            if item["type"] == "U16":
                val = decoder.decode_16bit_uint()
            elif item["type"] == "I16":
                val = decoder.decode_16bit_int()
            elif item["type"] == "U32":
                val = decoder.decode_32bit_uint()
            elif item["type"] == "I32":
                val = decoder.decode_32bit_int()
            elif item["type"] == "BYTE":
                if item["length"] == 1:
                    val = f"{decoder.decode_8bit_uint():02d} {decoder.decode_8bit_uint():02d}"
                elif item["length"] == 2:
                    val = f"{decoder.decode_8bit_uint():02d} {decoder.decode_8bit_uint():02d}  {decoder.decode_8bit_uint():02d} {decoder.decode_8bit_uint():02d}"
                elif item["length"] == 4:
                    val = f"{decoder.decode_8bit_uint():02d} {decoder.decode_8bit_uint():02d} {decoder.decode_8bit_uint():02d} {decoder.decode_8bit_uint():02d}  {decoder.decode_8bit_uint():02d} {decoder.decode_8bit_uint():02d} {decoder.decode_8bit_uint():02d} {decoder.decode_8bit_uint():02d}"
            elif item["type"] == "BIT":
                if item["length"] == 1:
                    val = f"{decoder.decode_8bit_uint():08b}"
                if item["length"] == 2:
                    val = f"{decoder.decode_8bit_uint():08b} {decoder.decode_8bit_uint():08b}"
            elif item["type"] == "DAT":
                val = f"{decoder.decode_8bit_uint():02d}-{decoder.decode_8bit_uint():02d}-{decoder.decode_8bit_uint():02d} {decoder.decode_8bit_uint():02d}:{decoder.decode_8bit_uint():02d}:{decoder.decode_8bit_uint():02d}"
            elif item["type"] == "STR":
                val = decoder.decode_string(item["length"] * 2).decode()
            else:
                _LOGGER.error("Unknown type %s to decode", item["type"])
                return None

            if val and item["scale"] > 1:
                val /= item["scale"]
            return {"name": item["name"], "value": val, "unit": item["unit"]}
        except Exception as ex:
            _LOGGER.error("Exception while decoding data: %s", ex)
            return None


def main():
    """
    Start the client.

    The main() function is just a demo code how to use the API.
    """
    logging.basicConfig()
    if cfg["DEBUG"] is True:
        logging.getLogger().setLevel(logging.DEBUG)

    api = MTECModbusClient()
    api.connect(ip_addr=cfg["MODBUS_IP"], port=cfg["MODBUS_PORT"], slave=cfg["MODBUS_SLAVE"])

    # fetch all available data
    _LOGGER.info("Fetching all data")
    data = api.read_modbus_data()
    for param, val in data.items():
        _LOGGER.info("- %s : %s", param, val)

    api.disconnect()


# --------------------------------------------
if __name__ == "__main__":
    main()
