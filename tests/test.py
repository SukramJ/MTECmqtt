#!/usr/bin/env python3
"""
Test connection to M-TEC Energybutler
(c) 2024 by Christian Rödel
"""

from __future__ import annotations

import logging

from pymodbus.client import ModbusTcpClient
from pymodbus.framer import Framer

_LOGGER = logging.getLogger(__name__)


# =====================================================
class MTECmodbusAPI:
    # -------------------------------------------------
    def __init__(self):
        self.modbus_client = None
        self.slave = 0
        self._cluster_cache = {}
        _LOGGER.debug("API initialized")

    def __del__(self):
        self.disconnect()

    # -------------------------------------------------
    # Connect to Modbus server
    def connect(self, ip_addr, port, slave):
        self.slave = slave

        framer = "rtu"
        _LOGGER.debug("Connecting to server %s:%s (framer=%s)", ip_addr, port, framer)
        self.modbus_client = ModbusTcpClient(
            host=ip_addr,
            port=port,
            framer=Framer(framer),
            timeout=5,
            retries=3,
            retry_on_empty=True,
        )

        if self.modbus_client.connect():
            _LOGGER.debug("Successfully connected to server %s:%s", ip_addr, port)
            return True
        _LOGGER.error("Couldn't connect to server %s:%s", ip_addr, port)
        return False

    # -------------------------------------------------
    # Disconnect from Modbus server
    def disconnect(self):
        if self.modbus_client and self.modbus_client.is_socket_open():
            self.modbus_client.close()
            _LOGGER.debug("Successfully disconnected from server")


# --------------------------------
# The main() function is just a demo code how to use the API
def main():
    logging.basicConfig()
    logging.getLogger().setLevel(logging.DEBUG)

    print("Please enter")
    ip_addr = input("espressif server IP Address: ")
    port = input("espressif Port (Standard is 5743): ")

    api = MTECmodbusAPI()
    api.connect(ip_addr=ip_addr, port=port, slave=252)
    api.disconnect()


# --------------------------------------------
if __name__ == "__main__":
    main()
