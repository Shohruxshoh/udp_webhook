import asyncio
import os
import json
import random
import hashlib
import logging
import jwt
import time
from pythonjsonlogger import jsonlogger
from dotenv import load_dotenv

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
dotenv_path = os.path.join(BASE_DIR, ".env")
load_dotenv(dotenv_path)

logger = logging.getLogger("udp_client")
logHandler = logging.StreamHandler()
formatter = jsonlogger.JsonFormatter(
    "%(asctime)s %(levelname)s %(name)s %(message)s"
)
logHandler.setFormatter(formatter)
logger.addHandler(logHandler)
logger.setLevel(logging.INFO)

PORT = int(os.getenv("PORT", 9999))

SERVER_ADDR = ("nginx", PORT) 
RETRY_LIMIT = 3
INTERVAL = 5

SECRET_KEY = os.getenv("SECRET_KEY")
ALGORITHM = os.getenv("ALGORITHM")
JWT_EXP = 30  

def generate_jwt() -> str:
    """Generates a JWT token for each message"""
    payload = {
        "iss": "udp_client",
        "iat": int(time.time()),
        "exp": int(time.time()) + JWT_EXP,
    }
    return jwt.encode(payload, SECRET_KEY, algorithm=ALGORITHM)


class UDPClientProtocol(asyncio.DatagramProtocol):
    def __init__(self):
        self.transport = None

    def connection_made(self, transport):
        self.transport = transport
        logger.info({"event": "connection_made", "server": SERVER_ADDR})

    def error_received(self, exc):
        logger.error({"event": "udp_error", "error": str(exc)})

    def connection_lost(self, exc):
        logger.warning({"event": "connection_lost", "error": str(exc)})


def add_checksum_and_token(message: dict) -> dict:
    """Adds checksum and JWT to the message"""
    msg_copy = message.copy()
    msg_copy["token"] = generate_jwt()
    raw = json.dumps({k: v for k, v in msg_copy.items() if k != "checksum"}, sort_keys=True).encode("utf-8")
    checksum = hashlib.md5(raw).hexdigest()
    msg_copy["checksum"] = checksum
    return msg_copy


async def send_with_retry(protocol: UDPClientProtocol, message: dict, client_id: int):
    """ Sends information to Service1 """
    msg_signed = add_checksum_and_token(message)
    data = json.dumps(msg_signed).encode("utf-8")

    for attempt in range(1, RETRY_LIMIT + 1):
        try:

            protocol.transport.sendto(data)
            logger.info(
                {
                    "event": "message_sent",
                    "client_id": client_id,
                    "message": msg_signed,
                    "attempt": attempt,
                }
            )
            break
        except Exception as e:
            logger.warning(
                {
                    "event": "send_retry",
                    "client_id": client_id,
                    "attempt": attempt,
                    "error": str(e),
                }
            )
            await asyncio.sleep(1)


async def client_task(client_id: int, protocol: UDPClientProtocol):
    """ It passes the data to the send_with_retry() function and sends it every 5 seconds. """
    counter = 1
    while True:
        msg = {"client_id": client_id, "text": f"This is being sent via the UDP protocol. #{counter}"}
        await send_with_retry(protocol, msg, client_id)
        counter += 1
        await asyncio.sleep(INTERVAL)


async def main():
    loop = asyncio.get_running_loop()
    transport, protocol = await loop.create_datagram_endpoint(
        lambda: UDPClientProtocol(),
        remote_addr=SERVER_ADDR,
    )

    try:
        tasks = await client_task(1, protocol=protocol)
        await asyncio.gather(*tasks)
    finally:
        transport.close()
        logger.info({"event": "client_stopped"})


if __name__ == "__main__":
    asyncio.run(main())
