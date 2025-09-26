import asyncio
import json
import pika
import time
import os
import hashlib
import logging
import jwt
from pythonjsonlogger import jsonlogger
from dotenv import load_dotenv

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
dotenv_path = os.path.join(BASE_DIR, ".env")
load_dotenv(dotenv_path)

logger = logging.getLogger("udp_server")
logHandler = logging.StreamHandler()
formatter = jsonlogger.JsonFormatter("%(asctime)s %(levelname)s %(name)s %(message)s")
logHandler.setFormatter(formatter)
logger.addHandler(logHandler)
logger.setLevel(logging.INFO)


RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "rabbitmqtest")
QUEUE_NAME = os.getenv("QUEUE_NAME", "udp_messagestest")

SECRET_KEY = os.getenv("SECRET_KEY")
ALGORITHM = os.getenv("ALGORITHM")
PORT = int(os.getenv("PORT", 9999))


def connect_rabbitmq():
    """It keeps trying to connect to RabbitMQ."""
    while True:
        try:
            connection = pika.BlockingConnection(
                pika.ConnectionParameters(host=RABBITMQ_HOST)
            )
            channel = connection.channel()
            channel.queue_declare(queue=QUEUE_NAME, durable=True)
            logger.info({"event": "rabbitmq_connected"})
            return connection, channel
        except pika.exceptions.AMQPConnectionError:
            logger.error({"event": "rabbitmq_unavailable", "retry_in": 5})
            time.sleep(5)


def verify_checksum(message: dict) -> bool:
    """Checks the checksum of the incoming message for correctness."""
    if "checksum" not in message:
        return False
    checksum = message.pop("checksum")
    raw = json.dumps(message, sort_keys=True).encode("utf-8")
    calc = hashlib.md5(raw).hexdigest()
    return checksum == calc


def verify_jwt(token: str) -> bool:
    """JWT token verification"""
    try:
        jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        return True
    except jwt.ExpiredSignatureError:
        logger.error({"event": "jwt_expired"})
        return False
    except jwt.InvalidTokenError:
        logger.error({"event": "jwt_invalid"})
        return False


class UDPServerProtocol(asyncio.DatagramProtocol):
    def __init__(self):
        self.connection, self.channel = connect_rabbitmq()

    def datagram_received(self, data, addr):
        """ Checks the incoming message and writes to RabbitMQ """
        try:
            message = json.loads(data.decode("utf-8"))
            token = message.get("token")
            if not token or not verify_jwt(token):
                logger.error(
                    {"event": "auth_failed", "addr": f"{addr[0]}:{addr[1]}"}
                )
                return

            if verify_checksum(message.copy()):
                logger.info(
                    {
                        "event": "udp_message_received",
                        "addr": f"{addr[0]}:{addr[1]}",
                        "message": message,
                        "checksum_valid": True,
                    }
                )

                try:
                    self.channel.basic_publish(
                        exchange="",
                        routing_key=QUEUE_NAME,
                        body=json.dumps(message),
                        properties=pika.BasicProperties(delivery_mode=2),
                    )
                    logger.info({"event": "rabbitmq_published", "message": message})
                except pika.exceptions.AMQPError:
                    logger.warning({"event": "rabbitmq_connection_lost"})
                    self.connection, self.channel = connect_rabbitmq()
                    self.channel.basic_publish(
                        exchange="",
                        routing_key=QUEUE_NAME,
                        body=json.dumps(message),
                        properties=pika.BasicProperties(delivery_mode=2),
                    )
                    logger.info({"event": "rabbitmq_republished", "message": message})
            else:
                logger.error(
                    {
                        "event": "checksum_failed",
                        "addr": f"{addr[0]}:{addr[1]}",
                        "message": message,
                    }
                )

        except json.JSONDecodeError:
            logger.error(
                {
                    "event": "invalid_json",
                    "addr": f"{addr[0]}:{addr[1]}",
                    "raw_data": data.decode(errors="ignore"),
                }
            )

    def connection_lost(self, exc):
        if self.connection and self.connection.is_open:
            self.connection.close()
            logger.info({"event": "server_connection_closed"})


async def main():
    logger.info({"event": "udp_server_started", "host": "0.0.0.0", "port": PORT})
    loop = asyncio.get_running_loop()
    transport, _ = await loop.create_datagram_endpoint(
        lambda: UDPServerProtocol(), local_addr=("0.0.0.0", PORT)
    )
    try:
        await asyncio.Future()
    finally:
        transport.close()


if __name__ == "__main__":
    asyncio.run(main())
