import pika
import psycopg2
import json
import os
import time
import logging
from pythonjsonlogger import jsonlogger
from dotenv import load_dotenv

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
dotenv_path = os.path.join(BASE_DIR, ".env")
load_dotenv(dotenv_path)

logger = logging.getLogger("rabbitmq_consumer")
logHandler = logging.StreamHandler()
formatter = jsonlogger.JsonFormatter("%(asctime)s %(levelname)s %(name)s %(message)s")
logHandler.setFormatter(formatter)
logger.addHandler(logHandler)
logger.setLevel(logging.INFO)

RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "rabbitmq")
QUEUE_NAME = os.getenv("QUEUE_NAME", "udp_messages")

DB_NAME = os.getenv("POSTGRES_DB", "clientdb")
DB_USER = os.getenv("POSTGRES_USER", "postgres")
DB_PASS = os.getenv("POSTGRES_PASSWORD", "12345")
DB_HOST = os.getenv("POSTGRES_HOST", "localhost")
DB_PORT = os.getenv("POSTGRES_PORT", "5432")

conn = None

def connect_postgres():
    """Retrying to connect to PostgreSQL"""
    global conn
    while True:
        try:
            conn = psycopg2.connect(
                dbname=DB_NAME,
                user=DB_USER,
                password=DB_PASS,
                host=DB_HOST,
                port=DB_PORT,
            )
            conn.autocommit = True
            logger.info({"event": "postgres_connected"})
            return conn
        except psycopg2.OperationalError as e:
            logger.error({"event": "postgres_unavailable", "error": str(e), "retry_in": 5})
            time.sleep(5)


def create_table():
    """ Creates a table """
    with conn.cursor() as cur:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS udp_messages (
                id SERIAL PRIMARY KEY,
                client_id INT,
                text TEXT,
                token TEXT,
                checksum TEXT,
                received_at TIMESTAMP DEFAULT NOW()
            )
            """
        )
    logger.info({"event": "table_ready"})


def callback(ch, method, properties, body):
    """ Writes data to a table """
    try:
        message = json.loads(body.decode("utf-8"))

        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO udp_messages (client_id, text, token, checksum)
                VALUES (%s, %s, %s, %s)
                """,
                (
                    message.get("client_id"),
                    message.get("text"),
                    message.get("token"),
                    message.get("checksum"),
                ),
            )

        logger.info({"event": "db_inserted", "message": message})

        ch.basic_ack(delivery_tag=method.delivery_tag)

    except Exception as e:
        logger.error({"event": "db_insert_error", "error": str(e), "body": body})
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)


def main():
    global conn
    conn = connect_postgres()
    create_table()

    while True:
        try:
            connection = pika.BlockingConnection(
                pika.ConnectionParameters(host=RABBITMQ_HOST)
            )
            channel = connection.channel()
            channel.queue_declare(queue=QUEUE_NAME, durable=True)

            logger.info({"event": "rabbitmq_connected"})

            channel.basic_qos(prefetch_count=1)
            channel.basic_consume(queue=QUEUE_NAME, on_message_callback=callback)

            logger.info({"event": "consumer_started"})
            channel.start_consuming()

        except pika.exceptions.AMQPConnectionError:
            logger.error({"event": "rabbitmq_unavailable", "retry_in": 5})
            time.sleep(5)


if __name__ == "__main__":
    main()
