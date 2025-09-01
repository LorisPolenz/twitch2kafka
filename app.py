import re
import os
import json
import asyncio
import logging
import websockets
from kafka import KafkaProducer
from kafka.errors import KafkaError

# Setup logging to see which channel each message comes from
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# Grok pattern translated to regex
pattern = r":(?P<nick>[^!]*)!(?P<username>[^@]*)@(?P<host>\S+) (?P<command>\w+) (?P<channel>\S+) :(?P<user_message>.*)"


def check_env_variables():
    required_vars = ['TWITCH_CHANNELS',
                     'DESTINATION_KAFKA_TOPIC', 'KAFKA_BOOTSTRAP_SERVERS', 'KAFKA_API_VERSION']
    for var in required_vars:
        if not os.getenv(var):
            logger.error(f"Environment variable '{var}' is not set.")
            return False
    return True


def publish_kafka(message):
    if not message:
        return

    logging.debug(f"Publishing message to Kafka: {message}")

    try:
        producer.send(os.getenv('DESTINATION_KAFKA_TOPIC'),
                      value=message.encode('utf-8'))

    except KafkaError as e:
        logger.error(f"Failed to send message to Kafka: {e}")


def process_message(message):
    # Match and extract
    match = re.match(pattern, message)
    if match:
        data = match.groupdict()
        json_data = json.dumps(data)
        return json_data
    else:
        logging.warning(f"No match found for message: {message}")

    return None


async def read_twitch_chat(channel):
    uri = "wss://irc-ws.chat.twitch.tv:443"
    logging.info(f"Connecting to {channel}...")
    async with websockets.connect(uri) as websocket:
        await websocket.send("NICK justinfan12345\r\n")
        await websocket.send(f"JOIN #{channel}\r\n")

        while True:
            message = await websocket.recv()
            if not message:
                return

            # Ignore messages from the bot user
            if "justinfan12345" in message:
                continue

            processed_message = process_message(message)
            publish_kafka(processed_message)


async def run_multiple_channels_gather(channels):

    # Run multiple chat clients concurrently
    tasks = [read_twitch_chat(channel) for channel in channels]
    await asyncio.gather(*tasks, return_exceptions=True)


# Run before creating Kafka producer
if not check_env_variables():
    logging.error("Missing environment variables. Exiting...")
    exit(1)

producer = KafkaProducer(
    bootstrap_servers=os.getenv('KAFKA_BOOTSTRAP_SERVERS'),
    acks='all',
    api_version=tuple(int(x)
                      for x in os.getenv('KAFKA_API_VERSION').split(","))
)

logging.info("Kafka Producer connected")
logging.debug(f"Producer metrics: {producer.metrics()}")


async def main():
    channels = os.getenv('TWITCH_CHANNELS').split(',')

    await run_multiple_channels_gather(channels)

if __name__ == "__main__":
    print("Starting Twitch chat crawl...")

    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.info("\nShutting down gracefully...")
