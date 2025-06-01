from confluent_kafka import Consumer
import json
import logging
import os


logging.basicConfig(level=logging.INFO)


conf = {
    "bootstrap.servers": os.environ.get("BROKER_URL"),
    "group.id": os.environ.get("GROUP_ID"),
    "auto.offset.reset": "earliest",
}


# https://docs.confluent.io/kafka-clients/python/current/overview.html#ak-consumer
def consume_messages():
    consumer = Consumer(conf)
    consumer.subscribe([os.environ.get("TOPIC_NAME")])
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                logging.info("No message")
                continue
            if msg.error():
                print(f"Error while consuming messages: {msg.error()}")
                logging.error(msg.error())
            data = json.loads(msg.value().decode("utf-8"))
            print(f"message: {data}")
            logging.info(data)
    finally:
        consumer.close()
        logging.info("Consumer closed")


def startup():
    logging.info("Starting consumer...")
    consume_messages()


# https://stackoverflow.com/questions/75839415/kafka-fastapi-docker-template
if __name__ == "__main__":
    try:
        startup()
    except Exception as e:
        print(f"Exception occurred: {e}")
