from confluent_kafka import Producer
import json
import os

# https://docs.confluent.io/kafka-clients/python/current/overview.html#ak-producer
conf = {"bootstrap.servers": os.environ.get("BROKER_URL")}
producer = Producer(conf)


def send_messages(err, msg):
    if err is not None:
        print(f"配信失敗: {err}")
    else:
        print(f"メッセージ配信成功: {msg.value().decode('utf-8')}")


message = {"event": "task_done", "id": 123}
producer.produce(topic="my-topic", value=json.dumps(message), callback=send_messages)
producer.flush()
