import csv

from confluent_kafka import Producer
from time import sleep
from ride import Ride
from config import config


def read_messages(source_file):
    """
    Read messages from the given recorded CSV file.
    """
    messages = list()

    with open(source_file, 'r') as f:
        reader = csv.reader(f)
        _ = next(reader)
        
        for row in reader:
            data_key = int(row[0])
            data = Ride(row)
            message = {
                data_key: data
            }
            
            messages.append(message)

    return messages


def delivery_report(err, event):
    """
    Callback on delivery.
    """
    if err is not None:
        print(f"Delivery failed on reading for {event.key().decode('UTF-8')}: {err}")
    else:
        print(f"Ride reading for {event.key().decode('UTF-8')} produced to {event.topic()}")


def publish(producer, topic, messages):
    """
    Publish event messages to a kafka topic.    
    """
    for message in messages:
        key = list(message.keys())[0]
        value = list(message.values())[0]
        producer.produce(topic=topic, 
                         key=key, 
                         value=value, 
                         on_delivery=delivery_report)
        
    producer.flush()
    sleep(1)


def main():
    """
    Ingest streaming data.
    """
    source_file = "data/raw/head.csv"
    topic = "green_rides"
    
    producer = Producer(config)
    messages = read_messages(source_file)
    publish(producer, topic=topic, messages=messages)


if __name__ == "__main__":
    main()