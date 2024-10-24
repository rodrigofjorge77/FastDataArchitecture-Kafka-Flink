import json
import time
from confluent_kafka import SerializingProducer
from datetime import datetime
import requests

def delivery_report(err, msg):
    if err is not None:
        print(f'Message delivery failed: {err}')
    else:
        print(f"Message delivered to {msg.topic} [{msg.partition()}]")

def main():

    url = "https://random-data-api.com/api/commerce/random_commerce"

    for count in range(100):

        payload = {}
        headers = {}

        response = requests.request("GET", url, headers=headers, data=payload)

        dResp = json.loads(response.text)

        topic = 'sales_transactions'
        producer= SerializingProducer({'bootstrap.servers': 'localhost:9092'})

        curr_time = datetime.now()
        
        try:

            print(response.text)

            producer.produce(topic,
                                key   = str(dResp["id"]),
                                value = response.text,
                                on_delivery = delivery_report
                                )
            producer.poll(0)

                #wait for 5 seconds before sending the next transaction
            time.sleep(2)
        except BufferError:
            print("Buffer full! Waiting...")
            time.sleep(1)
        except Exception as e:
            print(e)

if __name__ == "__main__":
    main()
