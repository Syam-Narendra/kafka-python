from confluent_kafka import Producer
import requests
import time
import json

producer = Producer({"bootstrap.servers": "localhost:9092"})


try:
    counter = 0

    def sendMsg():
        global counter
        counter += 1
        msg = requests.get("https://official-joke-api.appspot.com/random_joke")
        print(str(counter) + " Joke sent")
        # joke = json.loads(msg.text)
        # print(joke)
        producer.produce("random-joke", value=msg.text)
        sendMsg()

    sendMsg()

except Exception as e:
    print(f"Error: {e}")
finally:
    producer.flush()
