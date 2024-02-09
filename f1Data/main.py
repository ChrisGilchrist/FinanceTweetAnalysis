import quixstreams as qx
import os
import fastf1

# Quix injects credentials automatically to the client.
# Alternatively, you can always pass an SDK token manually as an argument.
client = qx.QuixStreamingClient()

# Use Input / Output topics to stream data in or out of your service
consumer_topic = client.get_topic_consumer(os.environ["input"])
producer_topic = client.get_topic_producer(os.environ["output"])

session = fastf1.get_session(2019, 'Monza', 'Q')
session.load(telemetry=False, laps=False, weather=False)
vettel = session.get_driver('VET')
print(f"Pronto {vettel['FirstName']}?")

# for more samples, please see samples or docs
