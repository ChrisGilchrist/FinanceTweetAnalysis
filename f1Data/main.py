import quixstreams as qx
import os
import fastf1

# Quix injects credentials automatically to the client.
# Alternatively, you can always pass an SDK token manually as an argument.
client = qx.QuixStreamingClient()

# Use Input / Output topics to stream data in or out of your service
consumer_topic = client.get_topic_consumer(os.environ["input"])
producer_topic = client.get_topic_producer(os.environ["output"])
session = fastf1.get_session(2021, 7, 'Q')

session.name
session.date
print(session.date)