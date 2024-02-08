import os
from quixstreams import Application, State
from quixstreams.models.serializers.quix import JSONDeserializer, JSONSerializer
from transformers import pipeline

# import our get_app function to help with building the app for local/Quix deployed code
from app_factory import get_app

# import the dotenv module to load environment variables from a file
from dotenv import load_dotenv
load_dotenv(override=False)

# get the environment variable value or default to False
USE_LOCAL_KAFKA=os.getenv("use_local_kafka", False)

# Create an Application.
app = get_app(use_local_kafka=USE_LOCAL_KAFKA)

# Set the pipeline
classifier = pipeline("text-classification", model="StephanAkkerman/FinTwitBERT-sentiment")

input_topic = app.topic(os.environ["input"], value_deserializer=JSONDeserializer())
output_topic = app.topic(os.environ["output"], value_serializer=JSONSerializer())

# Create a StreamingDataFrame instance
# StreamingDataFrame is a primary interface to define the message processing pipeline
sdf = app.dataframe(topic=input)

# Print the incoming messages
sdf = sdf.update(lambda value: print('Received a message:', value))

if __name__ == "__main__":
    # Run the streaming application 
    app.run(sdf)
