import os
import pandas as pd
import zipfile
import time
from quixstreams import Application


# import our get_app function to help with building the app for local/Quix deployed code
from app_factory import get_app

# import the dotenv module to load environment variables from a file
from dotenv import load_dotenv
load_dotenv(override=False)

# get the environment variable value or default to False
USE_LOCAL_KAFKA=os.getenv("use_local_kafka", False)

# Create an Application.
app = get_app(use_local_kafka=USE_LOCAL_KAFKA)

# Define the topic using the "output" environment variable
topic_name = os.environ["output"]
topic = app.topic(topic_name)

# Create a pre-configured Producer object.
# Producer is already setup to use Quix brokers.
# It will also ensure that the topics exist before producing to them if
# Application.Quix is initiliazed with "auto_create_topics=True".
producer = app.get_producer()

# Define the path to the zip file
zip_file_path = 'messages.csv.zip'

# Extract the contents of the zip file to a temporary directory
with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
    # Extract all the contents to a temporary directory
    zip_ref.extractall('temp')

# Find the CSV file inside the temporary directory
csv_file_path = None
for root, dirs, files in os.walk('temp'):
    for file in files:
        if file.endswith('.csv'):
            csv_file_path = os.path.join(root, file)
            break
    if csv_file_path:
        break

# Check if the CSV file was found
if csv_file_path:
    # Read the CSV file using pandas
    df = pd.read_csv(csv_file_path)

    # Iterate over each row in the DataFrame
    for index, row in df.iterrows():
        # Get the value of the first cell in the row
        first_cell_value = row.iloc[0]
        print(first_cell_value)

        # publish the data to the topic
        producer.produce(
            topic='messages',
            key='message',
            value=first_cell_value
        )

        # Wait for one second
        time.sleep(1)

    # Remove the temporary directory and its contents
    os.remove(csv_file_path)
    os.rmdir('temp')
else:
    print("CSV file not found in the zip folder.")

