import quixstreams as qx
import os

# Quix injects credentials automatically to the client.
# Alternatively, you can always pass an SDK token manually as an argument.
client = qx.QuixStreamingClient()

# Use Input / Output topics to stream data in or out of your service
producer_topic = client.get_topic_producer(os.environ["output"])

# for more samples, please see samples or docs
import pandas as pd
import time

# Read the Excel file
df = pd.read_excel('chris.xlsx')

# Iterate over each row in the DataFrame
for index, row in df.iterrows():
    # Iterate over each cell value in the row
    for cell_value in row:
        print(cell_value)
        # Wait for one second
        time.sleep(1)

