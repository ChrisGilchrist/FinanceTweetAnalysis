import quixstreams as qx
import os

# Quix injects credentials automatically to the client.
# Alternatively, you can always pass an SDK token manually as an argument.
client = qx.QuixStreamingClient()

# Use Input / Output topics to stream data in or out of your service
producer_topic = client.get_topic_producer(os.environ["output"])

import pandas as pd
import zipfile
import os
import time

# Define the path to the zip file
zip_file_path = 'chris.zip'

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
        # Wait for one second
        time.sleep(1)

    # Remove the temporary directory and its contents
    os.remove(csv_file_path)
    os.rmdir('temp')
else:
    print("CSV file not found in the zip folder.")

