import quixstreams as qx
import os

# Quix injects credentials automatically to the client.
# Alternatively, you can always pass an SDK token manually as an argument.
client = qx.QuixStreamingClient()

# Use Input / Output topics to stream data in or out of your service
consumer_topic = client.get_topic_consumer(os.environ["input"])
producer_topic = client.get_topic_producer(os.environ["output"])

# for more samples, please see samples or docs
import pandas as pd
import zipfile
import os
import time

# Define the path to the zip file
zip_file_path = 'messages.csv.zip'

# Extract the contents of the zip file to a temporary directory
with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
    # Extract all the contents to a temporary directory
    zip_ref.extractall('temp')

# Find the Excel file inside the temporary directory
excel_file_path = None
for root, dirs, files in os.walk('temp'):
    for file in files:
        if file.endswith('.csv'):
            excel_file_path = os.path.join(root, file)
            break
    if excel_file_path:
        break

# Check if the Excel file was found
if excel_file_path:
    # Read the Excel file using pandas
    df = pd.read_excel(excel_file_path)

    # Iterate over each row in the DataFrame
    for index, row in df.iterrows():
        # Get the value of the first cell in the row
        first_cell_value = row.iloc[0]
        print(first_cell_value)
        # Wait for one second
        time.sleep(1)

    # Remove the temporary directory and its contents
    os.remove(excel_file_path)
    os.rmdir('temp')
else:
    print("Excel file not found in the zip folder.")