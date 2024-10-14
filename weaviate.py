import weaviate, os
from weaviate import EmbeddedOptions
import pandas as pd

# === Client creation === #
weaviate_url = "MY-Weaviate-url"

client = weaviate.Client(
    url=weaviate_url,  # Replace with your Weaviate endpoint
)
print(f"Client created? {client.is_ready()}")


# Carica il CSV con pandas
df = pd.read_csv('/data4/hnsw/sift/sift_docs.csv', sep=";")


# Settings for displaying the import progress
counter = 0
interval = 20  # print progress every this many records; should be bigger than the batch_size

def add_object(obj) -> None:
    global counter
    properties = {
        #"embedding": obj["embedding"],
        "randint": obj["randint"],
    }
    client.batch.configure(batch_size=100)  # Configure batch
    with client.batch as batch:
        # Add the object to the batch
        batch.add_data_object(
            data_object=properties,
            class_name="Document",
            vector=obj["embedding"]
        )

        # Calculate and display progress
        counter += 1
        if counter % interval == 0:
            print(f"Imported {counter} articles...")


with pd.read_csv(
    '/data4/hnsw/sift/sift_docs.csv', sep=";",
    usecols=["embedding", "randint"],
    chunksize=100,  # number of rows per chunk
) as csv_iterator:
    # Iterate through the dataframe chunks and add each CSV record to the batch
    for chunk in csv_iterator:
        for index, row in chunk.iterrows():
            add_object(row)

print(f"Finished importing {counter} articles.")
