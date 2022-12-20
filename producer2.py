import csv
import json
from kafka import KafkaProducer

# Create the Kafka producer
producer = KafkaProducer(bootstrap_servers='sandbox-hdp.hortonworks.com:6667',
                         value_serializer=lambda x: json.dumps(
                             x).encode('utf-8'),
                         api_version=(0, 10, 1))

# Open the CSV file
with open('heart_failure_clinical_records_dataset.csv', 'r') as f:
    # Create a CSV reader
    reader = csv.DictReader(f)

    # Open the JSON file for writing
    with open('output/data_produced.json', 'w') as outfile:
        # Iterate over the rows in the CSV file
        for row in reader:
            # Convert the row to a JSON object
            data = json.dumps(row)

            # Send the data to the Kafka topic
            producer.send('health_care', value=data)

            # Write the data to the JSON file
            json.dump(row, outfile)
            outfile.write('\n')

# Flush the producer to ensure all messages are sent
producer.flush()
