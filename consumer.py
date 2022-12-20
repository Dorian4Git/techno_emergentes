import json
from kafka import KafkaConsumer
from json import loads

# Create the KafkaConsumer instance
consumer = KafkaConsumer(
    'health_care',
    bootstrap_servers=['sandbox-hdp.hortonworks.com:6667'],
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    consumer_timeout_ms=10000,
    value_deserializer=lambda x: loads(x.decode('utf-8')),
    api_version=(0, 10, 1))

# Initialize a set to store the unique records
unique_records = set()

# Flag to indicate if the first line has been written
first_line_written = False

# Store the first line
first_line = None

# create file in "output/data_consumed.json" to overwrite it
with open('output/data_consumed.json', 'w') as f:
    pass

for message in consumer:
    # Load the message value as a dictionary
    record = message.value

    # Cast specific variables to the desired data type
    record['age'] = int(float(record['age']))
    record['anaemia'] = int(record['anaemia'])
    record['diabetes'] = int(record['diabetes'])
    record['high_blood_pressure'] = int(record['high_blood_pressure'])
    record['sex'] = int(record['sex'])
    record['smoking'] = int(record['smoking'])
    record['time'] = int(record['time'])
    record['DEATH_EVENT'] = int(record['DEATH_EVENT'])
    record['creatinine_phosphokinase'] = float(
        record['creatinine_phosphokinase'])
    record['ejection_fraction'] = float(record['ejection_fraction'])
    record['platelets'] = float(record['platelets'])
    record['serum_creatinine'] = float(record['serum_creatinine'])
    record['serum_sodium'] = float(record['serum_sodium'])

    # Print the record
    print(record)

    # Write the record to a JSON file
    with open('output/data_consumed.json', 'a') as f:

        # Check if the current line is the same as the first line
        if json.dumps(record) == first_line:
            print("Found the same line as the first one, stopping write to file.")
            break
        else:
            first_line = json.dumps(record)
            first_line_written = True

            # Convert the dictionary object to a tuple
            record_tuple = tuple(record.items())

            # Check if the record is not a duplicate
            if record_tuple not in unique_records:
                # Add the record to the set of unique records
                unique_records.add(record_tuple)

                # Write the record to the file
                json.dump(record, f)
                f.write('\n')

# Close the consumer
consumer.close()
