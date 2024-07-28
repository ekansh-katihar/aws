## install boto3 via `apt install python3-boto3` OR `pip install boto3`
import boto3, os, csv, json, uuid
import traceback

# Replace with your file path and stream name
file_path = "./stocks-kinesis/APPL_6M.csv"  # Update with actual file path
stream_name = "stocks"
session = boto3.Session()
kinesis_client = session.client('kinesis')

def read_csv_and_put_records():
    with open(file_path, 'r') as csvfile:
        reader = csv.DictReader(csvfile)  # Use DictReader for header handling
        records = []
        for row in reader:
            data = {'date': row['date'], 'price': float(row['close'])}
            record = {'Data': json.dumps(data), 'PartitionKey': 'AAPL'}  # Adjust partition key if needed
            records.append(record)

            # Batch records for efficient sending
            if len(records) == 500:  # Adjust batch size as needed
                response = kinesis_client.put_records(StreamName=stream_name, Records=records)
                print(response)
                records = []  # Clear the batch

        # Send any remaining records
        if records:
            response = kinesis_client.put_records(StreamName=stream_name, Records=records)
            print(response)

def read_stream():
    ## Get shard id
    response = kinesis_client.describe_stream(StreamName=stream_name)
    shard_id = response['StreamDescription']['Shards'][0]['ShardId']
    shard_id = 'shardId-000000000002'
    print(shard_id)
    # Get the shard iterator.
    response = kinesis_client.get_shard_iterator(
            StreamName=stream_name,
            ShardId=shard_id,
            ShardIteratorType='TRIM_HORIZON'
        )
    shard_iterator = response['ShardIterator']
    max_records = 100
    record_count = 0

    try:
        while record_count < max_records:
            response = kinesis_client.get_records(
                ShardIterator=shard_iterator,
                Limit=10
            )
            shard_iterator = response['NextShardIterator']
            records = response['Records']
            record_count += len(records)
            print(records)
            

    except Exception as e:
        print(traceback.format_exc())

# Handle potential errors gracefully
try:
    read_csv_and_put_records()
    #print("writing data done, now will read: ")
    #read_stream()
except Exception as e:
    print(f"Error occurred: {e}")
