## install boto3 via `apt install python3-boto3` OR `pip install boto3`
import boto3, json
from decimal import Decimal
from datetime import datetime
import traceback
from botocore.exceptions import ClientError



stream_name = "stocks"
session = boto3.Session()
kinesis_client = session.client('kinesis')
dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('stocks_daily')

def process_record(record):
    print(f"processing record {record}")
    # Extract data from the record
    data = json.loads(record['Data'].decode('utf-8'))    
    # Convert date to YYYYMMDD format
    try:
        date_str = data['date']
        price = Decimal(str(data['price']))
        date_obj = datetime.strptime(date_str, '%m/%d/%Y')
        formatted_date = date_obj.strftime('%Y%m%d')
        numbered_date = int(formatted_date)
    except ValueError:
        print(f"Error parsing date: {date_str}")
        return

    # Prepare data for DynamoDB
    item = {'symbol': 'AAPL', 'date': numbered_date, 'price': price}
    print(f"inserting record {item}")
    # Put data into DynamoDB table
    try:
        table.put_item(Item=item)
        print(f"Successfully inserted data for {formatted_date} at ${price}")
    except ClientError as e:
        print(f"Error putting data to DynamoDB: {e}")

def main():
    response = kinesis_client.describe_stream(StreamName=stream_name)
    # Hard coded to read from the 3rd ( 2nd index shard)
    shard_id = response['StreamDescription']['Shards'][2]['ShardId']
    #shard_id = kinesis_client.get_shard_iterator(StreamName='stocks', ShardIteratorType='TRIM_HORIZON')['ShardIterator']
    # shard_id = kinesis_client.get_shard_iterator(StreamName='stocks', ShardIdIteratorType='LATEST')['ShardIterator']
    print(f"shard id {shard_id}")
    response = kinesis_client.get_shard_iterator(
            StreamName=stream_name,
            ShardId=shard_id,
            ShardIteratorType='TRIM_HORIZON'
        )
    shard_iterator = response['ShardIterator']
    record_count = 0
    try:
        while True:
            response = kinesis_client.get_records(
                ShardIterator=shard_iterator,
                Limit=10000
            )
            records = response['Records']
            if not records:
                break
            # Process each record
            for record in records:
                process_record(record)   
            # Checkpoint the position to ensure we don't re-process records
            shard_iterator = response['NextShardIterator']
            record_count += len(records)
            # TODO: checkpointing not working
            # kinesis_client.put_record(StreamName='stocks', 
            #     ShardId=shard_id, 
            #     Data=json.dumps({'checkpoint': shard_iterator}), 
            #     PartitionKey='default')
    except ClientError as e:
        print(traceback.format_exc())

    print(f"Processed {record_count}")
            

if __name__ == '__main__':
    main()