import boto3
import random
import base64
import json
import datetime

s3 = boto3.resource('s3')
bucket_name = 'thomasfeduk-kinesis1'

sqs = boto3.resource('sqs')
queue = sqs.Queue('https://sqs.us-east-1.amazonaws.com/443035303084/kworker-dlq-manual')

def lambda_handler(event, context):
    try:
        from var_dump import var_dump
    except Exception:
        pass

    print(f"Records length: {len(event['Records'])}")
    # print("Event:")
    # print(event)

    i = 1
    for record in event['Records']:
        payload = base64.b64decode(record["kinesis"]["data"])
        data = json.loads(payload)
        print("Data: ")
        print(data)
        filename = ',K-Starting #' + str(data["number"]).zfill(2) + ' (Order #' + str(i).zfill(2) + ') - ' \
                   + datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S') + ' - '\
                   + str(random.randrange(1, 99999))
        # s3.Object(bucket_name, filename).put(Body=json.dumps(event))

        if data["error"]:
            print("Writing message to sqs dlq: " + str(data["number"]))
            filename = '~K-Error #' + str(data["number"]).zfill(2) + ' (Order #' + str(i).zfill(2) + ') - ' \
                       + datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S') + ' - ' \
                       + str(random.randrange(1, 99999))
            s3.Object(bucket_name, filename).put(Body=json.dumps(event))
            response = queue.send_message(
                MessageBody=json.dumps(record),
            )
            # raise Exception("Error for event #" + str(data["number"]).zfill(2))
            continue
        filename = '.K-Success! #' + str(data["number"]).zfill(2) + ' (Order #' + str(i).zfill(2) + ') - ' \
                   + datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S') + ' - '\
                   + str(random.randrange(1, 99999))
        s3.Object(bucket_name, filename).put(Body=json.dumps(event))
        i += 1
    return {"batchItemFailures": []}


if __name__ == "__main__":
    f = open('event.json', )
    event = json.loads(f.read())
    # Closing file
    f.close()
    lambda_handler(event, "context")
