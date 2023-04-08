import boto3
import random
import base64
import json
import datetime

s3 = boto3.resource('s3')
bucket_name = 'thomasfeduk-kinesis1'


def lambda_handler(event, context):
    try:
        from var_dump import var_dump
    except Exception:
        pass

    i = 1
    for record in event['Records']:
        payload = base64.b64decode(record["kinesis"]["data"])
        data = json.loads(payload)
        filename = ',Starting #' + str(data["number"]).zfill(2) + ' (Order #' + str(i).zfill(2) + ') - ' \
                   + datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S') + ' - '\
                   + str(random.randrange(1, 99999))
        s3.Object(bucket_name, filename).put(Body=json.dumps(event))

        if data["error"]:
            filename = '~Error #' + str(data["number"]).zfill(2) + ' (Order #' + str(i).zfill(2) + ') - ' \
                       + datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S') + ' - ' \
                       + str(random.randrange(1, 99999))
            s3.Object(bucket_name, filename).put(Body=json.dumps(event))
            raise Exception("Error for event #" + str(data["number"]).zfill(2))
            # print("Print Error for event #" + str(data["number"]).zfill(2))
            # return {"batchItemFailures": [{"itemIdentifier": record["kinesis"]["sequenceNumber"]}]}

        filename = '.Success! #' + str(data["number"]).zfill(2) + ' (Order #' + str(i).zfill(2) + ') - ' \
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
