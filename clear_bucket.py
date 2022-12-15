import boto3
bucket_name = 'thomasfeduk-kinesis1'

s3 = boto3.resource('s3')
s3.Bucket(bucket_name).objects.all().delete()

sqs = boto3.resource('sqs')

queue = sqs.Queue('https://sqs.us-east-1.amazonaws.com/443035303084/kworker-dlq')
queue.purge()
queue = sqs.Queue('https://sqs.us-east-1.amazonaws.com/443035303084/kworker-dlq-manual')
queue.purge()
queue = sqs.Queue('https://sqs.us-east-1.amazonaws.com/443035303084/kworker-dlq-manual-reallydead')
queue.purge()