import json
import boto3
from var_dump import var_dump
from debug import die, pvd, pvdd

f = open("payload.json", "r")
payload = f.read()
f.close()

client = boto3.client('lambda')
response = client.invoke(
    FunctionName='scraper',
    InvocationType='RequestResponse',
    LogType='Tail',
    # ClientContext='string',
    Payload=payload,
    # Qualifier='LATEST'
)

print(json.dumps(response, default=str, indent=4))
