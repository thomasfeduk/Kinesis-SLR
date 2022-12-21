import json
import boto3
from var_dump import var_dump
from includes.debug import die, pvd, pvdd



f = open("includes/payload.json", "r")
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

if "FunctionError" in response.keys():
    print(json.dumps(response, default=str, indent=4))
    die()


print(json.dumps(json.loads(response['Payload'].read().decode("utf-8")), indent=4))
die()
print(json.dumps(response, default=str, indent=4))
