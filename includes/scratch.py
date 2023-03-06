from includes.debug import *



# Work in progresss for the raw kinesis api

import datetime
import hashlib
import hmac
import http.client
import json
import urllib.parse

region_name = 'us-east-1'
stream_name = 'user-activities'
shard_id = 'shardId-000000000004'
starting_sequence_number = '49637936497446807206520640457435500652579383632734453826'
limit = 25
access_key = ''
secret_key = ''

# Set up the HTTP connection
conn = http.client.HTTPSConnection(f'kinesis.{region_name}.amazonaws.com')

# Build the request data
data = {
    'ShardIterator': starting_sequence_number,
    'Limit': limit
}
data_json = json.dumps(data)

# Generate a timestamp for the request
t = datetime.datetime.utcnow()
amz_date = t.strftime('%Y%m%dT%H%M%SZ')
date_stamp = t.strftime('%Y%m%d')

# Set up the parameters for the AWS request
method = 'POST'
path = '/'
host = f'kinesis.{region_name}.amazonaws.com'
region = region_name
service = 'kinesis'

# Generate the canonical request
payload_hash = hashlib.sha256(data_json.encode('utf-8')).hexdigest()
canonical_headers = f'content-type:application/x-amz-json-1.1\nhost:{host}\nx-amz-date:{amz_date}\n'
signed_headers = 'content-type;host;x-amz-date'
canonical_request = f'{method}\n{path}\n\n{canonical_headers}\n{signed_headers}\n{payload_hash}'
canonical_request_hash = hashlib.sha256(canonical_request.encode('utf-8')).hexdigest()

# Generate the string to sign
algorithm = 'AWS4-HMAC-SHA256'
credential_scope = f'{date_stamp}/{region}/{service}/aws4_request'
string_to_sign = f'{algorithm}\n{amz_date}\n{credential_scope}\n{canonical_request_hash}'

# Generate the signing key
k_date = hmac.new(('AWS4' + secret_key).encode('utf-8'), date_stamp.encode('utf-8'), hashlib.sha256).digest()
k_region = hmac.new(k_date, region.encode('utf-8'), hashlib.sha256).digest()
k_service = hmac.new(k_region, service.encode('utf-8'), hashlib.sha256).digest()
k_signing = hmac.new(k_service, b'aws4_request', hashlib.sha256).digest()

# Generate the signature
signature = hmac.new(k_signing, string_to_sign.encode('utf-8'), hashlib.sha256).hexdigest()

# Build the authorization header
auth_params = f'{algorithm} Credential={access_key}/{credential_scope}, SignedHeaders={signed_headers}, Signature={signature}'

# Build the request headers
headers = {
    'Content-Length': str(len(data_json)),
    'Content-Type': 'application/x-amz-json-1.1',
    'Host': host,
    'X-Amz-Date': amz_date,
    'X-Amz-Target': 'Kinesis_20131202.GetRecords',
    'Authorization': auth_params
}

pvd(f"method: {method}")
pvd(f"host: {host}")
pvd(f"path: {path}")
pvd(f"data_json: {data_json}")
pvd(f"headers: {headers}")

# Send the request
conn.request(method, f"{host}{path}", body=data_json, headers=headers)

# Read the response
response = conn.getresponse()
if response.status != 200:
    raise Exception(f'Error getting records: {response.status} {response.reason}')

# Parse the response
data = response.read()
# data = response.read().decode('utf-8')

# Extract the rec

pvdd(data)

#####################################################
#####################################################
#####################################################
#####################################################
#####################################################

def serialize(obj):
    if isinstance(obj, (str, int, float, bool)):
        return obj
    elif isinstance(obj, list):
        return [serialize(item) for item in obj]
    elif isinstance(obj, dict):
        return {key: serialize(value) for key, value in obj.items()}
    elif hasattr(obj, '__dict__'):
        return serialize({k: v for k, v in vars(obj).items() if isinstance(v, property)})
    return str(obj)


#####################################################
#####################################################
#####################################################
#####################################################
#####################################################




def __json_encode__(self):
    return {"name": self.name, "age": self.age}


def to_json(self):
    return json.dumps(self, default=lambda o: o.__json_encode__())


def __str__(self):
    return self.to_json()

#####################################################
#####################################################
#####################################################
#####################################################
#####################################################


class Person:
    def __init__(self, name, age):
        self.name = name
        self.age = age

    def __repr__(self):
        return f"Person({self.name}, {self.age})"

    def __eq__(self, other):
        if isinstance(other, Person):
            return self.name == other.name and self.age == other.age
        return False

    def __json_encode__(self):
        return {"name": self.name, "age": self.age}

    def to_json(self):
        return json.dumps(self, default=lambda o: o.__json_encode__())

    def __str__(self):
        return self.to_json()

person = Person("Alice", 30)
json_string = str(person)
print(json_string)

die()

