import requests
from botocore.awsrequest import AWSRequest
from botocore.auth import SigV4Auth
from botocore.credentials import Credentials

from dotenv import load_dotenv
import os

load_dotenv()


# AWS credentials
access_key = os.getenv("ACCESS_KEY")
secret_key = os.getenv("SECRET_KEY")
region = "eu-north-1"  

# API Gateway details
invoke_url = "https://e31gpskeu0.execute-api.eu-north-1.amazonaws.com/PROD/"
api_root = "/health"  
method = "GET"
payload = ""  # Empty payload for a GET request

# Full URL
url = invoke_url + api_root

# Create AWSRequest
request = AWSRequest(method=method, url=url, data=payload)
credentials = Credentials(access_key, secret_key)
SigV4Auth(credentials, "execute-api", region).add_auth(request)

# Send the signed request
response = requests.request(
    method=request.method,
    url=request.url,
    headers=dict(request.headers),
    data=request.body
)

# Print the response
print("Status Code:", response.status_code)
print("Response Body:", response.text)