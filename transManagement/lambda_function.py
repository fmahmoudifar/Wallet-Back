import boto3
import json
import logging
from custom_encoder import CustomEncoder

logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodbTableName = "Transactions"
dynamodb = boto3.resource("dynamodb")
table = dynamodb.Table(dynamodbTableName)

GET_METHOD = "GET"
POST_METHOD = "POST"
PATCH_METHOD = "PATCH"
DELETE_METHOD = "DELETE"
HEALTH_PATH = "/health"
TRANSACTOIN_PATH = "/transaction"
TRANSACTOINS_PATH = "/transactions"

def lambda_handler(event, context):
    logger.info(f"Received event: {event}")    
    http_method = event["httpMethod"]
    path = event["path"]
    
    try:
        if http_method == GET_METHOD and path == HEALTH_PATH:
            response = build_response(200, {"status": "Healthy"})
            
        elif http_method == GET_METHOD and path == TRANSACTOIN_PATH:
            query_params = event.get("queryStringParameters", {})
            trans_id = query_params.get("transId")
            username = query_params.get("username")
            
            if not trans_id or not username:
                response = build_response(400, {"Message": "transId and username are required"})
            else:
                response = get_transaction(trans_id, username)
            
        elif http_method == GET_METHOD and path == TRANSACTOINS_PATH:
            response = get_transactions()
            
        elif http_method == POST_METHOD and path == TRANSACTOIN_PATH:
            response == save_transaction(json.loads(event["body"]))
            
        elif http_method == PATCH_METHOD and path == TRANSACTOIN_PATH:
            request_body = json.loads(event["body"])
            trans_id = request_body.get("transId")
            username = request_body.get("username")
            update_key = request_body.get("updateKey")
            update_value = request_body.get("updateValue")
            
            if not trans_id or not username or not update_key or not update_value:
                response = build_response(400, {"Message": "Missing required fields for updating the transaction"})
            else:
                response = modify_transaction(trans_id, username, update_key, update_value)
            
            
                

def build_response(status_code, body=None):
    