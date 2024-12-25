import boto3
import json
import logging
from custom_encoder import CustomEncoder

# Set up logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# DynamoDB Table Configuration
dynamodbTableName = "Wallets"
dynamodb = boto3.resource("dynamodb")
table = dynamodb.Table(dynamodbTableName)

# HTTP Methods and Paths
GET_METHOD = "GET"
POST_METHOD = "POST"
PATCH_METHOD = "PATCH"
DELETE_METHOD = "DELETE"
HEALTH_PATH = "/health"
WALLET_PATH = "/wallet"
WALLETS_PATH = "/wallets"

def lambda_handler(event, context):
    logger.info(f"Received event: {event}")
    http_method = event["httpMethod"]
    path = event["path"]

    try:
        if http_method == GET_METHOD and path == HEALTH_PATH:
            response = build_response(200, {"status": "Healthy"})
        elif http_method == GET_METHOD and path == WALLET_PATH:
            query_params = event.get("queryStringParameters", {})
            wallet_name = query_params.get("walletName")
            username = query_params.get("username")

            if not wallet_name or not username:
                response = build_response(400, {"Message": "walletName and username are required"})
            else:
                response = get_wallet(wallet_name, username)
                
        elif http_method == GET_METHOD and path == WALLETS_PATH:
            response = get_wallets()
            
        elif http_method == POST_METHOD and path == WALLET_PATH:
            response = save_wallet(json.loads(event["body"]))
            
        elif http_method == PATCH_METHOD and path == WALLET_PATH:
            request_body = json.loads(event["body"])
            wallet_name = request_body.get("walletName")
            username = request_body.get("username")
            update_key = request_body.get("updateKey")
            update_value = request_body.get("updateValue")

            if not wallet_name or not username or not update_key or not update_value:
                response = build_response(400, {"Message": "Missing required fields for updating wallet"})
            else:
                response = modify_wallet(wallet_name, username, update_key, update_value)
                
        elif http_method == DELETE_METHOD and path == WALLET_PATH:
            request_body = json.loads(event["body"])
            wallet_name = request_body.get("walletName")
            username = request_body.get("username")

            if not wallet_name or not username:
                response = build_response(400, {"Message": "walletName and username are required for deletion"})
            else:
                response = delete_wallet(wallet_name, username)
        else:
            response = build_response(404, {"Message": "Path not found"})
    except Exception as e:
        logger.exception("Error processing request")
        response = build_response(500, {"Message": "Internal server error"})
    return response

def get_wallet(wallet_name, username):
    try:
        response = table.get_item(
            Key={
                "walletName": wallet_name,
                "username": username
            }
        )
        if "Item" in response:
            return build_response(200, response["Item"])
        else:
            return build_response(404, {"Message": f"walletName: {wallet_name}, username: {username} not found"})
    except Exception as e:
        logger.exception("Error retrieving wallet")
        return build_response(500, {"Message": "Error retrieving wallet"})

def get_wallets():
    try:
        response = table.scan()
        result = response["Items"]

        while "LastEvaluatedKey" in response:
            response = table.scan(ExclusiveStartKey=response["LastEvaluatedKey"])
            result.extend(response["Items"])

        return build_response(200, {"wallets": result})
    except Exception as e:
        logger.exception("Error retrieving wallets")
        return build_response(500, {"Message": "Error retrieving wallets"})

def save_wallet(request_body):
    try:
        table.put_item(Item=request_body)
        return build_response(200, {
            "Operation": "SAVE",
            "Message": "SUCCESS",
            "Item": request_body
        })
    except Exception as e:
        logger.exception("Error saving wallet")
        return build_response(500, {"Message": "Error saving wallet"})

def modify_wallet(wallet_name, username, update_key, update_value):
    try:
        response = table.update_item(
            Key={
                "walletName": wallet_name,
                "username": username
            },
            UpdateExpression=f"SET {update_key} = :value",
            ExpressionAttributeValues={
                ":value": update_value
            },
            ReturnValues="UPDATED_NEW"
        )
        return build_response(200, {
            "Operation": "UPDATE",
            "Message": "SUCCESS",
            "UpdatedAttributes": response["Attributes"]
        })
    except Exception as e:
        logger.exception("Error updating wallet")
        return build_response(500, {"Message": "Error updating wallet"})

def delete_wallet(wallet_name, username):
    try:
        response = table.delete_item(
            Key={
                "walletName": wallet_name,
                "username": username
            },
            ReturnValues="ALL_OLD"
        )
        if "Attributes" in response:
            return build_response(200, {
                "Operation": "DELETE",
                "Message": "SUCCESS",
                "DeletedItem": response["Attributes"]
            })
        else:
            return build_response(404, {"Message": f"walletName: {wallet_name}, username: {username} not found"})
    except Exception as e:
        logger.exception("Error deleting wallet")
        return build_response(500, {"Message": "Error deleting wallet"})

def build_response(status_code, body=None):
    response = {
        "statusCode": status_code,
        "headers": {
            "Content-Type": "application/json",
            "Access-Control-Allow-Origin": "*"
        }
    }
    if body is not None:
        response["body"] = json.dumps(body, cls=CustomEncoder)
    return response
