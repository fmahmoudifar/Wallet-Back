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
            wallet_id = query_params.get("walletId")
            user_id = query_params.get("userId")

            if not wallet_id or not user_id:
                response = build_response(400, {"Message": "walletId and userId are required"})
            else:
                response = get_wallet(wallet_id, user_id)
        elif http_method == GET_METHOD and path == WALLETS_PATH:
            response = get_wallets()
        elif http_method == POST_METHOD and path == WALLET_PATH:
            response = save_wallet(json.loads(event["body"]))
        elif http_method == PATCH_METHOD and path == WALLET_PATH:
            request_body = json.loads(event["body"])
            wallet_id = request_body.get("walletId")
            user_id = request_body.get("userId")
            currency = request_body.get("currency")
            wallet_name = request_body.get("walletName")
            wallet_type = request_body.get("walletType")
            account_number = request_body.get("accountNumber")
            balance = request_body.get("balance")
            note = request_body.get("note")

            if not wallet_id or not user_id:
                response = build_response(400, {"Message": "Missing required fields for updating wallet"})
            else:
                response = modify_wallet(wallet_id, user_id, currency, wallet_name, wallet_type, account_number, balance, note)
        elif http_method == DELETE_METHOD and path == WALLET_PATH:
            request_body = json.loads(event["body"])
            wallet_id = request_body.get("walletId")
            user_id = request_body.get("userId")

            if not wallet_id or not user_id:
                response = build_response(400, {"Message": "walletId and userId are required for deletion"})
            else:
                response = delete_wallet(wallet_id, user_id)
        else:
            response = build_response(404, {"Message": "Path not found"})
    except Exception as e:
        logger.exception("Error processing request")
        response = build_response(500, {"Message": "Internal server error"})
    return response

def get_wallet(wallet_id, user_id):
    try:
        response = table.get_item(
            Key={
                "walletId": wallet_id,
                "userId": user_id
            }
        )
        if "Item" in response:
            return build_response(200, response["Item"])
        else:
            return build_response(404, {"Message": f"walletId: {wallet_id}, userId: {user_id} not found"})
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

def modify_wallet(wallet_id, user_id, currency, wallet_name, wallet_type, account_number, balance, note):
    try:
        update_expression = "SET currency = :currency, walletName = :walletName, walletType = :walletType, accountNumber = :accountNumber, balance = :balance, note = :note"
        expression_attribute_values = {
            ":currency": currency,
            ":walletName": wallet_name,
            ":walletType": wallet_type,
            ":accountNumber": account_number,
            ":balance": balance,
            ":note": note
        }
        
        response = table.update_item(
            Key={
                "walletId": wallet_id,
                "userId": user_id
            },
            UpdateExpression=update_expression,
            ExpressionAttributeValues=expression_attribute_values,
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

def delete_wallet(wallet_id, user_id):
    try:
        response = table.delete_item(
            Key={
                "walletId": wallet_id,
                "userId": user_id
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
            return build_response(404, {"Message": f"walletId: {wallet_id}, userId: {user_id} not found"})
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