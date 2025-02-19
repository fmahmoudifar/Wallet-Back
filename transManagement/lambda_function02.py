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
HEALTH_PATH = "/healthT"
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
                try:
                    trans_id = int(trans_id)  # Convert transId to an integer
                    response = get_transaction(trans_id, username)
                except ValueError:
                    response = build_response(400, {"Message": "transId must be a valid number"})
            
        elif http_method == GET_METHOD and path == TRANSACTOINS_PATH:
            response = get_transactions()
            
        elif http_method == POST_METHOD and path == TRANSACTOIN_PATH:
            response = save_transaction(json.loads(event["body"]))
   
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
        
        elif http_method == DELETE_METHOD and path == TRANSACTOIN_PATH:
            request_body = json.loads(event["body"])
            trans_id = request_body.get("transId")
            username = request_body.get("username")
            
            if not trans_id or not username:
                response = build_response(400, {"Message": "transId and username are required"})
            else:
                response = delete_transaction(trans_id, username)
        
        else:
            response = build_response(404, {"Message": "Path not found"})
    except Exception as e:
        logger.exception("Error processing request")
        response = build_response(500, {"Message": "Internal server error"})
    return response

def get_transaction(trans_id, username):
    try:
        if not isinstance(trans_id, int):
            try:
                trans_id = int(trans_id)
            except ValueError:
                return build_response(400, {"Message": "transId must be a valid number"})

        logger.info(f"Fetching transaction with Key: {{'transId': {trans_id}, 'username': '{username}'}}")

        response = table.get_item(
            Key={
                "transId": trans_id,
                "username": username
            }
        )

        if "Item" in response:
            return build_response(200, response["Item"])
        else:
            return build_response(404, {"Message": f"Transaction not found for transId: {trans_id}, username: {username}"})
    except Exception as e:
        logger.exception("Error retrieving transaction")
        return build_response(500, {"Message": "Error retrieving transaction"})


def get_transactions():
    try:
        response = table.scan()
        result = response["Items"]

        while "LastEvaluatedKey" in response:
            response = table.scan(ExclusiveStartKey=response["LastEvaluatedKey"])
            result.extend(response["Items"])

        return build_response(200, {"transactions": result})
    except Exception as e:
        logger.exception("Error retrieving transactions")
        return build_response(500, {"Message": "Error retrieving transactions"})

def save_transaction(request_body):
    try:
        table.put_item(Item=request_body)
        return build_response(200, {
            "Operation": "SAVE",
            "Message": "SUCCESS",
            "Item": request_body
        })
    except Exception as e:
        logger.exception("Error saving transaction")
        return build_response(500, {"Message": "Error saving transaction"})

def modify_transaction(trans_id, username, update_key, update_value):
    try:
        response = table.update_item(
            Key={
                "transId": trans_id,
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
        logger.exception("Error updating transaction")
        return build_response(500, {"Message": "Error updating transaction"})

def delete_transaction(trans_id, username):
    try:
        response = table.delete_item(
            Key={
                "transId": trans_id,
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
            return build_response(404, {"Message": f"transId: {trans_id}, username: {username} not found"})
    except Exception as e:
        logger.exception("Error deleting transaction")
        return build_response(500, {"Message": "Error deleting transaction"})
                

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