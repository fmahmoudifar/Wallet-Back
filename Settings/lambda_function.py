import boto3
import json
import logging
from custom_encoder import CustomEncoder
from decimal import Decimal
from boto3.dynamodb.conditions import Attr

logger = logging.getLogger()
logger.setLevel(logging.INFO)
 
dynamodbTableName = "Settings"
dynamodb = boto3.resource("dynamodb")
table = dynamodb.Table(dynamodbTableName)

GET_METHOD = "GET"
POST_METHOD = "POST"
PATCH_METHOD = "PATCH"
DELETE_METHOD = "DELETE"
HEALTH_PATH = "/healthC"
SET_PATH = "/settings"

def lambda_handler(event, context):
    logger.info(f"Received event: {event}")    
    http_method = event["httpMethod"]
    path = event["path"]
    
    try:
        if http_method == GET_METHOD and path == HEALTH_PATH:
            response = build_response(200, {"status": "Healthy"})
            
        elif http_method == GET_METHOD and path == SET_PATH:
            query_params = event.get("queryStringParameters", {})
            user_id = query_params.get("userId")

            if not user_id:
                response = build_response(400, {"Message": "userId are required"})
            else:
                response = get_settings(user_id)
  
        elif http_method == PATCH_METHOD and path == SET_PATH:
            request_body = json.loads(event["body"])
            user_id = request_body.get("userId")
            currency = request_body.get("currency")
            theme = request_body.get("theme")

            if not user_id:
                response = build_response(400, {"Message": "Missing required fields for updating settings"})
            else:
                response = modify_setting(user_id, currency, theme)
        
        else:
            response = build_response(404, {"Message": "Path not found"})
    except Exception as e:
        logger.exception("Error processing request")
        return build_response(500, {"Message": f"Internal server error: {str(e)}"})
    return response

# def get_settings(user_id):
#     try:
#         logger.info(f"Fetching setting with Key: {'userId': '{user_id}'}")

#         response = table.get_item(
#             Key={
#                 "userId": user_id
#             }
#         )

#         if "Item" in response:
#             return build_response(200, response["Item"])
#         else:
#             return build_response(404, {"Message": f"setting not found for userId: {user_id}"})
#     except Exception as e:
#         logger.exception("Error retrieving settings")
#         return build_response(500, {"Message": "Error retrieving settings"})


def get_settings(user_id):
    try:
        response = table.scan(
            FilterExpression=Attr('userId').eq(user_id)
        )
        result = response["Items"]

        while "LastEvaluatedKey" in response:
            response = table.scan(
                ExclusiveStartKey=response["LastEvaluatedKey"],
                FilterExpression=Attr('userId').eq(user_id)
            )
            result.extend(response["Items"])

        return build_response(200, {"settings": result})
    except Exception as e:
        logger.exception("Error retrieving settings")
        return build_response(500, {"Message": "Error retrieving settings"})


# def save_setting(request_body):
#     try:
#         table.put_item(Item=request_body)
#         return build_response(200, {
#             "Operation": "SAVE",
#             "Message": "SUCCESS",
#             "Item": request_body
#         })
#     except Exception as e:
#         logger.exception("Error saving setting")
#         return build_response(500, {"Message": "Error saving setting"})


def modify_setting(user_id, currency, theme):
    try:    
        update_expression = """SET currency = :currency, theme = :theme"""
        expression_attribute_values = {
            ":currency": currency,
            ":theme": theme
        }
        
        response = table.update_item(
            Key={
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
        logger.exception("Error updating setting")
        return build_response(500, {"Message": "Error updating setting"})


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

