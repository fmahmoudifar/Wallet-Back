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

ALLOWED_FIELDS = {"currency", "theme", "incomeCategories", "expenseCategories", "dashboardColors"}


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

            if not user_id:
                response = build_response(400, {"Message": "Missing required fields for updating settings"})
            else:
                response = modify_setting(user_id, request_body)

        else:
            response = build_response(404, {"Message": "Path not found"})
    except Exception as e:
        logger.exception("Error processing request")
        return build_response(500, {"Message": f"Internal server error: {str(e)}"})
    return response


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


def modify_setting(user_id, fields):
    try:
        updates = {k: v for k, v in fields.items() if k in ALLOWED_FIELDS}
        if not updates:
            return build_response(400, {"Message": "No valid fields to update"})

        set_clauses = []
        expr_names = {}
        expr_values = {}

        for i, (field, value) in enumerate(updates.items()):
            name_key = f"#f{i}"
            value_key = f":v{i}"
            set_clauses.append(f"{name_key} = {value_key}")
            expr_names[name_key] = field
            expr_values[value_key] = value

        response = table.update_item(
            Key={"userId": user_id},
            UpdateExpression="SET " + ", ".join(set_clauses),
            ExpressionAttributeNames=expr_names,
            ExpressionAttributeValues=expr_values,
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
