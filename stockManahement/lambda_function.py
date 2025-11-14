import boto3
import json
import logging
from custom_encoder import CustomEncoder
from decimal import Decimal

logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodbTableName = "Stocks"
dynamodb = boto3.resource("dynamodb")
table = dynamodb.Table(dynamodbTableName)

GET_METHOD = "GET"
POST_METHOD = "POST"
PATCH_METHOD = "PATCH"
DELETE_METHOD = "DELETE"
HEALTH_PATH = "/healthC"
STOCK_PATH = "/stock"
STOCKS_PATH = "/stocks"

def lambda_handler(event, context):
    logger.info(f"Received event: {event}")    
    http_method = event["httpMethod"]
    path = event["path"]
    
    try:
        if http_method == GET_METHOD and path == HEALTH_PATH:
            response = build_response(200, {"status": "Healthy"})
            
        elif http_method == GET_METHOD and path == STOCK_PATH:
            query_params = event.get("queryStringParameters", {})
            stock_id = query_params.get("stockId")
            user_id = query_params.get("userId")

            if not stock_id or not user_id:
                response = build_response(400, {"Message": "stockId and userId are required"})
            else:
                response = get_stock(stock_id, user_id)
            
        elif http_method == GET_METHOD and path == STOCKS_PATH:
            response = get_stocks()
            
        elif http_method == POST_METHOD and path == STOCK_PATH:
            response = save_stock(json.loads(event["body"]))
   
        elif http_method == PATCH_METHOD and path == STOCK_PATH:
            request_body = json.loads(event["body"])
            stock_id = request_body.get("stockId")
            user_id = request_body.get("userId")
            stockName = request_body.get("stockName")
            tdate = request_body.get("tdate")
            from_wallet = request_body.get("fromWallet")
            to_wallet = request_body.get("toWallet")
            side = request_body.get("side")
            quantity = request_body.get("quantity")
            price = request_body.get("price")
            currency = request_body.get("currency")
            fee = request_body.get("fee")
            note = request_body.get("note")

            if not stock_id or not user_id:
                response = build_response(400, {"Message": "Missing required fields for updating stock"})
            else:
                response = modify_stock(stock_id, user_id, stockName, tdate, from_wallet, to_wallet, side, quantity, price, currency, fee, note)
        
        elif http_method == DELETE_METHOD and path == STOCK_PATH:
            request_body = json.loads(event["body"])
            stock_id = request_body.get("stockId")
            user_id = request_body.get("userId")
            
            if not stock_id or not user_id:
                response = build_response(400, {"Message": "stockId and userId are required"})
            else:
                response = delete_stock(stock_id, user_id)
        
        else:
            response = build_response(404, {"Message": "Path not found"})
    except Exception as e:
        logger.exception("Error processing request")
        return build_response(500, {"Message": f"Internal server error: {str(e)}"})
    return response

def get_stock(stock_id, user_id):
    try:
        logger.info(f"Fetching stock with Key: {{'stockId': {stock_id}, 'userId': '{user_id}'}}")

        response = table.get_item(
            Key={
                "stockId": stock_id,
                "userId": user_id
            }
        )

        if "Item" in response:
            return build_response(200, response["Item"])
        else:
            return build_response(404, {"Message": f"stock not found for stockId: {stock_id}, userId: {user_id}"})
    except Exception as e:
        logger.exception("Error retrieving stock")
        return build_response(500, {"Message": "Error retrieving stock"})

def get_stocks():
    try:
        response = table.scan()
        result = response["Items"]

        while "LastEvaluatedKey" in response:
            response = table.scan(ExclusiveStartKey=response["LastEvaluatedKey"])
            result.extend(response["Items"])

        return build_response(200, {"stocks": result})
    except Exception as e:
        logger.exception("Error retrieving stocks")
        return build_response(500, {"Message": "Error retrieving stocks"})

def save_stock(request_body):
    try:
        table.put_item(Item=request_body)
        return build_response(200, {
            "Operation": "SAVE",
            "Message": "SUCCESS",
            "Item": request_body
        })
    except Exception as e:
        logger.exception("Error saving stock")
        return build_response(500, {"Message": "Error saving stock"})


def modify_stock(stock_id, user_id, stockName, tdate, from_wallet, to_wallet, side, quantity, price, currency, fee, note):
    try:    
        update_expression = """SET stockName = :stockName, tdate = :tdate, fromWallet = :fromWallet,
          toWallet = :toWallet, quantity = :quantity, price = :price, currency = :currency, fee = :fee, note = :note"""
        expression_attribute_values = {
            ":stockName": stockName,
            ":tdate": tdate,
            ":fromWallet": from_wallet,
            ":toWallet": to_wallet,
            ":side": side,
            ":quantity": quantity,
            ":price": price,
            ":currency": currency,
            ":fee": fee,
            ":note": note
        }
        
        response = table.update_item(
            Key={
                "stockId": stock_id,
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
        logger.exception("Error updating stock")
        return build_response(500, {"Message": "Error updating stock"})


def delete_stock(stock_id, user_id):
    try:
        response = table.delete_item(
            Key={
                "stockId": stock_id,
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
            return build_response(404, {"Message": f"stockId: {stock_id}, userId: {user_id} not found"})
    except Exception as e:
        logger.exception("Error deleting stock")
        return build_response(500, {"Message": "Error deleting stock"})


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

