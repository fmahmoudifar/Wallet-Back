import boto3
import json
import logging
from custom_encoder import CustomEncoder
from decimal import Decimal

logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodbTableName = "Cryptos"
dynamodb = boto3.resource("dynamodb")
table = dynamodb.Table(dynamodbTableName)

GET_METHOD = "GET"
POST_METHOD = "POST"
PATCH_METHOD = "PATCH"
DELETE_METHOD = "DELETE"
HEALTH_PATH = "/health"
CRYPTO_PATH = "/crypto"
CRYPTOS_PATH = "/cryptos"

def lambda_handler(event, context):
    logger.info(f"Received event: {event}")    
    http_method = event["httpMethod"]
    path = event["path"]
    
    try:
        if http_method == GET_METHOD and path == HEALTH_PATH:
            response = build_response(200, {"status": "Healthy"})
            
        elif http_method == GET_METHOD and path == CRYPTO_PATH:
            query_params = event.get("queryStringParameters", {})
            crypto_id = query_params.get("cryptoId")
            user_id = query_params.get("userId")

            if not crypto_id or not user_id:
                response = build_response(400, {"Message": "cryptoId and userId are required"})
            else:
                response = get_crypto(crypto_id, user_id)
            
        elif http_method == GET_METHOD and path == CRYPTOS_PATH:
            response = get_cryptos()
            
        elif http_method == POST_METHOD and path == CRYPTO_PATH:
            response = save_crypto(json.loads(event["body"]))
   
        elif http_method == PATCH_METHOD and path == CRYPTO_PATH:
            request_body = json.loads(event["body"])
            crypto_id = request_body.get("cryptoId")
            user_id = request_body.get("userId")
            mtype = request_body.get("mtype")
            crypto_type = request_body.get("cryptoType")
            main_cat = request_body.get("mainCat")
            sub_cat = request_body.get("subCat")
            tdate = request_body.get("tdate")
            from_wallet = request_body.get("fromWallet")
            to_wallet = request_body.get("toWallet")
            amount = request_body.get("amount")
            price = request_body.get("price")
            currency = request_body.get("currency")
            fee = request_body.get("fee")
            note = request_body.get("note")

            if not crypto_id or not user_id:
                response = build_response(400, {"Message": "Missing required fields for updating crypto"})
            else:
                response = modify_crypto(crypto_id, user_id, mtype, crypto_type, main_cat, sub_cat, tdate, from_wallet, to_wallet, amount, price, currency, fee, note)
        
        elif http_method == DELETE_METHOD and path == CRYPTO_PATH:
            request_body = json.loads(event["body"])
            crypto_id = request_body.get("cryptoId")
            user_id = request_body.get("userId")
            
            if not crypto_id or not user_id:
                response = build_response(400, {"Message": "cryptoId and userId are required"})
            else:
                response = delete_crypto(crypto_id, user_id)
        
        else:
            response = build_response(404, {"Message": "Path not found"})
    except Exception as e:
        logger.exception("Error processing request")
        return build_response(500, {"Message": f"Internal server error: {str(e)}"})
    return response

def get_crypto(crypto_id, user_id):
    try:
        logger.info(f"Fetching crypto with Key: {{'cryptoId': {crypto_id}, 'userId': '{user_id}'}}")

        response = table.get_item(
            Key={
                "cryptoId": crypto_id,
                "userId": user_id
            }
        )

        if "Item" in response:
            return build_response(200, response["Item"])
        else:
            return build_response(404, {"Message": f"crypto not found for cryptoId: {crypto_id}, userId: {user_id}"})
    except Exception as e:
        logger.exception("Error retrieving crypto")
        return build_response(500, {"Message": "Error retrieving crypto"})

def get_cryptos():
    try:
        response = table.scan()
        result = response["Items"]

        while "LastEvaluatedKey" in response:
            response = table.scan(ExclusiveStartKey=response["LastEvaluatedKey"])
            result.extend(response["Items"])

        return build_response(200, {"cryptos": result})
    except Exception as e:
        logger.exception("Error retrieving cryptos")
        return build_response(500, {"Message": "Error retrieving cryptos"})

# def save_crypto(request_body):
#     try:
#         table.put_item(Item=request_body)
#         return build_response(200, {
#             "Operation": "SAVE",
#             "Message": "SUCCESS",
#             "Item": request_body
#         })
#     except Exception as e:
#         logger.exception("Error saving crypto")
#         return build_response(500, {"Message": "Error saving crypto"})

def save_crypto(request_body):
    try:
        # Convert amount, fee, and price to float
        request_body["amount"] = float(request_body["amount"])
        request_body["fee"] = float(request_body["fee"])
        request_body["price"] = float(request_body["price"])
        
        table.put_item(Item=request_body)
        return build_response(200, {
            "Operation": "SAVE",
            "Message": "SUCCESS",
            "Item": request_body
        })
    except Exception as e:
        logger.exception("Error saving crypto")
        return build_response(500, {"Message": "Error saving crypto"})


def modify_crypto(crypto_id, user_id, mtype, crypto_type, main_cat, sub_cat, tdate, from_wallet, to_wallet, amount, price, currency, fee, note):
    try:    
        update_expression = """SET mtype = :mtype, cryptoType = :cryptoType, mainCat = :mainCat, subCat = :subCat, tdate = :tdate, fromWallet = :fromWallet,
          toWallet = :toWallet, amount = :amount, price = :price, currency = :currency, fee = :fee, note = :note"""
        expression_attribute_values = {
            ":mtype": mtype,
            ":cryptoType": crypto_type,
            ":mainCat": main_cat,
            ":subCat": sub_cat,
            ":tdate": tdate,
            ":fromWallet": from_wallet,
            ":toWallet": to_wallet,
            ":amount": amount,
            ":price": price,
            ":currency": currency,
            ":fee": fee,
            ":note": note
        }
        
        response = table.update_item(
            Key={
                "cryptoId": crypto_id,
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
        logger.exception("Error updating crypto")
        return build_response(500, {"Message": "Error updating crypto"})

# def modify_crypto(crypto_id, user_id, mtype, crypto_type, main_cat, sub_cat, tdate, from_wallet, to_wallet, amount, price, currency, fee, note):
#     try:
#         update_expression = """SET mtype = :mtype, cryptoType = :cryptoType, mainCat = :mainCat, subCat = :subCat, tdate = :tdate, 
#             fromWallet = :fromWallet, toWallet = :toWallet, amount = :amount, price = :price, currency = :currency, fee = :fee, note = :note"""
        
#         expression_attribute_values = {
#             ":mtype": mtype,
#             ":cryptoType": crypto_type,
#             ":mainCat": main_cat,
#             ":subCat": sub_cat,
#             ":tdate": tdate,
#             ":fromWallet": from_wallet,
#             ":toWallet": to_wallet,
#             ":amount": float(amount),
#             ":price": float(price),
#             ":currency": currency,
#             ":fee": float(fee),
#             ":note": note
#         }
        
#         response = table.update_item(
#             Key={"cryptoId": crypto_id, "userId": user_id},
#             UpdateExpression=update_expression,
#             ExpressionAttributeValues=expression_attribute_values,
#             ReturnValues="UPDATED_NEW"
#         )
#         return build_response(200, {
#             "Operation": "UPDATE",
#             "Message": "SUCCESS",
#             "UpdatedAttributes": response["Attributes"]
#         })
#     except Exception as e:
#         logger.exception("Error updating crypto")
#         return build_response(500, {"Message": "Error updating crypto"})

def delete_crypto(crypto_id, user_id):
    try:
        response = table.delete_item(
            Key={
                "cryptoId": crypto_id,
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
            return build_response(404, {"Message": f"cryptoId: {crypto_id}, userId: {user_id} not found"})
    except Exception as e:
        logger.exception("Error deleting crypto")
        return build_response(500, {"Message": "Error deleting crypto"})


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
