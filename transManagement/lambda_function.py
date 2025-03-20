import boto3
import json
import logging
from custom_encoder import CustomEncoder
from decimal import Decimal

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
TRANSACTION_PATH = "/transaction"
TRANSACTIONS_PATH = "/transactions"

def lambda_handler(event, context):
    logger.info(f"Received event: {event}")    
    http_method = event["httpMethod"]
    path = event["path"]
    
    try:
        if http_method == GET_METHOD and path == HEALTH_PATH:
            response = build_response(200, {"status": "Healthy"})
            
        elif http_method == GET_METHOD and path == TRANSACTION_PATH:
            query_params = event.get("queryStringParameters", {})
            trans_id = query_params.get("transId")
            user_id = query_params.get("userId")

            if not trans_id or not user_id:
                response = build_response(400, {"Message": "transId and userId are required"})
            else:
                response = get_transaction(trans_id, user_id)
            
        elif http_method == GET_METHOD and path == TRANSACTIONS_PATH:
            response = get_transactions()
            
        elif http_method == POST_METHOD and path == TRANSACTION_PATH:
            response = save_transaction(json.loads(event["body"]))
   
        elif http_method == PATCH_METHOD and path == TRANSACTION_PATH:
            request_body = json.loads(event["body"])
            trans_id = request_body.get("transId")
            user_id = request_body.get("userId")
            mtype = request_body.get("mtype")
            trans_type = request_body.get("transType")
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

            if not trans_id or not user_id:
                response = build_response(400, {"Message": "Missing required fields for updating transaction"})
            else:
                response = modify_transaction(trans_id, user_id, mtype, trans_type, main_cat, sub_cat, tdate, from_wallet, to_wallet, amount, price, currency, fee, note)
        
        elif http_method == DELETE_METHOD and path == TRANSACTION_PATH:
            request_body = json.loads(event["body"])
            trans_id = request_body.get("transId")
            user_id = request_body.get("userId")
            
            if not trans_id or not user_id:
                response = build_response(400, {"Message": "transId and userId are required"})
            else:
                response = delete_transaction(trans_id, user_id)
        
        else:
            response = build_response(404, {"Message": "Path not found"})
    except Exception as e:
        logger.exception("Error processing request")
        return build_response(500, {"Message": f"Internal server error: {str(e)}"})
    return response

def get_transaction(trans_id, user_id):
    try:
        logger.info(f"Fetching transaction with Key: {{'transId': {trans_id}, 'userId': '{user_id}'}}")

        response = table.get_item(
            Key={
                "transId": trans_id,
                "userId": user_id
            }
        )

        if "Item" in response:
            return build_response(200, response["Item"])
        else:
            return build_response(404, {"Message": f"Transaction not found for transId: {trans_id}, userId: {user_id}"})
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

# def save_transaction(request_body):
#     try:
#         # Convert amount, fee, and price to float
#         request_body["amount"] = float(request_body["amount"])
#         request_body["fee"] = float(request_body["fee"])
#         request_body["price"] = float(request_body["price"])
        
#         table.put_item(Item=request_body)
#         return build_response(200, {
#             "Operation": "SAVE",
#             "Message": "SUCCESS",
#             "Item": request_body
#         })
#     except Exception as e:
#         logger.exception("Error saving transaction")
#         return build_response(500, {"Message": "Error saving transaction"})


def modify_transaction(trans_id, user_id, mtype, trans_type, main_cat, sub_cat, tdate, from_wallet, to_wallet, amount, price, currency, fee, note):
    try:    
        update_expression = """SET mtype = :mtype, transType = :transType, mainCat = :mainCat, subCat = :subCat, tdate = :tdate, fromWallet = :fromWallet,
          toWallet = :toWallet, amount = :amount, price = :price, currency = :currency, fee = :fee, note = :note"""
        expression_attribute_values = {
            ":mtype": mtype,
            ":transType": trans_type,
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
                "transId": trans_id,
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
        logger.exception("Error updating transaction")
        return build_response(500, {"Message": "Error updating transaction"})

# def modify_transaction(trans_id, user_id, mtype, trans_type, main_cat, sub_cat, tdate, from_wallet, to_wallet, amount, price, currency, fee, note):
#     try:
#         update_expression = """SET mtype = :mtype, transType = :transType, mainCat = :mainCat, subCat = :subCat, tdate = :tdate, 
#             fromWallet = :fromWallet, toWallet = :toWallet, amount = :amount, price = :price, currency = :currency, fee = :fee, note = :note"""
        
#         expression_attribute_values = {
#             ":mtype": mtype,
#             ":transType": trans_type,
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
#             Key={"transId": trans_id, "userId": user_id},
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
#         logger.exception("Error updating transaction")
#         return build_response(500, {"Message": "Error updating transaction"})

def delete_transaction(trans_id, user_id):
    try:
        response = table.delete_item(
            Key={
                "transId": trans_id,
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
            return build_response(404, {"Message": f"transId: {trans_id}, userId: {user_id} not found"})
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
