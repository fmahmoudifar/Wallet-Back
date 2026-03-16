import boto3
import json
import logging
from custom_encoder import CustomEncoder
from decimal import Decimal

logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodbTableName = "Loans"
dynamodb = boto3.resource("dynamodb")
table = dynamodb.Table(dynamodbTableName)

GET_METHOD = "GET"
POST_METHOD = "POST"
PATCH_METHOD = "PATCH"
DELETE_METHOD = "DELETE"
HEALTH_PATH = "/healthC"
LOAN_PATH = "/loan"
LOANS_PATH = "/loans"

def lambda_handler(event, context):
    logger.info(f"Received event: {event}")    
    http_method = event["httpMethod"]
    path = event["path"]
    
    try:
        if http_method == GET_METHOD and path == HEALTH_PATH:
            response = build_response(200, {"status": "Healthy"})
            
        elif http_method == GET_METHOD and path == LOAN_PATH:
            query_params = event.get("queryStringParameters", {})
            loan_id = query_params.get("loanId")
            user_id = query_params.get("userId")

            if not loan_id or not user_id:
                response = build_response(400, {"Message": "loanId and userId are required"})
            else:
                response = get_loan(loan_id, user_id)
            
        elif http_method == GET_METHOD and path == LOANS_PATH:
            response = get_loans()
            
        elif http_method == POST_METHOD and path == LOAN_PATH:
            response = save_loan(json.loads(event["body"]))
   
        elif http_method == PATCH_METHOD and path == LOAN_PATH:
            request_body = json.loads(event["body"])
            loan_id = request_body.get("loanId")
            user_id = request_body.get("userId")
            loan_type = request_body.get("type")  
            counterparty = request_body.get("counterparty")
            tdate = request_body.get("tdate")
            ddate = request_body.get("ddate")
            position = request_body.get("position")
            from_wallet = request_body.get("fromWallet")
            to_wallet = request_body.get("toWallet")
            action = request_body.get("action")
            amount = request_body.get("amount")
            currency = request_body.get("currency")
            fee = request_body.get("fee")
            note = request_body.get("note")

            if not loan_id or not user_id:
                response = build_response(400, {"Message": "Missing required fields for updating loan"})
            else:
                response = modify_loan(loan_id, user_id, loan_type, counterparty, tdate, ddate, position, from_wallet, to_wallet, action, amount, currency, fee, note)
        
        elif http_method == DELETE_METHOD and path == LOAN_PATH:
            request_body = json.loads(event["body"])
            loan_id = request_body.get("loanId")
            user_id = request_body.get("userId")
            
            if not loan_id or not user_id:
                response = build_response(400, {"Message": "loanId and userId are required"})
            else:
                response = delete_loan(loan_id, user_id)
        
        else:
            response = build_response(404, {"Message": "Path not found"})
    except Exception as e:
        logger.exception("Error processing request")
        return build_response(500, {"Message": f"Internal server error: {str(e)}"})
    return response

def get_loan(loan_id, user_id):
    try:
        logger.info(f"Fetching loan with Key: {{'loanId': {loan_id}, 'userId': '{user_id}'}}")

        response = table.get_item(
            Key={
                "loanId": loan_id,
                "userId": user_id
            }
        )

        if "Item" in response:
            return build_response(200, response["Item"])
        else:
            return build_response(404, {"Message": f"loan not found for loanId: {loan_id}, userId: {user_id}"})
    except Exception as e:
        logger.exception("Error retrieving loan")
        return build_response(500, {"Message": "Error retrieving loan"})

def get_loans():
    try:
        response = table.scan()
        result = response["Items"]

        while "LastEvaluatedKey" in response:
            response = table.scan(ExclusiveStartKey=response["LastEvaluatedKey"])
            result.extend(response["Items"])

        return build_response(200, {"loans": result})
    except Exception as e:
        logger.exception("Error retrieving loans")
        return build_response(500, {"Message": "Error retrieving loans"})

def save_loan(request_body):
    try:
        table.put_item(Item=request_body)
        return build_response(200, {
            "Operation": "SAVE",
            "Message": "SUCCESS",
            "Item": request_body
        })
    except Exception as e:
        logger.exception("Error saving loan")
        return build_response(500, {"Message": "Error saving loan"})

def modify_loan(loan_id, user_id, loan_type, counterparty, tdate, ddate, position, from_wallet, to_wallet, action, amount, currency, fee, note):
    try:
        # Fields that can be cleared (set to empty) by the user
        clearable_fields = {"fromWallet", "toWallet", "ddate", "note"}

        all_fields = {
            "type": loan_type,
            "counterparty": counterparty,
            "tdate": tdate,
            "ddate": ddate,
            "position": position,
            "fromWallet": from_wallet,
            "toWallet": to_wallet,
            "action": action,
            "amount": amount,
            "currency": currency,
            "fee": fee,
            "note": note,
        }

        set_fields = {}
        remove_fields = []

        for key, value in all_fields.items():
            if isinstance(value, str) and value.strip() == "":
                if key in clearable_fields:
                    remove_fields.append(key)
                # else: skip non-clearable empty fields (keep old value)
            elif value is not None:
                set_fields[key] = value

        if not set_fields and not remove_fields:
            return build_response(400, {"Message": "No fields to update"})

        reserved_keywords = {"type", "action", "position"}
        expression_attribute_names = {}
        expression_attribute_values = {}
        expression_parts = []

        if set_fields:
            set_clauses = []
            for key, value in set_fields.items():
                name_key = f"#{key}" if key in reserved_keywords else key
                value_key = f":{key}"
                set_clauses.append(f"{name_key} = {value_key}")
                if name_key.startswith("#"):
                    expression_attribute_names[name_key] = key
                expression_attribute_values[value_key] = value
            expression_parts.append("SET " + ", ".join(set_clauses))

        if remove_fields:
            remove_clauses = []
            for key in remove_fields:
                name_key = f"#{key}" if key in reserved_keywords else key
                remove_clauses.append(name_key)
                if name_key.startswith("#"):
                    expression_attribute_names[name_key] = key
            expression_parts.append("REMOVE " + ", ".join(remove_clauses))

        update_expression = " ".join(expression_parts)

        update_kwargs = {
            "Key": {
                "loanId": loan_id,
                "userId": user_id
            },
            "UpdateExpression": update_expression,
            "ExpressionAttributeValues": expression_attribute_values,
            "ReturnValues": "UPDATED_NEW"
        }
        if expression_attribute_names:
            update_kwargs["ExpressionAttributeNames"] = expression_attribute_names
        # If only REMOVE (no SET), ExpressionAttributeValues will be empty — omit it
        if not expression_attribute_values:
            del update_kwargs["ExpressionAttributeValues"]

        response = table.update_item(**update_kwargs)
        return build_response(200, {
            "Operation": "UPDATE",
            "Message": "SUCCESS",
            "UpdatedAttributes": response["Attributes"]
        })
    except Exception as e:
        logger.exception("Error updating loan")
        return build_response(500, {"Message": "Error updating loan"})


def delete_loan(loan_id, user_id):
    try:
        response = table.delete_item(
            Key={
                "loanId": loan_id,
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
            return build_response(404, {"Message": f"loanId: {loan_id}, userId: {user_id} not found"})
    except Exception as e:
        logger.exception("Error deleting loan")
        return build_response(500, {"Message": "Error deleting loan"})


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
