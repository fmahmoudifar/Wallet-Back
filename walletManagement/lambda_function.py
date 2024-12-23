import boto3
import json
import logging
from custom_encoder import CustomEncoder
logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodbTableName = "Wallets"
dynamodb = boto3.resource("dynamodb")
table = dynamodb.Table(dynamodbTableName)

getMethod = "GET"
postMethod = "POST"
patchMethod = "PATCH"
deleteMethod = "DELETE"
healthPath = "/health"
walletPath = "/wallet"
walletsPath = "/wallets"


def lambda_handler(event, context):
    logger.info(event)
    httpMethod = event["httpMethod"]
    path = event["path"]
    if httpMethod == getMethod and path == healthPath:
        response = buildResponse(200)
    elif httpMethod == getMethod and path == walletPath:
        queryParams = event["queryStringParameters"]
        walletName = queryParams["walletName"]
        username = queryParams["username"]
        response = getWallet(walletName, username)
    elif httpMethod == getMethod and path == walletsPath:
        response = getWallets()
    elif httpMethod == postMethod and path == walletPath:
        response = saveWallet(json.loads(event["body"]))
    elif httpMethod == patchMethod and path == walletPath:
        requestBody = json.loads(event["body"])
        response = modifyWallet(requestBody["walletName"], requestBody["updateKey"], requestBody["updateValue"])
    elif httpMethod == deleteMethod and path == walletPath:
        requestBody = json.loads(event["body"])
        response = deleteWallet(requestBody["walletName"])
    else:
        response = buildResponse(404, "Not Found")
    return response

def getWallet(walletName, username):
    try:
        response = table.get_item(
            Key={
                "walletName": walletName,
                "username": username
            }
        )
        if "Item" in response:
            return buildResponse(200, response["Item"])
        else:
            return buildResponse(404, {"Message": f"walletName: {walletName}, username: {username} not found"})
    except Exception as e:
        logger.exception("Could not get the wallet")
        return buildResponse(500, {"Message": "Error retrieving wallet"})

def getWallets():
    try:
        response = table.scan()
        result = response["Items"]

        while "LastEvaluateKey" in response:
            response = table.scan(ExclusiveStartKey=response["LastEvaluatedKey"])
            result.extend(response["Items"])

        body = {
            "products": response
        }
        return buildResponse(200, body)
    except:
        logger.exception("Could not get the wallets")

def saveWallet(requestBody):
    try:
        table.put_item(Item=requestBody)
        body = {
            "Operation": "SAVE",
            "Message": "SUCCESS",
            "Item": requestBody
        }
        return buildResponse(200, body)
    except:
        logger.exception("Could not save the wallet")

def modifyWallet(walletName, updateKey, updateValue):
    try:
        response = table.update_item(
            Key={
                "walletName": walletName
            },

            UpdateExpression="set {0}s = :value".format(updateKey),
            ExpressionAttributeValues={
                ":value": updateValue
            },
            ReturnValues="UPDATED_NEW"
        )
        body = {
            "Operation": "UPDATE",
            "Message": "SUCCESS",
            "UpdatedAttributes": response
        }
        return buildResponse(200, body)
    except:
        logger.exception("Could not update the wallet")

def deleteWallet(walletName):
    try:
        response = table.delete_item(
            Key={
                "walletName": walletName
            },
            ReturnValues="ALL_OLD"
        )
        body = {
            "Operation": "DELETE",
            "Message": "SUCCESS",
            "deltedItem": response
        }
        return buildResponse(200, body)
    except:
        logger.exception("Could not delete the wallet")


def buildResponse(statusCode, body=None):
    response = {
        "statusCode": statusCode,
        "headers": {
            "Content-Type": "application/json",
            "Access-Control-Allow-Origin": "*"
        }
    }

    if body is not None:
        response["body"] = json.dumps(body, cls=CustomEncoder)
    return response