import boto3
import json
import logging
from custom_encoder import CustomEncoder
logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodbTableName = "assets"
dynamodb = boto3.resource("dynamodb")
table = dynamodb.Table(dynamodbTableName)

getMethod = "GET"
postMethod = "POST"
patchMethod = "PATCH"
deleteMethod = "DELETE"
healthPath = "/health"
productPath = "/asset"
productsPath = "/assets"


def lambda_handler(event, context):
    logger.info(event)
    httpMethod = event["httpMethod"]
    path = event["path"]
    if httpMethod == getMethod and path == healthPath:
        response = buildResponse(200)
    elif httpMethod == getMethod and path == productPath:
        response = getAsset(event["queryStringParameters"]["assetId"])
    elif httpMethod == getMethod and path == productsPath:
        response = getAssets()
    elif httpMethod == postMethod and path == productPath:
        response = saveAsset(json.loads(event["body"]))
    elif httpMethod == patchMethod and path == productPath:
        requestBody = json.loads(event["body"])
        response = modifyAsset(requestBody["assetId"], requestBody["updateKey"], requestBody["updateValue"])
    elif httpMethod == deleteMethod and path == productPath:
        requestBody = json.loads(event["body"])
        response = deleteAsset(requestBody["assetId"])
    else:
        response = buildResponse(404, "Not Found")
    return response

def getAsset(assetId):
    try:
        response = table.get_item(
            Key={
                "assetId": assetId
            }
        )
        if "Item" in response:
            return buildResponse(200, response["Item"])
        else:
            return buildResponse(404, {"Message": "assetId: {0}s not found".format(assetId)})
    except:
        logger.exception("Could not get the asset")

def getAssets():
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
        logger.exception("Could not get the assets")

def saveAsset(requestBody):
    try:
        table.put_item(Item=requestBody)
        body = {
            "Operation": "SAVE",
            "Message": "SUCCESS",
            "Item": requestBody
        }
        return buildResponse(200, body)
    except:
        logger.exception("Could not save the asset")

def modifyAsset(assetId, updateKey, updateValue):
    try:
        response = table.update_item(
            Key={
                "assetId": assetId
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
        logger.exception("Could not update the asset")

def deleteAsset(assetId):
    try:
        response = table.delete_item(
            Key={
                "assetId": assetId
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
        logger.exception("Could not delete the asset")


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