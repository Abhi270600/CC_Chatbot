import boto3
import json
import urllib3
import base64
import os

# AWS Services Clients
sqs = boto3.client("sqs")
dynamodb = boto3.resource("dynamodb", region_name="us-east-1")
ses = boto3.client("ses")

# Constants
SQS_URL = "https://sqs.us-east-1.amazonaws.com/861276083927/DiningSuggestionsQueue"
DYNAMO_TABLE_NAME = "yelp-restaurants"
OPENSEARCH_ENDPOINT = "https://search-restaurants-tbwuejrkt6sp4kuw5oys5dvv5e.aos.us-east-1.on.aws"
ES_USERNAME = "aa12037"
ES_PASSWORD = "Abhikeer@123"
SENDER_EMAIL = "abhi27.personal@gmail.com"

# Initialize HTTP Manager
http = urllib3.PoolManager()

# Base64 Encode Credentials for OpenSearch Authentication
auth_header = base64.b64encode(f"{ES_USERNAME}:{ES_PASSWORD}".encode()).decode()


def fetch_sqs_message():
    """ Pulls a message from SQS """
    response = sqs.receive_message(
        QueueUrl=SQS_URL,
        MaxNumberOfMessages=1,
        WaitTimeSeconds=10
    )

    if "Messages" not in response:
        return None

    message = response["Messages"][0]
    receipt_handle = message["ReceiptHandle"]
    body = json.loads(message["Body"])

    return body, receipt_handle


def fetch_restaurant_from_es(cuisine):
    """ Queries OpenSearch for a random restaurant recommendation """
    es_url = f"{OPENSEARCH_ENDPOINT}/restaurants/_search"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Basic {auth_header}"
    }

    query = {
        "query": {
            "function_score": {
                "query": {"match": {"Cuisine": cuisine}},
                "random_score": {}
            }
        },
        "size": 1
    }

    response = http.request("GET", es_url, body=json.dumps(query), headers=headers)
    result = json.loads(response.data.decode("utf-8"))

    if result["hits"]["hits"]:
        return result["hits"]["hits"][0]["_source"]["RestaurantID"]
    return None


def fetch_restaurant_from_dynamo(business_id):
    """ Fetch full restaurant details from DynamoDB """
    table = dynamodb.Table(DYNAMO_TABLE_NAME)

    response = table.get_item(Key={"BusinessID": business_id})

    return response.get("Item", None)


def send_email(to_email, restaurant):
    """ Sends an email with the restaurant recommendation """
    subject = "Your Restaurant Recommendation"

    body_text = f"""Hello,

    We found a great restaurant for you!

    Name: {restaurant["Name"]}
    Address: {restaurant["Address"]}
    Rating: {restaurant["Rating"]}
    Reviews: {restaurant["NumReviews"]}
    Zip Code: {restaurant["ZipCode"]}

    Enjoy your meal!
    """

    response = ses.send_email(
        Source=SENDER_EMAIL,
        Destination={"ToAddresses": [to_email]},
        Message={
            "Subject": {"Data": subject},
            "Body": {"Text": {"Data": body_text}}
        }
    )

    return response


def lambda_handler(event, context):
    """ Main Lambda function """
    # Fetch SQS message
    sqs_message, receipt_handle = fetch_sqs_message()
    if not sqs_message:
        print("No messages in the queue")
        return

    cuisine = sqs_message["Cuisine"]
    email = sqs_message["Email"]

    # Get restaurant recommendation
    business_id = fetch_restaurant_from_es(cuisine)
    if not business_id:
        print("No restaurant found for cuisine:", cuisine)
        return

    # Get full details from DynamoDB
    restaurant = fetch_restaurant_from_dynamo(business_id)
    if not restaurant:
        print("No details found in DynamoDB for BusinessID:", business_id)
        return

    # Send Email
    send_email(email, restaurant)

    # Delete processed SQS message
    sqs.delete_message(QueueUrl=SQS_URL, ReceiptHandle=receipt_handle)

    return {"status": "Email sent successfully"}
