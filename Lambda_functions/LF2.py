import boto3
import json
import urllib3
import base64
import os
from boto3.dynamodb.conditions import Attr

# AWS Services Clients
sqs = boto3.client("sqs")
dynamodb = boto3.resource("dynamodb", region_name="us-east-1")
ses = boto3.client("ses")

# Constants
SQS_URL = "https://sqs.us-east-1.amazonaws.com/861276083927/DiningSuggestionsQueue"
DYNAMO_TABLE_NAME = "yelp-restaurants"
DYNAMO_TABLE_NAME_USER_SEARCHES = "UserSearchState"
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

    print("SQS Response:", response)  # Debugging Line

    if "Messages" not in response:
        return None

    message = response["Messages"][0]
    receipt_handle = message["ReceiptHandle"]

    try:
        body = json.loads(message["Body"])
    except json.JSONDecodeError as e:
        print("Error decoding SQS message body:", message["Body"], e)
        return None

    return body, receipt_handle


def fetch_restaurants_from_es(cuisine):
    """ Queries OpenSearch for 3 random restaurant recommendations """
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
        "size": 3  # Fetch 3 restaurants instead of 1
    }

    response = http.request("GET", es_url, body=json.dumps(query), headers=headers)

    try:
        result = json.loads(response.data.decode("utf-8"))
    except json.JSONDecodeError as e:
        print("Error parsing OpenSearch response:", e)
        return None

    if result["hits"]["hits"]:
        return [hit["_source"]["RestaurantID"] for hit in result["hits"]["hits"]]
    return None


def fetch_restaurant_from_dynamo(business_id):
    """ Fetch full restaurant details from DynamoDB """
    table = dynamodb.Table(DYNAMO_TABLE_NAME)

    response = table.get_item(Key={"BusinessID": business_id})
    print("DynamoDB Response:", response)  # Debugging Line

    if "Item" not in response:
        print(f"No item found in DynamoDB for BusinessID: {business_id}")
        return None

    return response.get("Item", None)


def update_user_state(session_id, restaurant_ids):
    # Update the UserSearchState DynamoDB table with the restaurant IDs for the given session
    table = dynamodb.Table('UserSearchState')

    # Update the item with the new restaurant ids
    table.update_item(
        Key={'UserId': session_id},
        UpdateExpression="SET RestaurantIDs = :ids",
        ExpressionAttributeValues={
            ':ids': restaurant_ids
        }
    )


def check_dynamo(cuisine, session_id):
    """ Fetch full restaurant details from DynamoDB new table"""
    table = dynamodb.Table(DYNAMO_TABLE_NAME_USER_SEARCHES)

    response = table.get_item(Key={"UserId": session_id})
    print("DynamoDB Response:", response)  # Debugging Line

    if "Item" not in response:
        print(f"No item found in UserStateSearch DynamoDB")
        return None

    return response.get("Item", None)


def send_email(to_email, restaurants, cuisine):
    """ Sends an email with the restaurant recommendations """
    subject = f"Your {cuisine} Restaurant Recommendations"

    body_text = f"""Hello,

    We found some great {cuisine} restaurants for you!

    """

    for i, restaurant in enumerate(restaurants, 1):
        body_text += f"""
    Recommendation {i}:
    Name: {restaurant["Name"]}
    Address: {restaurant["Address"]}
    Rating: {restaurant["Rating"]}
    Reviews: {restaurant["NumReviews"]}
    Zip Code: {restaurant["ZipCode"]}

    """

    body_text += """
    Enjoy your meal!
    """

    try:
        response = ses.send_email(
            Source=SENDER_EMAIL,
            Destination={"ToAddresses": [to_email]},
            Message={
                "Subject": {"Data": subject},
                "Body": {"Text": {"Data": body_text}}
            }
        )
        print("SES Email Response:", response)  # Debugging Line
    except Exception as e:
        print("Error sending email:", e)
        return None

    return response


def lambda_handler(event, context):
    """ Main Lambda function """
    try:
        # Fetch SQS message
        sqs_message, receipt_handle = fetch_sqs_message()
        if not sqs_message:
            print("No messages in the queue")
            return

        print("SQS Message:", sqs_message)  # Debugging Line
        session_id = sqs_message["SessionID"]
        print("Session ID:", session_id)  # Debugging Line

        cuisine = sqs_message["Cuisine"]
        email = sqs_message["Email"]

        # Get 3 restaurant recommendations

        if sqs_message["State"] == "old":
            old_restaurants = check_dynamo(cuisine, session_id)
            print("Old Restaurants:", old_restaurants)  # Debugging Line
            business_ids = old_restaurants["RestaurantIDs"]

        else:
            business_ids = fetch_restaurants_from_es(cuisine)
            if not business_ids:
                print("No restaurants found for cuisine:", cuisine)
                return

        # Get full details from DynamoDB for each restaurant
        restaurants = []
        for business_id in business_ids:
            restaurant = fetch_restaurant_from_dynamo(business_id)
            if restaurant:
                restaurants.append(restaurant)

        update_user_state(session_id, business_ids)

        if not restaurants:
            print("No details found in DynamoDB")
            return

        # Send Email with all 3 recommendations
        send_email(email, restaurants, cuisine)

        # Delete processed SQS message
        sqs.delete_message(QueueUrl=SQS_URL, ReceiptHandle=receipt_handle)

        return {"status": "Email sent successfully"}
    except Exception as e:
        print(f"Lambda function error: {str(e)}")
        return {"status": "Internal Server Error"}