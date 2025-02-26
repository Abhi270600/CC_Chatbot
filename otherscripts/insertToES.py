import json
import boto3
import urllib3
import os
import random
import base64

# OpenSearch Configuration
OPENSEARCH_ENDPOINT = "https://search-restaurants-tbwuejrkt6sp4kuw5oys5dvv5e.aos.us-east-1.on.aws"
ES_USERNAME = "aa12037"  # Change if using IAM Role
ES_PASSWORD = "Abhikeer@123"
INDEX_NAME = "restaurants"

# DynamoDB Configuration
DYNAMODB_TABLE = "yelp-restaurants"
dynamodb = boto3.resource("dynamodb", region_name="us-east-1")
table = dynamodb.Table(DYNAMODB_TABLE)

# List of cuisines to fetch
CUISINES = ["Indian", "Chinese", "Mexican", "Italian", "Thai"]


def fetch_restaurants(cuisine):
    """Fetch restaurants of a specific cuisine from DynamoDB."""
    response = table.scan(
        FilterExpression="Cuisine = :c",
        ExpressionAttributeValues={":c": cuisine}
    )

    # Shuffle to get random records (if more than needed)
    items = response.get("Items", [])
    random.shuffle(items)
    return items  # Get the first `num_records` items


def push_to_opensearch(restaurants):
    """Push restaurant data to OpenSearch."""
    auth_header = base64.b64encode(f"{ES_USERNAME}:{ES_PASSWORD}".encode()).decode()

    headers = {"Content-Type": "application/json", "Authorization": f"Basic {auth_header}"}

    for restaurant in restaurants:
        data = {
            "RestaurantID": restaurant["BusinessID"],
            "Cuisine": restaurant["Cuisine"]
        }

        encoded_data = json.dumps(data).encode("utf-8")

        es_url = f"{OPENSEARCH_ENDPOINT}/{INDEX_NAME}/_doc/{restaurant['BusinessID']}"

        http = urllib3.PoolManager()
        response = http.request("PUT", es_url, headers=headers, body=encoded_data)

        if response.status not in [200, 201]:
            print(f"Failed to insert {restaurant['BusinessID']}: {response.text}")


def lambda_handler(event, context):
    """AWS Lambda function entry point."""
    all_restaurants = []

    # Fetch restaurants for each cuisine
    for cuisine in CUISINES:
        restaurants = fetch_restaurants(cuisine)
        all_restaurants.extend(restaurants)

    # Push the collected restaurants to OpenSearch
    push_to_opensearch(all_restaurants)

    return {
        "statusCode": 200,
        "body": json.dumps(f"Pushed {len(all_restaurants)} restaurants to OpenSearch")
    }
