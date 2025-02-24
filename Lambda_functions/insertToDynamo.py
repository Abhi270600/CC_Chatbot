import json
import boto3

# Initialize AWS services
s3_client = boto3.client("s3")
dynamodb = boto3.resource("dynamodb", region_name="us-east-1")

# Table name
TABLE_NAME = "yelp-restaurants"


def lambda_handler(event, context):
    """Lambda function to read JSON from S3 and insert into DynamoDB."""

    BUCKET_NAME = "yelp-restaurants-bucket"
    FILE_KEY = "restaurants.json"

    # Read file from S3
    response = s3_client.get_object(Bucket=BUCKET_NAME, Key=FILE_KEY)
    restaurants = json.loads(response["Body"].read().decode("utf-8"))

    print(f"Successfully read {len(restaurants)} restaurants from S3")

    # Get DynamoDB table
    table = dynamodb.Table(TABLE_NAME)

    # Insert each restaurant into DynamoDB
    for restaurant in restaurants:
        item = {
            "BusinessID": restaurant["BusinessID"],
            "Name": restaurant["Name"],
            "Address": restaurant["Address"],
            "Coordinates": {
                "lat": str(restaurant["Coordinates"]["lat"]),
                "lon": str(restaurant["Coordinates"]["lon"])
            },
            "NumReviews": restaurant["NumReviews"],
            "Rating": str(restaurant["Rating"]),
            "ZipCode": restaurant["ZipCode"],
            "Cuisine": restaurant["Cuisine"],
            "InsertedAtTimestamp": restaurant["InsertedAtTimestamp"]
        }

        table.put_item(Item=item)

    return {
        "statusCode": 200,
        "body": json.dumps(f"Successfully inserted {len(restaurants)} restaurants into DynamoDB!")
    }

