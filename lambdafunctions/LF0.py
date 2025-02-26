import json
import boto3, datetime

# Initialize Lex client
lex_client = boto3.client('lexv2-runtime', region_name='us-east-1')

# Replace with your actual bot ID and alias ID
BOT_ID = "THH2GHPMGX"
BOT_ALIAS_ID = "TSTALIASID"
LOCALE_ID = "en_US"  # Change if using a different language

def lambda_handler(event, context):
    try:
        # Debug: Print the full event to check structure
        print("Received event:", json.dumps(event))

        # Extract and parse JSON body from API Gateway
        body = json.loads(event["body"]) if "body" in event else event

        # Validate message format
        messages = body.get("messages", [])
        if not messages or "unstructured" not in messages[0] or "text" not in messages[0]["unstructured"]:
            return {
                "statusCode": 400,
                "body": json.dumps({"error": "Missing or invalid message format"})
            }

        # Extract the actual user message
        user_message = messages[0]["unstructured"]["text"].strip()
        user_id = messages[0]["unstructured"].get("id", "default-user")

        # Call Lex bot
        lex_response = lex_client.recognize_text(
            botId=BOT_ID,
            botAliasId=BOT_ALIAS_ID,
            localeId=LOCALE_ID,
            sessionId=user_id,
            text=user_message
        )

        print("Lex response:", lex_response)

        # Extract Lex response message
        lex_response_message = (
            lex_response["messages"][0]["content"] if lex_response.get("messages") else "Sorry, I didn't understand that."
        )

        # Construct response in the required format
        formatted_response = {
            "messages": [
                {
                    "type": "unstructured",
                    "unstructured": {
                        "id": user_id,
                        "text": lex_response_message,
                        "timestamp": datetime.datetime.utcnow().isoformat() + "Z"
                    }
                }
            ]
        }

        return {
            "statusCode": 200,
            "body": json.dumps(formatted_response),
            "headers": {
                "Content-Type": "application/json",
                "Access-Control-Allow-Origin": "*"
            }
        }

    except Exception as e:
        print(f"Error: {str(e)}")
        return {
            "statusCode": 500,
            "body": json.dumps({"error": "Internal server error"})
        }