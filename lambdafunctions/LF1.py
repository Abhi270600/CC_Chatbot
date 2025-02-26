import json
import boto3
import re
from datetime import datetime
import dateutil.parser

# Initialize SQS and DynamoDB clients
sqs = boto3.client('sqs', region_name='us-east-1')
dynamodb = boto3.resource('dynamodb', region_name='us-east-1')

# Replace with your actual SQS queue URL and DynamoDB table name
SQS_QUEUE_URL = "https://sqs.us-east-1.amazonaws.com/861276083927/DiningSuggestionsQueue"
DYNAMO_TABLE_NAME = "UserSearchState"
denied_state = False

# Initialize DynamoDB table
user_state_table = dynamodb.Table(DYNAMO_TABLE_NAME)


def validate(slots):
    valid_cities = ['new york']
    valid_cuisines = ['chinese', 'indian', 'italian', 'mexican', 'thai']

    if not slots['Location']:
        return {
            'isValid': False,
            'violatedSlot': 'Location'
        }

    if slots['Location']['value']['originalValue'].lower() not in valid_cities:
        return {
            'isValid': False,
            'violatedSlot': 'Location',
            'message': f"We do not have suggestions in {slots['Location']['value']['originalValue']} currently. Please choose New York to proceed further."
        }

    if not slots['Cuisine']:
        return {
            'isValid': False,
            'violatedSlot': 'Cuisine'
        }

    if slots['Cuisine']['value']['originalValue'].lower() not in valid_cuisines:
        return {
            'isValid': False,
            'violatedSlot': 'Cuisine',
            'message': f"We do not have suggestions for {slots['Cuisine']['value']['originalValue']} restaurants currently. Please choose from {', '.join(valid_cuisines)}."
        }

    if not slots['DiningTime']:
        return {
            'isValid': False,
            'violatedSlot': 'DiningTime'
        }

    if not slots['NumPeople']:
        return {
            'isValid': False,
            'violatedSlot': 'NumPeople'
        }

    if not slots['Email']:
        return {
            'isValid': False,
            'violatedSlot': 'Email'
        }

    return {'isValid': True}


def store_last_search(user_id, location, cuisine, dining_time, num_people, email):
    """Store the user's last search in DynamoDB."""
    try:
        user_state_table.put_item(
            Item={
                'UserId': user_id,
                'LastLocation': location,
                'LastCuisine': cuisine,
                'DiningTime': dining_time,
                'NumPeople': num_people,
                'Email': email
            }
        )
        print(f"User state stored for user_id: {user_id}")
    except Exception as e:
        print(f"Error storing user state: {e}")


def get_last_search(user_id):
    """Retrieve the user's last search from DynamoDB."""
    try:
        response = user_state_table.get_item(Key={'UserId': user_id})
        if 'Item' in response:
            return response['Item']
        else:
            return None
    except Exception as e:
        print(f"Error retrieving user state: {e}")
        return None


def push_to_sqs(user_id, location, cuisine, dining_time, num_people, email, state):
    """Send the user's search details to SQS."""
    message = {
        'Location': location,
        'Cuisine': cuisine,
        'DiningTime': dining_time,
        'NumPeople': num_people,
        'Email': email,
        'SessionID': user_id,
        'State': state
    }

    try:
        response = sqs.send_message(
            QueueUrl=SQS_QUEUE_URL,
            MessageBody=json.dumps(message)
        )
        print(f"Message sent to SQS: {response}")
    except Exception as e:
        print(f"Error sending message to SQS: {e}")
        raise e


def elicit_slot(event, slot_name, message):
    """Elicit a slot value from the user."""
    return {
        'sessionState': {
            'dialogAction': {
                'type': 'ElicitSlot',
                'slotToElicit': slot_name
            },
            'intent': event['sessionState']['intent']
        },
        'messages': [{
            'contentType': 'PlainText',
            'content': message
        }]
    }


def close_session(event, message):
    """Close the session with a fulfillment message."""
    return {
        'sessionState': {
            'dialogAction': {
                'type': 'Close'
            },
            'intent': {
                'name': event['sessionState']['intent']['name'],
                'state': 'Fulfilled'
            }
        },
        'messages': [{
            'contentType': 'PlainText',
            'content': message
        }]
    }


def lambda_handler(event, context):
    print("Event: ", event)

    intent_name = event['sessionState']['intent']['name']
    slots = event['sessionState']['intent']['slots']
    user_id = event['sessionId']
    global denied_state

    print("Intent Name: ", intent_name)

    # Handle GreetingIntent
    if intent_name == "GreetingIntent":
        return {
            'sessionState': {
                'dialogAction': {
                    'type': 'Close'
                },
                'intent': {
                    'name': intent_name,
                    'state': 'Fulfilled'
                }
            },
            'messages': [{
                'contentType': 'PlainText',
                'content': 'Hi there, how can I help you today?'
            }]
        }

    # Handle ThankYouIntent
    elif intent_name == "ThankYouIntent":
        return {
            'sessionState': {
                'dialogAction': {
                    'type': 'Close'
                },
                'intent': {
                    'name': intent_name,
                    'state': 'Fulfilled'
                }
            },
            'messages': [{
                'contentType': 'PlainText',
                'content': "You're welcome! Let me know if you need anything else"
            }]
        }

    # Handle DiningSuggestionIntent
    elif intent_name == "DiningSuggestionIntent":

        if event['invocationSource'] == 'DialogCodeHook':

            validation_result = validate(slots)
            print(validation_result)
            print("DialogCodeHook")

            # Check if the user has a previous search
            last_search = get_last_search(user_id)
            print("Last search: ", last_search)

            if last_search and event['sessionState']['intent']['confirmationState'] == "None" and not denied_state:

                print("In confirmation state event")

                if not slots['Location']:
                    return {
                        "sessionState": {
                            "dialogAction": {
                                "type": "ElicitSlot",
                                "slotToElicit": 'Location'
                            },
                            "intent": {
                                "name": intent_name,
                                "slots": slots
                            }
                        }
                    }

                if slots['Location']['value']['interpretedValue'] not in ["New York"]:
                    return {
                        "sessionState": {
                            "dialogAction": {
                                "slotToElicit": 'Location',
                                "type": "ElicitSlot"
                            },
                            "intent": {
                                "name": intent_name,
                                "slots": slots
                            }
                        },
                        "messages": [
                            {
                                "contentType": "PlainText",
                                "content": f"We do not have suggestions in {slots['Location']['value']['interpretedValue']} currently. Please choose New York to proceed further."
                            }
                        ]

                    }

                if last_search and slots['Location']['value']['interpretedValue'] == last_search['LastLocation']:
                    # Prompt the user to reuse the previous search
                    print("In last search event")
                    return {
                        'sessionState': {
                            'dialogAction': {
                                'type': 'ConfirmIntent'
                            },
                            'intent': event['sessionState']['intent'],
                            'sessionAttributes': {
                                'PreviousLocation': last_search['LastLocation'],
                                'PreviousCuisine': last_search['LastCuisine']
                            },
                            'slots': slots,
                            'confirmationState': 'None'
                        },
                        'messages': [{
                            'contentType': 'PlainText',
                            'content': f"I have your previous preferences: {last_search['LastCuisine']} food in {last_search['LastLocation']}. Do you want to use them?"
                        }]
                    }

            elif not last_search or (event['sessionState']['intent']['confirmationState'] == "Denied" or denied_state):

                # Change denied_state to True
                denied_state = True
                print("In denied event and state changed to True")

                if not validation_result['isValid']:
                    if 'message' in validation_result:
                        return {
                            "sessionState": {
                                "dialogAction": {
                                    "slotToElicit": validation_result['violatedSlot'],
                                    "type": "ElicitSlot"
                                },
                                "intent": {
                                    "name": intent_name,
                                    "slots": slots
                                }
                            },
                            "messages": [
                                {
                                    "contentType": "PlainText",
                                    "content": validation_result['message']
                                }
                            ]

                        }
                    else:
                        return {
                            "sessionState": {
                                "dialogAction": {
                                    "type": "ElicitSlot",
                                    "slotToElicit": validation_result['violatedSlot']
                                },
                                "intent": {
                                    "name": intent_name,
                                    "slots": slots
                                }
                            }
                        }

                else:
                    return {
                        "sessionState": {
                            "dialogAction": {
                                "type": "Delegate"
                            },
                            "intent": {
                                "name": intent_name,
                                "slots": slots
                            }
                        }
                    }

            elif event['sessionState']['intent']['confirmationState'] == "Confirmed":

                print("Intent Confirmed, moving to FulfillmentCodeHook")

                last_search = get_last_search(user_id)
                if last_search:
                    slots['Location'] = {"value": {"interpretedValue": last_search["LastLocation"]}}
                    slots['Cuisine'] = {"value": {"interpretedValue": last_search["LastCuisine"]}}
                    slots['DiningTime'] = {"value": {"interpretedValue": last_search["DiningTime"]}}
                    slots['NumPeople'] = {"value": {"interpretedValue": last_search["NumPeople"]}}
                    slots['Email'] = {"value": {"interpretedValue": last_search["Email"]}}

                return {
                    'sessionState': {
                        'dialogAction': {
                            'type': 'Delegate'
                        },
                        'intent': {
                            'name': intent_name,
                            'slots': slots  # Ensure slots are preserved
                        }
                    }
                }


        elif event['invocationSource'] == 'FulfillmentCodeHook':

            print("FulfillmentCodeHook")

            if event['sessionState']['intent']['confirmationState'] == 'Confirmed':

                print("Confirmed")

                # User confirmed to reuse previous search
                location = slots['Location']['value']['interpretedValue']
                cuisine = slots['Cuisine']['value']['interpretedValue']
                dining_time = slots['DiningTime']['value']['interpretedValue']
                num_people = slots['NumPeople']['value']['interpretedValue']
                email = slots['Email']['value']['interpretedValue']
                state = 'old'

            else:
                print("Not Confirmed")
                # User provided new search details
                location = slots['Location']['value']['interpretedValue']
                cuisine = slots['Cuisine']['value']['interpretedValue']
                dining_time = slots['DiningTime']['value']['interpretedValue']
                num_people = slots['NumPeople']['value']['interpretedValue']
                email = slots['Email']['value']['interpretedValue']
                state = 'new'

                # Store the new search in DynamoDB
                store_last_search(user_id, location, cuisine, dining_time, num_people, email)

            # Push the search details to SQS
            try:
                print("Pushing to SQS")
                push_to_sqs(user_id, location, cuisine, dining_time, num_people, email, state)
                denied_state = False
                return close_session(event, f"Got it! We will send the recommendations to {email}.")
            except Exception as e:
                return close_session(event, "Sorry, I couldn't process your request at this time.")

    # Default response for unrecognized intents
    return {
        'sessionState': {
            'dialogAction': {
                'type': 'Close'
            },
            'intent': {
                'name': intent_name,
                'state': 'Failed'
            }
        },
        'messages': [{
            'contentType': 'PlainText',
            'content': 'Sorry, I did not understand your request.'
        }]
    }