import json
import boto3

# Initialize SQS client
sqs = boto3.client('sqs', region_name='us-east-1')

# Replace with your actual SQS queue URL
SQS_QUEUE_URL = "https://sqs.us-east-1.amazonaws.com/861276083927/DiningSuggestionsQueue"


def validate(slots):
    valid_cities = ['new york']
    valid_cuisines = ['chinese', 'indian', 'italian', 'mexican', 'thai']

    if not slots['Location']:
        print("Empty location")
        return {
            'isValid': False,
            'violatedSlot': 'Location'
        }

    if slots['Location']['value']['originalValue'].lower() not in valid_cities:
        print("Invalid location")
        return {
            'isValid': False,
            'violatedSlot': 'Location',
            'message': 'We do not have suggestions in {} currently, Please choose New York to proceed further'.format(
                slots['Location']['value']['originalValue'])
        }

    if not slots['Cuisine']:
        print("Empty Cuisine")
        return {
            'isValid': False,
            'violatedSlot': 'Cuisine'
        }

    if slots['Cuisine']['value']['originalValue'].lower() not in valid_cuisines:
        print("Invalid cuisine")
        return {
            'isValid': False,
            'violatedSlot': 'Cuisine',
            'message': 'We do not have suggestions for {} restaurants currently, please choose from {}'.format(
                slots['Cuisine']['value']['originalValue'], ', '.join(valid_cuisines))
        }

    if not slots['DiningTime']:
        print("Empty dining time")
        return {
            'isValid': False,
            'violatedSlot': 'DiningTime'
        }

    if not slots['NumPeople']:
        print("Empty people count")
        return {
            'isValid': False,
            'violatedSlot': 'NumPeople'
        }

    if not slots['Email']:
        print("Empty email")
        return {
            'isValid': False,
            'violatedSlot': 'Email'
        }

    return {'isValid': True}


def lambda_handler(event, context):
    intent_name = event['sessionState']['intent']['name']
    slots = event['sessionState']['intent']['slots']

    # Handle GreetingIntent
    if intent_name == "GreetingIntent":
        return {
            "sessionState": {
                "dialogAction": {
                    "type": "Close"
                },
                "intent": {
                    "name": intent_name,
                    "state": "Fulfilled"
                }
            },
            "messages": [
                {
                    "contentType": "PlainText",
                    "content": "Hi there, how can I help you?"
                }
            ]
        }

    # Handle ThankingIntent
    elif intent_name == "ThankYouIntent":
        return {
            "sessionState": {
                "dialogAction": {
                    "type": "Close"
                },
                "intent": {
                    "name": intent_name,
                    "state": "Fulfilled"
                }
            },
            "messages": [
                {
                    "contentType": "PlainText",
                    "content": "You're welcome! Let me know if you need anything else."
                }
            ]
        }

    # Handle DiningSuggestionIntent
    elif intent_name == "DiningSuggestionIntent":

        validation_result = validate(event['sessionState']['intent']['slots'])

        print(validation_result)
        print(event['invocationSource'])
        print(event['sessionState']['intent']['slots'])

        if event['invocationSource'] == 'DialogCodeHook':

            if not validation_result['isValid']:

                if 'message' in validation_result:

                    response = {
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
                    response = {
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

                response = {
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

        if event['invocationSource'] == 'FulfillmentCodeHook':

            location = slots['Location']['value']['interpretedValue']
            cuisine = slots['Cuisine']['value']['interpretedValue']
            dining_time = slots['DiningTime']['value']['interpretedValue']
            num_people = slots['NumPeople']['value']['interpretedValue']
            email = slots['Email']['value']['interpretedValue']

            message = {
                "Location": location,
                "Cuisine": cuisine,
                "DiningTime": dining_time,
                "NumPeople": num_people,
                "Email": email
            }

            # Send message to SQS
            try:
                response = sqs.send_message(
                    QueueUrl=SQS_QUEUE_URL,
                    MessageBody=json.dumps(message)
                )
                print(f"Message sent to SQS: {response}")
            except Exception as e:
                print(f"Error sending message to SQS: {e}")
                response = {
                    "sessionState": {
                        "dialogAction": {
                            "type": "Close"
                        },
                        "intent": {
                            "name": intent_name,
                            "slots": slots,
                            "state": "Failed"
                        }
                    },
                    "messages": [
                        {
                            "contentType": "PlainText",
                            "content": "Sorry, I couldn't process your request at this time."
                        }
                    ]
                }

            response = {
                "sessionState": {
                    "dialogAction": {
                        "type": "Close"
                    },
                    "intent": {
                        "name": intent_name,
                        "slots": slots,
                        "state": 'Fulfilled'
                    }
                },
                "messages": [
                    {
                        "contentType": "PlainText",
                        "content": f"Got it, we will send the recommendations to {email}."
                    }
                ]
            }

        return response
