import json

import requests
from src.config import DATA_CHANNEL_WEBHOOKS


def send_messages_to_slack(message: str) -> None:
    try:
        requests.post(
            DATA_CHANNEL_WEBHOOKS,
            data=json.dumps({'text': [{
                "color": "#f2c744",
                "blocks": [
                    {
                        "type": "section",
                        "text": {
                            "type": "mrkdwn",
                            "text": message
                        }
                    },

                ]
            }]}),
            headers={'Content-Type': 'application/json'}
        )
    except Exception as e:
        raise Exception(e)
