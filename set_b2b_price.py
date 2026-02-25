import os
import json
import requests
from datetime import datetime, timezone

from botocore.awsrequest import AWSRequest
from botocore.auth import SigV4Auth
from botocore.credentials import Credentials

ENDPOINT = "https://sellingpartnerapi-eu.amazon.com"
SERVICE = "execute-api"
REGION = os.environ.get("AWS_REGION", "eu-west-1")

MARKETPLACE_ID = "APJ6JRA9NG5V4"   # Amazon.it
SKU = "T_0372_15150238000"         # <-- SKU TEST
B2B_PRICE = 367.37                # <-- prezzo B2B test


def lwa_access_token():
    r = requests.post(
        "https://api.amazon.com/auth/o2/token",
        data={
            "grant_type": "refresh_token",
            "refresh_token": os.environ["SPAPI_LWA_REFRESH_TOKEN"],
            "client_id": os.environ["SPAPI_LWA_CLIENT_ID"],
            "client_secret": os.environ["SPAPI_LWA_CLIENT_SECRET"],
        },
        timeout=30,
    )
    r.raise_for_status()
    return r.json()["access_token"]


def signed_request(method, url, body, access_token):
    headers = {
        "x-amz-access-token": access_token,
        "content-type": "application/json",
        "host": "sellingpartnerapi-eu.amazon.com",
    }

    body_json = json.dumps(body)
    req = AWSRequest(method=method, url=url, data=body_json, headers=headers)

    SigV4Auth(
        Credentials(
            os.environ["AWS_ACCESS_KEY_ID"],
            os.environ["AWS_SECRET_ACCESS_KEY"],
        ),
        SERVICE,
        REGION,
    ).add_auth(req)

    return requests.request(
        method,
        url,
        headers=dict(req.headers),
        data=body_json,
        timeout=60,
    )


def main():
    access_token = lwa_access_token()

    url = f"{ENDPOINT}/products/pricing/v0/price"

    body = {
        "sku": SKU,
        "marketplaceId": MARKETPLACE_ID,
        "pricingType": "Business",
        "price": {
            "listingPrice": {
                "amount": B2B_PRICE,
                "currencyCode": "EUR"
            }
        }
    }

    resp = signed_request("PUT", url, body, access_token)

    print("STATUS:", resp.status_code)
    print(resp.text)

    resp.raise_for_status()


if __name__ == "__main__":
    main()
