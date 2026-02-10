import argparse
import json
import os
import sys
import requests

from botocore.awsrequest import AWSRequest
from botocore.auth import SigV4Auth
from botocore.credentials import Credentials

EU_ENDPOINT = "https://sellingpartnerapi-eu.amazon.com"
SERVICE = "execute-api"


def lwa_access_token() -> str:
    client_id = os.environ["SPAPI_LWA_CLIENT_ID"]
    client_secret = os.environ["SPAPI_LWA_CLIENT_SECRET"]
    refresh_token = os.environ["SPAPI_LWA_REFRESH_TOKEN"]

    r = requests.post(
        "https://api.amazon.com/auth/o2/token",
        data={
            "grant_type": "refresh_token",
            "refresh_token": refresh_token,
            "client_id": client_id,
            "client_secret": client_secret,
        },
        timeout=60,
    )

    if r.status_code >= 400:
        print("LWA ERROR STATUS:", r.status_code, file=sys.stderr)
        print("LWA ERROR BODY:", r.text[:2000], file=sys.stderr)

    r.raise_for_status()
    return r.json()["access_token"]


def _aws_headers(region: str, access_token: str) -> dict:
    # NOTE: host must match endpoint domain
    return {
        "x-amz-access-token": access_token,
        "content-type": "application/json",
        "accept": "application/json",
        "host": "sellingpartnerapi-eu.amazon.com",
    }


def _sanitize_headers(h: dict) -> dict:
    out = {}
    for k, v in (h or {}).items():
        kl = str(k).lower()
        if kl in {"authorization", "x-amz-access-token"}:
            out[k] = "***REDACTED***"
        else:
            out[k] = v
    return out


def signed_json(method: str, url: str, region: str, access_token: str, body):
    """
    Signed SP-API call (supports GET with body=None).
    On error, writes full request/response dump to spapi_request_response.txt
    (sanitizing tokens).
    """
    aws_access_key = os.environ["AWS_ACCESS_KEY_ID"]
    aws_secret_key = os.environ["AWS_SECRET_ACCESS_KEY"]

    headers = _aws_headers(region, access_token)

    data = None
    body_text = ""
    if body is not None:
        body_text = json.dumps(body, ensure_ascii=False)
        data = body_text.encode("utf-8")

    req = AWSRequest(method=method, url=url, data=data, headers=headers)
    SigV4Auth(Credentials(aws_access_key, aws_secret_key), SERVICE, region).add_auth(req)

    req_headers = dict(req.headers)

    resp = requests.request(
        method,
        url,
        data=req.data,
        headers=req_headers,
        timeout=120,
    )

    if resp.status_code >= 400:
        dump_path = "spapi_request_response.txt"
        rid = resp.headers.get("x-amzn-RequestId") or resp.headers.get("x-amz-request-id") or ""

        with open(dump_path, "w", encoding="utf-8") as f:
            f.write("=== REQUEST ===\n")
            f.write(f"Endpoint: {url}\n")
            f.write(f"Method: {method}\n\n")

            f.write("Request headers:\n")
            json.dump(_sanitize_headers(req_headers), f, ensure_ascii=False, indent=2)
            f.write("\n\n")

            if body is not None:
                f.write("Request body:\n")
                f.write(body_text)
                f.write("\n\n")
            else:
                f.write("Request body: <none>\n\n")

            f.write("=== RESPONSE ===\n")
            f.write(f"Status: {resp.status_code}\n")
            if rid:
                f.write(f"RequestId: {rid}\n")
            f.write(f"Content-Type: {resp.headers.get('content-type','')}\n\n")

            f.write("Response headers:\n")
            json.dump(_sanitize_headers(dict(resp.headers)), f, ensure_ascii=False, indent=2)
            f.write("\n\n")

            f.write("Response body:\n")
            f.write(resp.text)
            f.write("\n")

        print("Wrote SP-API request/response dump to:", dump_path, file=sys.stderr)

        print("SP-API ERROR URL:", url, file=sys.stderr)
        print("SP-API ERROR STATUS:", resp.status_code, file=sys.stderr)
        if rid:
            print("SP-API REQUEST ID:", rid, file=sys.stderr)
        print("SP-API ERROR content-type:", resp.headers.get("content-type", ""), file=sys.stderr)
        try:
            print("SP-API ERROR JSON:", json.dumps(resp.json())[:4000], file=sys.stderr)
        except Exception:
            print("SP-API ERROR BODY:", resp.text[:4000], file=sys.stderr)

    resp.raise_for_status()

    if resp.text.strip():
        return resp.json()
    return {}



def create_feed_document(region: str, access_token: str, content_type: str) -> dict:
    url = f"{EU_ENDPOINT}/feeds/2021-06-30/documents"
    return signed_json("POST", url, region, access_token, {"contentType": content_type})


def upload_document(upload_url: str, file_path: str, content_type: str):
    with open(file_path, "rb") as f:
        data = f.read()

    r = requests.put(upload_url, data=data, headers={"content-type": content_type}, timeout=300)

    if r.status_code >= 400:
        print("UPLOAD ERROR STATUS:", r.status_code, file=sys.stderr)
        print("UPLOAD ERROR BODY:", r.text[:2000], file=sys.stderr)

    r.raise_for_status()


def create_feed(region: str, access_token: str, feed_type: str, marketplace_ids: list[str], feed_document_id: str) -> dict:
    url = f"{EU_ENDPOINT}/feeds/2021-06-30/feeds"
    body = {
        "feedType": feed_type,
        "marketplaceIds": marketplace_ids,
        "inputFeedDocumentId": feed_document_id,
    }
    return signed_json("POST", url, region, access_token, body)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--file", required=True)
    ap.add_argument("--feed-type", required=True)
    ap.add_argument("--marketplace", action="append", required=True)
    ap.add_argument("--content-type", default="text/tab-separated-values; charset=UTF-8")
    ap.add_argument("--skip-sellers-check", action="store_true", help="Skip Sellers API authorization check")
    args = ap.parse_args()

    region = os.environ.get("AWS_REGION", "eu-west-1")

    if not os.path.exists(args.file):
        print(f"ERROR: file not found: {args.file}", file=sys.stderr)
        sys.exit(2)

    access_token = lwa_access_token()

    # ---- DEBUG / Authorization check (PROD sanity) ----
    # This confirms whether the app+token can call a basic SP-API resource.
    # If this fails with 401/403, you're not authorized in production (or token is sandbox/mismatch).
    if not args.skip_sellers_check:
        mp = signed_json(
            "GET",
            f"{EU_ENDPOINT}/sellers/v1/marketplaceParticipations",
            region,
            access_token,
            None,
        )
        print("MarketplaceParticipations OK:", json.dumps(mp)[:800])

    # ---- Feed document creation + upload ----
    doc = create_feed_document(region, access_token, args.content_type)
    feed_document_id = doc["feedDocumentId"]
    upload_url = doc["url"]

    upload_document(upload_url, args.file, args.content_type)

    # ---- Create feed ----
    feed = create_feed(region, access_token, args.feed_type, args.marketplace, feed_document_id)

    print("Created feed:", json.dumps(feed))
    print("feedType:", args.feed_type)
    print("file:", args.file)
    print("marketplaceIds:", args.marketplace)


if __name__ == "__main__":
    main()
