#!/usr/bin/env python3
import argparse
import json
import os
import time
import hashlib
import hmac
import datetime as dt
from typing import Dict, Optional

import requests


def lwa_access_token() -> str:
    url = "https://api.amazon.com/auth/o2/token"
    data = {
        "grant_type": "refresh_token",
        "refresh_token": os.environ["SPAPI_LWA_REFRESH_TOKEN"].strip(),
        "client_id": os.environ["SPAPI_LWA_CLIENT_ID"].strip(),
        "client_secret": os.environ["SPAPI_LWA_CLIENT_SECRET"].strip(),
    }
    r = requests.post(url, data=data, timeout=60)
    r.raise_for_status()
    return r.json()["access_token"]


def _sign(key: bytes, msg: str) -> bytes:
    return hmac.new(key, msg.encode("utf-8"), hashlib.sha256).digest()


def _get_signature_key(secret_key: str, date_stamp: str, region: str, service: str) -> bytes:
    k_date = _sign(("AWS4" + secret_key).encode("utf-8"), date_stamp)
    k_region = _sign(k_date, region)
    k_service = _sign(k_region, service)
    k_signing = _sign(k_service, "aws4_request")
    return k_signing


def sigv4_headers(method: str, url: str, region: str, access_token: str, body: bytes = b"") -> Dict[str, str]:
    access_key = os.environ["AWS_ACCESS_KEY_ID"].strip()
    secret_key = os.environ["AWS_SECRET_ACCESS_KEY"].strip()

    t = dt.datetime.utcnow()
    amz_date = t.strftime("%Y%m%dT%H%M%SZ")
    date_stamp = t.strftime("%Y%m%d")

    parsed = requests.utils.urlparse(url)
    host = parsed.netloc
    canonical_uri = parsed.path or "/"
    canonical_querystring = parsed.query or ""

    headers = {
        "host": host,
        "x-amz-date": amz_date,
        "x-amz-access-token": access_token,
        "accept": "application/json",
        "content-type": "application/json",
    }

    signed_header_keys = sorted(headers.keys())
    canonical_headers = "".join([f"{k}:{headers[k].strip()}\n" for k in signed_header_keys])
    signed_headers = ";".join(signed_header_keys)

    payload_hash = hashlib.sha256(body).hexdigest()

    canonical_request = "\n".join(
        [method, canonical_uri, canonical_querystring, canonical_headers, signed_headers, payload_hash]
    )

    algorithm = "AWS4-HMAC-SHA256"
    credential_scope = f"{date_stamp}/{region}/execute-api/aws4_request"
    string_to_sign = "\n".join(
        [algorithm, amz_date, credential_scope, hashlib.sha256(canonical_request.encode("utf-8")).hexdigest()]
    )

    signing_key = _get_signature_key(secret_key, date_stamp, region, "execute-api")
    signature = hmac.new(signing_key, string_to_sign.encode("utf-8"), hashlib.sha256).hexdigest()

    authorization_header = (
        f"{algorithm} Credential={access_key}/{credential_scope}, "
        f"SignedHeaders={signed_headers}, Signature={signature}"
    )

    out = {k.title(): v for k, v in headers.items()}
    out["Authorization"] = authorization_header
    return out


def signed_get(url: str, region: str, access_token: str) -> dict:
    headers = sigv4_headers("GET", url, region, access_token, b"")
    r = requests.get(url, headers=headers, timeout=120)
    r.raise_for_status()
    return r.json()


def download_report(doc_url: str) -> str:
    # doc_url è pre-signed, non va firmato
    r = requests.get(doc_url, timeout=120)
    r.raise_for_status()
    return r.text


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--feed-id", required=True)
    ap.add_argument("--region", default=os.environ.get("AWS_REGION", "eu-west-1"))
    ap.add_argument("--poll-seconds", type=int, default=15)
    ap.add_argument("--max-polls", type=int, default=20)
    args = ap.parse_args()

    access_token = lwa_access_token()

    base = "https://sellingpartnerapi-eu.amazon.com/feeds/2021-06-30"
    feed_url = f"{base}/feeds/{args.feed_id}"

    for i in range(args.max_polls):
        data = signed_get(feed_url, args.region, access_token)
        payload = data.get("payload", {})
        status = payload.get("processingStatus")
        print(f"[poll {i+1}/{args.max_polls}] processingStatus={status}")
        print(json.dumps(payload, ensure_ascii=False, indent=2))

        if status in {"DONE", "CANCELLED", "FATAL"}:
            break
        time.sleep(args.poll_seconds)

    # se c'è report, scaricalo
    result_doc_id = payload.get("resultFeedDocumentId")
    if not result_doc_id:
        print("No resultFeedDocumentId yet (no report available).")
        return

    doc = signed_get(f"{base}/documents/{result_doc_id}", args.region, access_token)
    doc_payload = doc.get("payload", {})
    url = doc_payload.get("url")
    if not url:
        print("No URL in document payload.")
        print(json.dumps(doc_payload, ensure_ascii=False, indent=2))
        return

    text = download_report(url)
    with open("feed_processing_report.txt", "w", encoding="utf-8") as f:
        f.write(text)

    print("Saved feed_processing_report.txt")


if __name__ == "__main__":
    main()
