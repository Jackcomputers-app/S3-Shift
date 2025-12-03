import json
import boto3
from urllib.parse import unquote_plus

s3 = boto3.client('s3')

SHIFT = 3  # change number of shift times here 


def caesar_cipher(text: str, shift: int) -> str:
    result = []

    for ch in text:
        if 'a' <= ch <= 'z':
            base = ord('a')
            result.append(chr((ord(ch) - base + shift) % 26 + base))
        elif 'A' <= ch <= 'Z':
            base = ord('A')
            result.append(chr((ord(ch) - base + shift) % 26 + base))
        else:
            result.append(ch)

    return ''.join(result)


def lambda_handler(event, context):
    print("Received event:", json.dumps(event))

    for record in event['Records']:
        bucket = record['s3']['bucket']['name']
        key = unquote_plus(record['s3']['object']['key'])

        # Only process the specific file "normal.txt"
        if key != "normal.txt":
            print(f"Skipping object {key}, only process normal.txt")
            continue

        # Check value of normal.txt 
        obj = s3.get_object(Bucket=bucket, Key=key)
        body_bytes = obj['Body'].read()

        try:
            text = body_bytes.decode('utf-8')
        except UnicodeDecodeError:
            print("normal.txt is not UTF-8 text, skipping")
            continue

        # Encrypt with Caesar cipher
        encrypted_text = caesar_cipher(text, SHIFT)
        encrypted_bytes = encrypted_text.encode('utf-8')

        # Write shift.txt in the same S3 bucket
        dest_key = "shift.txt"

        s3.put_object(
            Bucket=bucket,
            Key=dest_key,
            Body=encrypted_bytes,
            ContentType="text/plain"
        )

        print(f"Encrypted {bucket}/normal.txt -> {bucket}/{dest_key}")

    return {"status": "ok"}
