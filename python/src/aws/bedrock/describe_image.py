import os
import boto3
import json
import base64

bedrock_runtime_client = boto3.client(
    "bedrock-runtime",
    aws_access_key_id=os.getenv("AWS_KEY"),
    aws_secret_access_key=os.getenv("AWS_SECRET"),
    region_name=os.getenv("AWS_REGION"),
)

model_id = "anthropic.claude-3-haiku-20240307-v1:0"

with open("data/captures/bootie_creek.jpg", "rb") as image_file:
    encoded_image = base64.b64encode(image_file.read()).decode()

payload = {
    "messages": [
        {
            "role": "user",
            "content": [
                {
                    "type": "image",
                    "source": {
                        "type": "base64",
                        "media_type": "image/png",
                        "data": encoded_image,
                    },
                },
                {"type": "text", "text": "What do you see?"},
            ],
        }
    ],
    "max_tokens": 1000,
    "anthropic_version": "bedrock-2023-05-31",
}

response = bedrock_runtime_client.invoke_model(
    modelId=model_id, contentType="application/json", body=json.dumps(payload)
)

output_binary = response["body"].read()
output_json = json.loads(output_binary)
output = output_json["content"][0]["text"]

print(f"AI's description of the image:{output}")
