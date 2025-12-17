import re
from typing import Dict, Any, Union

def validate_batch_parameters(payload: Union[Dict[str, Any], object]) -> bool:
    """
    Validates batch parameters for S3 paths, IAM Role ARNs, and access type dependencies.
    """
    
    def get_val(key):
        if isinstance(payload, dict):
            return payload.get(key, "")
        return getattr(payload, key, "")

    # --- Regex Patterns ---
    
    # S3: s3://<bucket>/<key>
    s3_pattern = r"^s3://[a-z0-9][a-z0-9.-]+[a-z0-9]/(?:.*)$"
    
    # IAM Role ARN: arn:partition:iam::account-id:role/role-name
    # Enforces 'iam' service, empty region, and 'role' resource type
    role_arn_pattern = r"^arn:aws[a-zA-Z-]*:iam::[0-9]{12}:role/.+$"
    
    # SQS ARN: arn:partition:sqs:region:account-id:queue-name
    sqs_arn_pattern = r"^arn:aws[a-zA-Z-]*:sqs:[a-z0-9\-]+:[0-9]{12}:.+$"

    # --- Extract Values ---
    input_s3 = get_val("inputsS3Path")
    output_s3 = get_val("outputS3Path")
    input_access = get_val("inputAccessType")
    input_role_arn = get_val("inputAssumeRoleARN")
    output_access = get_val("outputAccessType")
    output_role_arn = get_val("outputAssumeRoleARN")
    comp_s3 = get_val("completionS3TriggerPath")
    comp_sqs = get_val("completionSQSQueue")

    # --- Validations ---

    # 1. Check S3 Path Syntax
    if not input_s3 or not re.match(s3_pattern, str(input_s3)):
        raise ValueError(f"Invalid inputsS3Path: '{input_s3}'. Must be a valid s3:// URI.")
    
    if not output_s3 or not re.match(s3_pattern, str(output_s3)):
        raise ValueError(f"Invalid outputS3Path: '{output_s3}'. Must be a valid s3:// URI.")

    # 2. Input Access Type Validation (Specific IAM Role Check)
    if input_access == "assume_role":
        if not input_role_arn:
            raise ValueError("inputAssumeRoleARN is required when inputAccessType is 'assume_role'.")
        if not re.match(role_arn_pattern, str(input_role_arn)):
            raise ValueError(f"Invalid inputAssumeRoleARN: '{input_role_arn}'. Must be a valid IAM Role ARN (e.g., arn:aws:iam::...:role/...).")

    # 3. Output Access Type Validation (Specific IAM Role Check)
    if output_access == "assume_role":
        if not output_role_arn:
            raise ValueError("outputAssumeRoleARN is required when outputAccessType is 'assume_role'.")
        if not re.match(role_arn_pattern, str(output_role_arn)):
            raise ValueError(f"Invalid outputAssumeRoleARN: '{output_role_arn}'. Must be a valid IAM Role ARN (e.g., arn:aws:iam::...:role/...).")

    # 4. Completion S3 Trigger Path
    if comp_s3 and str(comp_s3).strip():
        if not re.match(s3_pattern, str(comp_s3)):
            raise ValueError(f"Invalid completionS3TriggerPath: '{comp_s3}'. Must be a valid s3:// URI.")

    # 5. Completion SQS Queue ARN
    if comp_sqs and str(comp_sqs).strip():
        if not re.match(sqs_arn_pattern, str(comp_sqs)):
            raise ValueError(f"Invalid completionSQSQueue: '{comp_sqs}'. Must be a valid SQS ARN.")

    return True

import os, secrets, json
from contextlib import asynccontextmanager
from fastapi import FastAPI, Depends, HTTPException, Request, Header
from fastapi.responses import StreamingResponse
from openai import AsyncAzureOpenAI

# State & Config
state = {"key": ""}
client = AsyncAzureOpenAI(
    api_key=os.getenv("UPSTREAM_API_KEY", "dummy"),
    api_version=os.getenv("OPENAI_API_VERSION", "2023-05-15"),
    azure_endpoint=os.getenv("UPSTREAM_ENDPOINT", "https://your-custom-gateway.com")
)

@asynccontextmanager
async def lifespan(app: FastAPI):
    state["key"] = secrets.token_urlsafe(32)
    print(f"\n🔑 API KEY:\nBearer {state['key']}\n")
    yield

app = FastAPI(lifespan=lifespan)

async def auth(authorization: str = Header(None)):
    if not authorization or authorization.replace("Bearer ", "") != state["key"]:
        raise HTTPException(401, "Invalid Key")

@app.get("/v1/models")
async def list_models(_: str = Depends(auth), provider: str = Header(None)):
    if provider not in ["bedrock", "azure"]:
        raise HTTPException(400, "Header 'provider' must be 'bedrock' or 'azure'")
    return {"object": "list", "data": [{"id": "gpt-4", "object": "model", "created": 0, "owned_by": "openai"}]}

async def stream_gen(body):
    try:
        async for chunk in await client.chat.completions.create(**body):
            yield f"data: {chunk.model_dump_json()}\n\n"
        yield "data: [DONE]\n\n"
    except Exception as e:
        yield f"data: {json.dumps({'error': str(e)})}\n\n"

@app.post("/v1/chat/completions")
async def proxy(req: Request, _: str = Depends(auth)):
    try:
        body = await req.json()
        if body.get("stream"):
            return StreamingResponse(stream_gen(body), media_type="text/event-stream")
        return (await client.chat.completions.create(**body)).model_dump()
    except Exception as e:
        raise HTTPException(500, str(e))

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
