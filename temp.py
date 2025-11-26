import os
import secrets
import json
from contextlib import asynccontextmanager
from fastapi import FastAPI, Depends, HTTPException, Request, Header
from fastapi.responses import StreamingResponse
from openai import AsyncAzureOpenAI

# Configuration
UPSTREAM_ENDPOINT = os.getenv("UPSTREAM_ENDPOINT", "https://your-custom-gateway.com")
UPSTREAM_KEY = os.getenv("UPSTREAM_API_KEY", "dummy-key")
API_VERSION = os.getenv("OPENAI_API_VERSION", "2023-05-15")

# State to hold the generated key
app_state = {"proxy_key": ""}

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Generate and print the key on startup
    app_state["proxy_key"] = secrets.token_urlsafe(32)
    print(f"\n\n🔑 SERVER STARTED. USE THIS API KEY IN LANGFUSE:\nBearer {app_state['proxy_key']}\n\n")
    yield

app = FastAPI(lifespan=lifespan)

# Initialize Azure Client
client = AsyncAzureOpenAI(
    api_key=UPSTREAM_KEY,
    api_version=API_VERSION,
    azure_endpoint=UPSTREAM_ENDPOINT
)

async def verify_key(authorization: str = Header(None)):
    if not authorization or authorization.replace("Bearer ", "") != app_state["proxy_key"]:
        raise HTTPException(status_code=401, detail="Invalid Proxy API Key")

@app.get("/v1/models")
async def list_models(_: str = Depends(verify_key)):
    return {
        "object": "list",
        "data": [
            {"id": "gpt-3.5-turbo", "object": "model", "created": 1677610602, "owned_by": "openai"},
            {"id": "gpt-4", "object": "model", "created": 1687882411, "owned_by": "openai"}
        ]
    }

async def generate_stream(body):
    """Yields SSE events for streaming responses."""
    try:
        stream = await client.chat.completions.create(**body)
        async for chunk in stream:
            # Format as Server-Sent Event (SSE)
            yield f"data: {chunk.model_dump_json()}\n\n"
        yield "data: [DONE]\n\n"
    except Exception as e:
        # In a stream, we can't easily raise HTTP 500, so we yield an error block
        error_msg = json.dumps({"error": str(e)})
        yield f"data: {error_msg}\n\n"

@app.post("/v1/chat/completions")
async def proxy_chat(request: Request, _: str = Depends(verify_key)):
    try:
        body = await request.json()
        
        # Handle Streaming
        if body.get("stream", False):
            return StreamingResponse(
                generate_stream(body), 
                media_type="text/event-stream"
            )

        # Handle Normal Request (Non-Streaming)
        response = await client.chat.completions.create(**body)
        return response.model_dump()
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
