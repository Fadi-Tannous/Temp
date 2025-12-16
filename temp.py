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
