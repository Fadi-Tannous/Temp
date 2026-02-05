import requests
import json

# Replace these with your actual Gateway/Runtime details
GATEWAY_URL = "https://your-agentcore-gateway-url.aws.com/invocations"
HEADERS = {
    "Content-Type": "application/json",
    # "Authorization": "Bearer YOUR_TOKEN" # If your Gateway requires auth
}

def get_mcp_tools():
    """Fetch tool definitions using the MCP tools/list method."""
    payload = {
        "jsonrpc": "2.0",
        "method": "tools/list",
        "params": {},
        "id": "fetch-schema"
    }
    
    response = requests.post(GATEWAY_URL, headers=HEADERS, json=payload)
    response.raise_for_status()
    
    # Handle both standard JSON and potential SSE stream data
    result = response.json()
    if "result" in result:
        return result["result"].get("tools", [])
    return []

def generate_openapi(tools):
    """Wraps MCP tools into a valid OpenAPI 3.0.0 JSON structure."""
    openapi_spec = {
        "openapi": "3.0.0",
        "info": {
            "title": "FastMCP AgentCore Gateway",
            "version": "1.0.0",
            "description": "Generated from live AgentCore /invocations discovery."
        },
        "servers": [{"url": GATEWAY_URL.replace("/invocations", "")}],
        "paths": {
            "/invocations": {
                "post": {
                    "summary": "Invoke MCP Tools",
                    "operationId": "invokeMcpTool",
                    "requestBody": {
                        "content": {
                            "application/json": {
                                "schema": {"$ref": "#/components/schemas/McpRequest"}
                            }
                        }
                    },
                    "responses": {
                        "200": {"description": "Successful tool execution"}
                    }
                }
            }
        },
        "components": {
            "schemas": {
                "McpRequest": {
                    "type": "object",
                    "properties": {
                        "jsonrpc": {"type": "string", "enum": ["2.0"]},
                        "method": {"type": "string", "example": "tools/call"},
                        "params": {
                            "type": "object",
                            "properties": {
                                "name": {"type": "string", "description": "The tool name"},
                                "arguments": {"type": "object"}
                            }
                        },
                        "id": {"type": "string"}
                    }
                }
            }
        }
    }

    # Optional: You can add specific tool schemas to 'components' for better documentation
    for tool in tools:
        schema_name = f"Tool_{tool['name']}"
        openapi_spec["components"]["schemas"][schema_name] = tool.get("inputSchema", {})

    return openapi_spec

if __name__ == "__main__":
    try:
        print("Fetching tools from AgentCore Gateway...")
        tools_list = get_mcp_tools()
        
        print(f"Found {len(tools_list)} tools. Generating OpenAPI spec...")
        spec = generate_openapi(tools_list)
        
        with open("openapi.json", "w") as f:
            json.dump(spec, f, indent=2)
            
        print("Success! File saved as 'openapi.json'.")
    except Exception as e:
        print(f"Error: {e}")
