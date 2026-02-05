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

import json

def generate_openapi_from_mcp(url: str, tools: list) -> str:
    """
    Converts MCP tool definitions into a valid OpenAPI 3.0.0 JSON string
    mapped to the AWS AgentCore /invocations entry point.
    """
    # Initialize the base OpenAPI structure
    openapi_spec = {
        "openapi": "3.0.0",
        "info": {
            "title": "FastMCP Server Specification",
            "version": "1.0.0"
        },
        "servers": [{"url": url.rstrip("/")}],
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
                        "200": {
                            "description": "Successful operation",
                            "content": {
                                "application/json": {
                                    "schema": {"$ref": "#/components/schemas/McpResponse"}
                                }
                            }
                        }
                    }
                }
            }
        },
        "components": {
            "schemas": {
                "McpRequest": {
                    "type": "object",
                    "required": ["jsonrpc", "method", "params", "id"],
                    "properties": {
                        "jsonrpc": {"type": "string", "enum": ["2.0"]},
                        "method": {"type": "string", "enum": ["tools/call"]},
                        "params": {
                            "type": "object",
                            "required": ["name", "arguments"],
                            "properties": {
                                "name": {
                                    "type": "string",
                                    "enum": [t["name"] for t in tools]
                                },
                                "arguments": {"type": "object"}
                            }
                        },
                        "id": {"type": "string"}
                    }
                },
                "McpResponse": {
                    "type": "object",
                    "properties": {
                        "jsonrpc": {"type": "string"},
                        "result": {"type": "object"},
                        "id": {"type": "string"}
                    }
                }
            }
        }
    }

    # Inject individual tool schemas into components for reference/documentation
    for tool in tools:
        schema_name = f"ToolInput_{tool['name']}"
        # MCP inputSchema is typically a valid JSON Schema (subset of OpenAPI)
        openapi_spec["components"]["schemas"][schema_name] = tool.get("inputSchema", {"type": "object"})

    return json.dumps(openapi_spec, indent=2)

# Usage Example:
# tools_from_api = [{"name": "add", "inputSchema": {...}}, ...]
# print(generate_openapi_from_mcp("https://api.example.com", tools_from_api))

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
