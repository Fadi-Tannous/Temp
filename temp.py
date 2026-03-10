from traceloop.sdk.decorators import workflow, task

@task(name="format_agent_prompt")
def format_prompt(user_input: str):
    # This simulates a sub-task or tool execution
    print(f"Formatting prompt for: {user_input}")
    return f"System: You are Florence. User: {user_input}"

@workflow(name="florence_manual_test_run")
def run_agent_test():
    # This acts as the main entry point for the trace
    print("Starting agent test workflow...")
    formatted_data = format_prompt("Hello Dynatrace")
    return {"status": "success", "result": formatted_data}

# Execute the workflow to trigger the trace
run_agent_test()
