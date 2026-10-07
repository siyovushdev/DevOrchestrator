import asyncio
from mcp import Client
from task_bridge import MCP_URL, decode_mcp_result


async def main():
    async with Client(MCP_URL) as client:
        tools = await client.list_tools()

        print("TOOLS:")
        for tool in tools.tools:
            print("-", tool.name)

        result = await client.call_tool("list_projects", {})
        print()
        print("LIST_PROJECTS:")
        print(decode_mcp_result("list_projects", result))

        result = await client.call_tool("list_tasks", {})
        tasks = decode_mcp_result("list_tasks", result)
        print(f"TASK_LIST_TRANSPORT: PASS ({len(tasks)} tasks, {len(result.content)} content blocks)")


if __name__ == "__main__":
    asyncio.run(main())
