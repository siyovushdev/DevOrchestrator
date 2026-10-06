import asyncio
from mcp import Client


async def main():
    async with Client("http://127.0.0.1:8787/mcp") as client:
        tools = await client.list_tools()

        print("TOOLS:")
        for tool in tools.tools:
            print("-", tool.name)

        result = await client.call_tool("list_projects", {})
        print()
        print("LIST_PROJECTS:")
        print(result.structured_content)


asyncio.run(main())
