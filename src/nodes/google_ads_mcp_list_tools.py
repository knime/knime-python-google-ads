"""
KNIME node to list available Google Ads MCP tools with descriptions.
"""

import knime.extension as knext
import pandas as pd
from util.common import google_ad_port_type
import logging

LOGGER = logging.getLogger(__name__)


def get_mcp_tools_with_descriptions(connection):
    """
    Get available MCP tools from the Google Ads MCP server with their descriptions.
    Uses the provided Google Ads connection for authentication.
    """
    import os
    import inspect

    tools_info = []

    try:
        # Set up environment from the connection
        client = connection.client
        os.environ["GOOGLE_ADS_DEVELOPER_TOKEN"] = client.developer_token
        if client.login_customer_id:
            os.environ["GOOGLE_ADS_LOGIN_CUSTOMER_ID"] = client.login_customer_id

        # CRITICAL: Patch google.auth.default BEFORE importing any MCP modules
        import google.auth

        original_auth_default = google.auth.default

        def patched_auth_default(
            scopes=None, request=None, quota_project_id=None, default_scopes=None
        ):
            LOGGER.info(
                "Using patched google.auth.default with KNIME client credentials"
            )
            # Return our client's credentials instead of trying ADC
            # Google Ads client doesn't have project, so use None as default
            return client.credentials, None

        # Apply the patch to prevent ADC calls
        google.auth.default = patched_auth_default
        LOGGER.info("Patched google.auth.default to use KNIME credentials")

        # Now we can safely import MCP modules
        import ads_mcp.utils

        # Additional patches for the utils functions to use our client
        def patched_get_googleads_client():
            LOGGER.info("Using patched client from KNIME connection")
            return client

        def patched_get_googleads_service(service_name):
            LOGGER.info(f"Using patched service for {service_name}")
            return client.get_service(service_name)

        # Apply additional patches
        ads_mcp.utils._get_googleads_client = patched_get_googleads_client
        ads_mcp.utils.get_googleads_service = patched_get_googleads_service
        ads_mcp.utils._googleads_client = client

        LOGGER.info(
            "Successfully patched all MCP authentication to use KNIME Google Ads client"
        )

        # Now we can safely import the MCP modules with real authentication
        from ads_mcp.coordinator import mcp

        # Dynamically import all tool modules to trigger registration
        import ads_mcp.tools
        import importlib
        import pkgutil
        from pathlib import Path

        # Find and import all tool modules in ads_mcp.tools package
        tools_package = ads_mcp.tools

        # Handle both regular packages and namespace packages
        tools_path = None
        if hasattr(tools_package, "__file__") and tools_package.__file__:
            tools_path = Path(tools_package.__file__).parent
        elif hasattr(tools_package, "__path__"):
            # For namespace packages, use the first path
            tools_path = Path(list(tools_package.__path__)[0])
        else:
            raise RuntimeError("Could not determine path for ads_mcp.tools package")

        for module_info in pkgutil.iter_modules([str(tools_path)]):
            if not module_info.name.startswith("_"):  # Skip private modules
                try:
                    module_name = f"ads_mcp.tools.{module_info.name}"
                    importlib.import_module(module_name)
                    LOGGER.debug(f"Imported tool module: {module_name}")
                except Exception as e:
                    LOGGER.warning(f"Failed to import tool module {module_name}: {e}")

        # First ensure modules are imported with proper authentication
        LOGGER.info("Re-importing tool modules after setting authentication...")
        try:
            # Force reimport of modules now that authentication is set
            import importlib
            import sys

            # Remove modules from cache to force reload with new auth
            modules_to_reload = [
                name for name in sys.modules.keys() if name.startswith("ads_mcp.tools.")
            ]
            for module_name in modules_to_reload:
                if module_name in sys.modules:
                    del sys.modules[module_name]
                    LOGGER.debug(f"Removed {module_name} from module cache")

            # Now import modules again with authentication
            for module_info in pkgutil.iter_modules([str(tools_path)]):
                if not module_info.name.startswith("_"):
                    try:
                        module_name = f"ads_mcp.tools.{module_info.name}"
                        importlib.import_module(module_name)
                        LOGGER.info(
                            f"Successfully imported tool module with auth: {module_name}"
                        )
                    except Exception as e:
                        LOGGER.warning(
                            f"Failed to import tool module {module_name} even with auth: {e}"
                        )

        except Exception as e:
            LOGGER.warning(f"Failed to reload modules with auth: {e}")

        # Use the proper MCP API to list tools (handle async)
        LOGGER.info("Using MCP coordinator's list_tools() method...")

        try:
            # Get tools using the official MCP API
            tools_result = mcp.list_tools()

            # Check if it's a coroutine (async method)
            if hasattr(tools_result, "__await__"):
                LOGGER.info(
                    "list_tools() returned a coroutine, running synchronously..."
                )
                import asyncio

                # Run the coroutine synchronously
                try:
                    loop = asyncio.get_event_loop()
                    if loop.is_running():
                        # If loop is already running, we can't use run()
                        import concurrent.futures

                        with concurrent.futures.ThreadPoolExecutor() as executor:
                            future = executor.submit(asyncio.run, tools_result)
                            tools_list = future.result()
                    else:
                        tools_list = loop.run_until_complete(tools_result)
                except RuntimeError:
                    # No event loop, create one
                    tools_list = asyncio.run(tools_result)
            else:
                tools_list = tools_result

            LOGGER.info(f"MCP list_tools() final result type: {type(tools_list)}")
            LOGGER.info(f"MCP list_tools() final result: {tools_list}")

            # Convert the tools list to a dictionary for processing
            tools_dict = {}
            if hasattr(tools_list, "tools"):
                LOGGER.info(f"Found tools attribute: {tools_list.tools}")
                for tool in tools_list.tools:
                    if hasattr(tool, "name"):
                        tools_dict[tool.name] = tool
            elif isinstance(tools_list, list):
                LOGGER.info(f"Tools list has {len(tools_list)} items")
                for tool in tools_list:
                    if hasattr(tool, "name"):
                        tools_dict[tool.name] = tool
            elif isinstance(tools_list, dict):
                LOGGER.info(f"Tools dict has keys: {list(tools_list.keys())}")
                tools_dict = tools_list

            LOGGER.info(
                f"Final processed tools: {list(tools_dict.keys()) if tools_dict else 'None'}"
            )

        except Exception as e:
            LOGGER.error(f"Failed to use list_tools() API: {e}")
            import traceback

            LOGGER.error(f"Full traceback: {traceback.format_exc()}")

            # Fallback: try to access internal tool manager
            try:
                LOGGER.info("Trying fallback to _tool_manager...")
                if hasattr(mcp, "_tool_manager"):
                    tool_manager = mcp._tool_manager
                    LOGGER.info(f"Tool manager attributes: {dir(tool_manager)}")
                    if hasattr(tool_manager, "_tools"):
                        tools_dict = tool_manager._tools
                        LOGGER.info(
                            f"Fallback: found tools in _tool_manager: {list(tools_dict.keys())}"
                        )
                    else:
                        raise RuntimeError("Tool manager found but no _tools attribute")
                else:
                    raise RuntimeError("No _tool_manager found")
            except Exception as fallback_error:
                LOGGER.error(f"Fallback also failed: {fallback_error}")
                raise

        # Extract tool information with rich context
        for tool_name, tool_info in tools_dict.items():
            description = ""
            parameters = []
            examples = []
            hints = []
            tool_title = ""
            extended_description = ""

            # Get the actual function
            func = None
            if hasattr(tool_info, "func"):
                func = tool_info.func
            elif callable(tool_info):
                func = tool_info

            # Extract MCP tool metadata (title, description from add_tool)
            if hasattr(tool_info, "title"):
                tool_title = tool_info.title
            if hasattr(tool_info, "description"):
                extended_description = tool_info.description

            if func:
                # Extract basic description from docstring
                description = (func.__doc__ or "").strip()

                # Extract parameters from function signature with rich type info
                try:
                    sig = inspect.signature(func)
                    for param_name, param in sig.parameters.items():
                        param_type = "Any"
                        param_default = None
                        param_required = param.default == inspect.Parameter.empty

                        # Extract type annotation
                        if param.annotation != inspect.Parameter.empty:
                            param_type = str(param.annotation)
                            # Handle typing generics like List[str], Dict[str, Any]
                            if hasattr(param.annotation, "__origin__"):
                                param_type = str(param.annotation)

                        # Extract default value if present
                        if not param_required:
                            param_default = param.default

                        param_info = {
                            "name": param_name,
                            "type": param_type,
                            "required": param_required,
                            "default": param_default,
                        }
                        parameters.append(param_info)

                except Exception as e:
                    LOGGER.warning(f"Could not extract parameters for {tool_name}: {e}")

            # Parse extended description for hints and examples if it's the search tool
            if extended_description and "### Hints" in extended_description:
                # Extract hints section
                hints_section = (
                    extended_description.split("### Hints")[1]
                    if "### Hints" in extended_description
                    else ""
                )
                if hints_section:
                    hint_lines = [
                        line.strip()
                        for line in hints_section.split("\n")
                        if line.strip() and not line.startswith("###")
                    ]
                    hints = hint_lines[:10]  # Limit to first 10 hints for space

            # For search tool, include resource information
            resource_count = 0
            sample_resources = []
            if tool_name == "search":
                try:
                    # Try to get resource information from gaql_resources.json
                    import json

                    gaql_path = importlib.import_module(
                        "ads_mcp.utils"
                    ).get_gaql_resources_filepath()
                    with open(gaql_path, "r") as f:
                        resources_data = json.load(f)
                        resource_count = len(resources_data)
                        # Get sample resources (first 5)
                        sample_resources = [
                            res["resource"] for res in resources_data[:5]
                        ]
                except Exception as e:
                    LOGGER.warning(f"Could not load resource information: {e}")

            # Determine category and module safely
            module_name = (
                func.__module__ if func and hasattr(func, "__module__") else ""
            )
            category = "other"
            if module_name:
                if "core" in module_name:
                    category = "core"
                elif "search" in module_name:
                    category = "search"

            # Use extended_description as description if available, otherwise use docstring
            final_description = (
                extended_description if extended_description else description
            )

            tool_context = {
                "tool_name": tool_name,
                "title": tool_title
                if tool_title
                else tool_name.replace("_", " ").title(),
                "description": final_description,
                "parameters": str(parameters),
                "hints": str(hints[:5]),  # Top 5 hints
                "examples": str(examples),
            }

            tools_info.append(tool_context)

        if not tools_info:
            raise RuntimeError(
                "No tools discovered from MCP coordinator. Check MCP server configuration and authentication."
            )

    except Exception as e:
        LOGGER.error(f"Error during MCP tool discovery: {e}")
        raise RuntimeError(f"Failed to discover MCP tools: {e}")

    LOGGER.info(f"Discovered {len(tools_info)} MCP tools")
    return tools_info


@knext.node(
    name="Google Ads MCP Tools Lister (Labs)",
    node_type=knext.NodeType.MANIPULATOR,
    icon_path="icons/List-MCP-tools.png",
    category="Google Ads",
    keywords=["Google", "Google Ads", "MCP", "Tools", "List"],
    is_hidden=False,
)
@knext.input_port(
    name="Google Ads Connection",
    description="Google Ads connection for MCP server authentication.",
    port_type=google_ad_port_type,
)
@knext.output_table(
    name="Agent Message", description="JSON message for AI agent about available tools"
)
@knext.output_table(
    name="MCP Tools", description="Available MCP tools with descriptions"
)
class GoogleAdsMCPToolsList:
    """
    Dynamically discovers and lists all available Google Ads MCP tools with rich metadata.

    This node connects to the Google Ads MCP (Model Context Protocol) server and discovers
    all available tools with comprehensive metadata including descriptions, parameters,
    type hints, and usage examples for both AI agents and business users.

    **Configuration and Usage**

    The node connects to the Google Ads MCP server and authenticates using KNIME's
    [Google Ads Connector (Labs)](https://hub.knime.com/n/O-BpqOTHyFP_ckSM). It automatically discovers
    all available tools regardless of future additions to the MCP server, providing comprehensive tool
    information for both AI agents and business users.

    **Key Features**

    - **Dynamic Tool Discovery**: Automatically finds new tools without requiring code changes
    - **Rich Metadata Extraction**: Collects tool descriptions, parameters, hints, and usage examples
    - **Dual Output Format**: Provides both AI agent-optimized JSON and human-readable table formats
    - **Seamless Authentication**: Uses existing KNIME Google Ads connection credentials
    - **Fail-Fast Error Handling**: Ensures reliable tool discovery or provides clear failure indication

    **Output Ports**

    1. **Agent Message**: Structured JSON containing complete tool information optimized for AI systems
       and chat interfaces that need to understand available operations and their parameters.

    2. **MCP Tools**: Simplified table with Tool Name, Title, and Description columns designed for
       business users to review available capabilities.

    **MCP Workflow Integration**

    1. Connect your Google Ads connection to authenticate with the MCP server.
    2. Execute this node to discover all available tools dynamically.
    3. Use the **Agent Message** output with AI systems for automated tool selection.
    4. Use the **MCP Tools** output for human review of available capabilities.
    5. Proceed to the MCP Tools Executor node to run selected tools.

    **Mandatory Upstream Node**

    - You need to connect to the [Google Ads Connector (Labs)](https://hub.knime.com/n/O-BpqOTHyFP_ckSM)
      node to provide authentication credentials for the MCP server.

    **Supported Tools**

    - Includes `list_accessible_customers`, `search` (with 400+ Google Ads resources), and any future
      tools added to the Google Ads MCP server without requiring updates to this node.
    """

    def configure(self, configuration_context, connection_port):
        return None

    def execute(self, exec_context, connection_port):
        try:
            LOGGER.info("Discovering available MCP tools...")

            # Get MCP tools with descriptions using the connection
            tools_info = get_mcp_tools_with_descriptions(connection_port)

            # Create JSON message for agent (first output port)
            agent_message = {
                "message": f"I have discovered {len(tools_info)} Google Ads MCP tools available for use:",
                "tools": [
                    {
                        "name": tool["tool_name"],
                        "title": tool["title"],
                        "description": tool["description"],
                        "parameters": tool["parameters"],
                        "hints": tool["hints"] if tool["hints"] != "[]" else None,
                    }
                    for tool in tools_info
                ],
                "usage": "You can now call any of these tools by specifying the tool name and required parameters. Use the hints for proper formatting and constraints.",
            }

            # Convert JSON message to single-cell DataFrame
            import json

            agent_df = pd.DataFrame([{"message": json.dumps(agent_message, indent=2)}])

            # Create simplified DataFrame for business users (second output port)
            simplified_tools = [
                {
                    "Tool Name": tool["tool_name"],
                    "Title": tool["title"],
                    "Description": tool["description"],
                }
                for tool in tools_info
            ]
            tools_df = pd.DataFrame(simplified_tools)

            LOGGER.info(f"Found {len(tools_info)} MCP tools")

            return knext.Table.from_pandas(agent_df), knext.Table.from_pandas(tools_df)

        except Exception as e:
            LOGGER.error(f"Error listing MCP tools: {e}")
            raise RuntimeError(f"Failed to list MCP tools: {e}")
