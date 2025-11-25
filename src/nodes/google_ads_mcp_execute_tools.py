"""
KNIME node for natural language queries to Google Ads API using authenticated client.
"""

import knime.extension as knext
import pandas as pd
import json
from util.common import google_ad_port_type
import logging

LOGGER = logging.getLogger(__name__)


def call_mcp_tool(tool_name, parameters, connection):
    """
    Call a specific MCP tool using the actual MCP server with proper authentication.
    Uses the provided Google Ads connection for authentication.
    """
    import os
    import sys

    try:
        LOGGER.info(f"Calling MCP tool: {tool_name} with params: {parameters}")

        # Set up environment from the connection
        client = connection.client
        os.environ["GOOGLE_ADS_DEVELOPER_TOKEN"] = client.developer_token
        if client.login_customer_id:
            os.environ["GOOGLE_ADS_LOGIN_CUSTOMER_ID"] = client.login_customer_id

        # Clear MCP-specific modules for fresh authentication setup
        modules_to_clear = [m for m in sys.modules.keys() if m.startswith("ads_mcp")]
        for module_name in modules_to_clear:
            if module_name in sys.modules:
                try:
                    del sys.modules[module_name]
                except Exception as e:
                    LOGGER.warning(f"Could not clear module {module_name}: {e}")

        # Patch google.auth.default to use KNIME credentials
        import google.auth

        original_auth_default = getattr(
            google.auth, "_original_default", google.auth.default
        )

        def patched_auth_default(
            scopes=None, request=None, quota_project_id=None, default_scopes=None
        ):
            return client.credentials, None

        # Apply the patch to prevent ADC calls
        google.auth._original_default = original_auth_default
        google.auth.default = patched_auth_default

        # Now we can safely import MCP modules with fresh authentication
        import ads_mcp.utils

        # Replace the module-level client instance that was created during import
        ads_mcp.utils._googleads_client = client

        # CRITICAL: Also patch the credential creation functions to prevent ADC calls during service creation
        def patched_create_credentials():
            return client.credentials

        def patched_get_googleads_client():
            return client

        def patched_get_googleads_service(service_name):
            # Ensure the service is created with our authenticated client
            return client.get_service(service_name)

        # Apply comprehensive patches to ensure our client is used everywhere
        if hasattr(ads_mcp.utils, "_create_credentials"):
            ads_mcp.utils._create_credentials = patched_create_credentials
        if hasattr(ads_mcp.utils, "_get_googleads_client"):
            ads_mcp.utils._get_googleads_client = patched_get_googleads_client
        if hasattr(ads_mcp.utils, "get_googleads_service"):
            ads_mcp.utils.get_googleads_service = patched_get_googleads_service
        if hasattr(ads_mcp.utils, "get_googleads_client"):
            ads_mcp.utils.get_googleads_client = patched_get_googleads_client

        # Set the client as a module attribute
        ads_mcp.utils._googleads_client = client
        ads_mcp.utils.client = client

        # Import MCP coordinator and ensure tools are registered
        from ads_mcp.coordinator import mcp

        # Import tool modules to ensure they register with MCP coordinator
        try:
            import ads_mcp.tools.core
            import ads_mcp.tools.search
        except ImportError as e:
            LOGGER.warning(f"Could not import some MCP tool modules: {e}")
            # Try alternative import approach
            try:
                import ads_mcp.tools
            except ImportError as e2:
                LOGGER.error(f"Failed to import any MCP tools: {e2}")

        try:
            # Use the official MCP API to call the tool
            tool_result = mcp.call_tool(tool_name, parameters)

            # Check if it's a coroutine (async method)
            if hasattr(tool_result, "__await__"):
                import asyncio

                # Run the coroutine synchronously
                try:
                    loop = asyncio.get_event_loop()
                    if loop.is_running():
                        # If loop is already running, we can't use run()
                        import concurrent.futures

                        with concurrent.futures.ThreadPoolExecutor() as executor:
                            future = executor.submit(asyncio.run, tool_result)
                            result = future.result()
                    else:
                        result = loop.run_until_complete(tool_result)
                except RuntimeError:
                    # No event loop, create one
                    result = asyncio.run(tool_result)
            else:
                result = tool_result

            return result

        except Exception as e:
            LOGGER.error(f"Failed to execute MCP tool {tool_name}: {e}")
            raise RuntimeError(f"Failed to execute MCP tool {tool_name}: {e}")

    except Exception as e:
        LOGGER.error(f"Error calling MCP tool {tool_name}: {e}")
        raise RuntimeError(f"Failed to execute MCP tool {tool_name}: {e}")


@knext.node(
    name="Google Ads MCP Tool Executor (Labs)",
    node_type=knext.NodeType.MANIPULATOR,
    icon_path="icons/gads-icon.png",
    category="Google Ads",
    keywords=["Google", "Google Ads", "MCP", "Tools", "Execute", "Execution"],
    is_hidden=True,
)
@knext.input_port(
    name="Google Ads Connection",
    description="Google Ads connection with authentication details",
    port_type=google_ad_port_type,
)
@knext.output_table(name="Output Data", description="KNIME table with MCP tool results")
class GoogleAdsMCPToolsExecutor:
    """
    Executes Google Ads MCP tools with proper authentication and parameter handling.

    This node executes Google Ads MCP (Model Context Protocol) tools selected by AI agents
    with proper authentication and intelligent parameter preprocessing.

    **Configuration and Usage**

    The node takes a tool name and parameters (typically selected by an AI agent) and executes the corresponding
    MCP tool using KNIME's [Google Ads Connector (Labs)](https://hub.knime.com/n/O-BpqOTHyFP_ckSM) for authentication.
    It handles all the complex authentication setup and provides structured results as KNIME tables.

    **Configuration Parameters**

    - **Tool Name**: The specific MCP tool to execute (e.g., `search`, `list_accessible_customers`)
    - **Tool Parameters**: JSON-formatted parameters for the tool (use `{}` for tools requiring no parameters)

    **Mandatory Upstream Node**

    - You need to connect to the [Google Ads Connector (Labs)](https://hub.knime.com/n/O-BpqOTHyFP_ckSM)
      node to provide authentication credentials for the MCP server.

    **Output**

    - Returns a KNIME table with structured results from the executed MCP tool, automatically converted
      to the most appropriate tabular format for further analysis.
    """

    tool_name = knext.StringParameter(
        label="Tool Name",
        description="Name of the MCP tool to execute (e.g., 'search', 'list_accessible_customers')",
        default_value="list_accessible_customers",
    )

    tool_parameters = knext.MultilineStringParameter(
        label="Tool Parameters (JSON)",
        description="Parameters for the MCP tool in JSON format. Use {} for tools that don't need parameters.",
        default_value="{}",
        number_of_lines=5,
    )

    def _fix_common_parameter_issues(self, parameters):
        """
        Auto-fix common agent parameter mistakes to improve success rate.
        """
        if not isinstance(parameters, dict):
            return parameters

        # Skip processing if parameters are empty - some tools like list_accessible_customers don't need parameters
        if not parameters:
            return parameters

        # Fix fields parameter if it's a comma-separated string instead of array
        if "fields" in parameters and isinstance(parameters["fields"], str):
            # Convert comma-separated string to array
            if "," in parameters["fields"]:
                fixed_fields = [
                    field.strip() for field in parameters["fields"].split(",")
                ]
                LOGGER.info(
                    f"Auto-fixed fields parameter from string '{parameters['fields']}' to array {fixed_fields}"
                )
                parameters["fields"] = fixed_fields
            else:
                # Single field as string, convert to single-item array
                parameters["fields"] = [parameters["fields"]]
                LOGGER.info(f"Auto-fixed single field parameter to array format")

        # Fix customer_id format (remove any hyphens or spaces)
        if "customer_id" in parameters and isinstance(parameters["customer_id"], str):
            original_id = parameters["customer_id"]
            clean_id = "".join(char for char in original_id if char.isdigit())
            if clean_id != original_id:
                parameters["customer_id"] = clean_id
                LOGGER.info(
                    f"Auto-fixed customer_id from '{original_id}' to '{clean_id}'"
                )

        return parameters

    def _extract_mcp_data(self, result):
        """
        Extract clean data from various MCP response formats.
        """
        # Handle MCP tuple format: (TextContent_list, result_dict)
        if isinstance(result, tuple) and len(result) == 2:
            text_contents, result_dict = result
            LOGGER.info(f"Extracting from MCP tuple format")

            # Use the result dictionary which contains the actual data
            if isinstance(result_dict, dict) and "result" in result_dict:
                return result_dict["result"]
            else:
                return result_dict

        # Handle MCP content format: {"content": [{"text": "..."}]}
        elif isinstance(result, dict) and "content" in result:
            content = result["content"]
            if content and isinstance(content[0], dict) and "text" in content[0]:
                text_content = content[0]["text"]
                try:
                    # Try to parse as JSON
                    return json.loads(text_content)
                except (json.JSONDecodeError, TypeError):
                    # Return as plain text
                    return text_content
            else:
                return content

        # Handle direct results
        elif isinstance(result, (list, dict, str, int, float, bool)):
            return result

        # Fallback for any other type
        else:
            return str(result)

    def configure(self, configuration_context, input_spec):
        # Validate that we have a Google Ads connection
        return None

    def execute(self, exec_context, google_ads_connection):
        try:
            LOGGER.info(f"Executing MCP tool: {self.tool_name}")

            # Parse tool parameters
            try:
                param_text = self.tool_parameters.strip()
                if param_text and param_text != "{}":
                    parameters = json.loads(param_text)
                else:
                    parameters = {}
                LOGGER.info(f"Parsed parameters: {parameters}")
            except json.JSONDecodeError as e:
                raise knext.InvalidParametersError(
                    f"Invalid JSON in tool parameters: {e}"
                )

            # Auto-fix common agent parameter mistakes
            parameters = self._fix_common_parameter_issues(parameters)

            # Add customer_id from connection if not provided (fallback only)
            # Skip this for tools that don't need customer_id like list_accessible_customers
            if (
                "customer_id" not in parameters
                and self.tool_name not in ["list_accessible_customers"]
                and google_ads_connection
                and hasattr(google_ads_connection, "spec")
            ):
                if (
                    hasattr(google_ads_connection.spec, "account_id")
                    and google_ads_connection.spec.account_id
                ):
                    parameters["customer_id"] = google_ads_connection.spec.account_id
                    LOGGER.info(
                        f"Added fallback customer_id from connection: {parameters['customer_id']}"
                    )

            # Execute the MCP tool with proper authentication
            result = call_mcp_tool(self.tool_name, parameters, google_ads_connection)

        except Exception as e:
            LOGGER.error(f"Failed to execute MCP tool '{self.tool_name}': {e}")
            raise knext.WorkflowExecutionError(
                f"Failed to execute MCP tool '{self.tool_name}': {e}"
            )

        # Convert MCP result to clean JSON format for agents
        try:
            # Extract the actual data from MCP response format
            clean_data = self._extract_mcp_data(result)

            # Create agent-friendly JSON response
            response = {
                "tool_name": self.tool_name,
                "parameters": json.loads(self.tool_parameters)
                if self.tool_parameters.strip()
                else {},
                "data": clean_data,
                "data_type": type(clean_data).__name__,
                "record_count": len(clean_data) if isinstance(clean_data, list) else 1,
            }

            # Convert to single-row DataFrame with JSON
            df = pd.DataFrame(
                [{"mcp_response": json.dumps(response, indent=2, default=str)}]
            )

        except Exception as e:
            # Fallback: wrap raw result as JSON
            error_msg = f"Could not process MCP result: {e}"
            LOGGER.warning(error_msg)

            fallback_response = {
                "tool_name": self.tool_name,
                "parameters": self.tool_parameters,
                "raw_result": str(result),
                "error": str(e),
            }

            df = pd.DataFrame(
                [{"mcp_response": json.dumps(fallback_response, indent=2)}]
            )
            exec_context.set_warning(error_msg)

        return knext.Table.from_pandas(df)
