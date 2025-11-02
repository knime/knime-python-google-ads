"""
KNIME node for natural language queries to Google Ads API using authenticated client.
"""

import knime.extension as knext
import pandas as pd
import json
from util.common import google_ad_port_type
import logging

LOGGER = logging.getLogger(__name__)


def get_mcp_tools():
    """
    Fetch available tools from the MCP server by inspecting the registered tools.
    """
    try:
        # Try to import and inspect the MCP coordinator to see registered tools
        from ads_mcp.coordinator import mcp
        
        # Get the tools registered with the MCP server
        # The FastMCP instance should have a way to list tools
        if hasattr(mcp, '_tools') or hasattr(mcp, 'tools'):
            tools_dict = getattr(mcp, '_tools', {}) or getattr(mcp, 'tools', {})
            tool_names = list(tools_dict.keys())
            LOGGER.info(f"Found MCP tools: {tool_names}")
            return tool_names
        
        # Fallback: try to get tools from the app if available
        if hasattr(mcp, 'app') and hasattr(mcp.app, '_tools'):
            tools_dict = mcp.app._tools
            tool_names = list(tools_dict.keys())
            LOGGER.info(f"Found MCP tools via app: {tool_names}")
            return tool_names
            
        # If we can't find the tools, return known ones from the MCP server
        LOGGER.warning("Could not inspect MCP tools, returning known tools")
        return ["list_accessible_customers", "search"]
        
    except ImportError as e:
        LOGGER.error(f"Could not import MCP coordinator: {e}")
        return ["list_accessible_customers", "search"]
    
    except Exception as e:
        LOGGER.error(f"Error getting MCP tools: {e}")
        return ["list_accessible_customers", "search"]


def call_mcp_tool(tool_name, parameters, client):
    """
    Call a specific MCP tool using our authenticated client.
    """
    try:
        LOGGER.info(f"Calling MCP tool: {tool_name} with params: {parameters}")
        
        if tool_name == "list_accessible_customers":
            # Call the list_accessible_customers functionality
            service = client.get_service("CustomerService")
            accessible_customers = service.list_accessible_customers()
            
            customer_ids = [
                cust_rn.removeprefix("customers/")
                for cust_rn in accessible_customers.resource_names
            ]
            
            return customer_ids
            
        elif tool_name == "search":
            # Call the search functionality  
            service = client.get_service("GoogleAdsService")
            customer_id = parameters.get("customer_id")
            fields = parameters.get("fields", ["campaign.id", "campaign.name"])
            resource = parameters.get("resource", "campaign")
            conditions = parameters.get("conditions", [])
            
            # Build GAQL query
            query_parts = [f"SELECT {', '.join(fields)}"]
            query_parts.append(f"FROM {resource}")
            
            if conditions:
                query_parts.append(f"WHERE {' AND '.join(conditions)}")
                
            query_parts.append("ORDER BY campaign.name")
            gaql_query = " ".join(query_parts)
            
            LOGGER.info(f"Executing GAQL: {gaql_query}")
            
            results = []
            response = service.search_stream(customer_id=str(customer_id), query=gaql_query)
            
            for batch in response:
                for row in batch.results:
                    row_data = {}
                    for field in fields:
                        # Navigate nested attributes
                        field_parts = field.split(".")
                        value = row
                        for part in field_parts:
                            if part == "type":
                                part = part + "_"
                            value = getattr(value, part)
                        row_data[field] = value
                    results.append(row_data)
            
            LOGGER.info(f"Search returned {len(results)} results")
            return results
            
        else:
            return {"error": f"Unknown MCP tool: {tool_name}"}
            
    except Exception as e:
        LOGGER.error(f"Error calling MCP tool {tool_name}: {e}")
        return {"error": str(e), "tool": tool_name, "parameters": parameters}


def process_natural_language_query(natural_language_query, google_ads_connection=None):
    """
    Bridge to MCP tools - imports MCP server tools directly and injects our authenticated client.
    """
    if not google_ads_connection:
        raise RuntimeError("Google Ads connection required")
    
    try:
        # Use our authenticated client to directly call what the MCP tools do
        # This bypasses the MCP utils client creation entirely
        
        client = google_ads_connection.client
        customer_id = google_ads_connection.spec.account_id
        
        # Process the natural language query and call the appropriate MCP tool
        LOGGER.info(f"MCP Bridge processing query: '{natural_language_query}'")
        
        # Get available tools 
        available_tools = get_mcp_tools()
        LOGGER.info(f"Available MCP tools: {available_tools}")
        
        # Analyze the query and call the appropriate MCP tool
        query_lower = natural_language_query.lower()
        
        if any(keyword in query_lower for keyword in ['customers', 'accounts', 'accessible']):
            # Call list_accessible_customers tool
            return call_mcp_tool("list_accessible_customers", {}, client)
            
        elif any(keyword in query_lower for keyword in ['campaign', 'campaigns', 'ad group', 'keyword', 'search']):
            # Call search tool with appropriate parameters
            if 'campaign' in query_lower:
                # Search for campaigns
                search_params = {
                    "customer_id": customer_id,
                    "fields": ["campaign.id", "campaign.name", "campaign.status"],
                    "resource": "campaign"
                }
                
                if 'active' in query_lower:
                    search_params["conditions"] = ["campaign.status = 'ENABLED'"]
                    
                return call_mcp_tool("search", search_params, client)
                
        # Default: return available tools info
        return {
            "query": natural_language_query,
            "customer_id": customer_id,
            "available_tools": available_tools,
            "message": "Query not recognized - showing available tools"
        }
            
    except ImportError as e:
        LOGGER.error(f"Could not import MCP tools: {e}")
        return {
            "error": "MCP tools not available",
            "message": f"Could not import MCP server tools: {e}",
            "help": "Ensure google-ads-mcp package is properly installed"
        }
    except Exception as e:
        LOGGER.error(f"Error using MCP tools: {e}")
        return {
            "error": "MCP tool execution failed",
            "message": str(e),
            "query": natural_language_query
        }





def run_mcp_query_sync(natural_language_query, google_ads_connection=None):
    """Direct call to MCP bridge - no longer async."""
    return process_natural_language_query(natural_language_query, google_ads_connection)


@knext.node(
    name="Google Ads MCP Tools Executor",
    node_type=knext.NodeType.MANIPULATOR,
    icon_path="icons/gads-icon.png",
    category="Google Ads",
    keywords=["Google", "Google Ads", "MCP", "Tools", "Execute"],
)
@knext.input_port(
    name="Google Ads Connection", 
    description="Google Ads connection with authentication details", 
    port_type=google_ad_port_type
)
@knext.output_table(name="Output Data", description="KNIME table with MCP tool results")
class GoogleAdsMCPToolsExecutor:
    """
    Executes a specific Google Ads MCP tool selected by an AI agent.
    
    MCP Workflow Architecture: This node is part of a 3-step MCP workflow.
    1. MCP Tools List discovers available tools and descriptions
    2. AI Agent/Chat takes user query plus tool list, selects appropriate tool  
    3. MCP Tools Executor (this node) executes the selected tool
    
    Input Parameters:
    - Tool Name: The specific MCP tool to execute (e.g., search, list_accessible_customers)
    - Tool Parameters: JSON parameters for the tool
    
    Supported Tools:
    - list_accessible_customers: No parameters needed
    - search: Requires customer_id, fields, resource, optional conditions
    
    Usage:
    1. Connect Google Ads Connector for authentication
    2. Set Tool Name (selected by agent) 
    3. Set Tool Parameters as JSON (prepared by agent)
    4. Execute to get structured results
    """

    tool_name = knext.StringParameter(
        label="Tool Name",
        description="Name of the MCP tool to execute (e.g., 'search', 'list_accessible_customers')",
        default_value="list_accessible_customers"
    )
    
    tool_parameters = knext.MultilineStringParameter(
        label="Tool Parameters (JSON)",
        description="Parameters for the MCP tool in JSON format. Use {} for tools that don't need parameters.",
        default_value="{}",
        number_of_lines=5,
    )

    def configure(self, configuration_context, input_spec):
        # Validate that we have a Google Ads connection
        return None

    def execute(self, exec_context, google_ads_connection):
        # Execute the specific MCP tool with parameters
        try:
            LOGGER.info(f"Executing MCP tool: {self.tool_name}")
            
            # Parse tool parameters
            try:
                if self.tool_parameters.strip():
                    parameters = json.loads(self.tool_parameters)
                else:
                    parameters = {}
            except json.JSONDecodeError as e:
                raise knext.InvalidParametersError(f"Invalid JSON in tool parameters: {e}")
            
            # Add customer_id from connection if not provided
            if 'customer_id' not in parameters and google_ads_connection:
                parameters['customer_id'] = google_ads_connection.spec.account_id
            
            # Execute the MCP tool
            result = call_mcp_tool(self.tool_name, parameters, google_ads_connection.client)
                
        except Exception as e:
            raise knext.WorkflowExecutionError(f"Failed to execute MCP tool '{self.tool_name}': {e}")

        # Convert MCP result to DataFrame for KNIME output
        try:
            LOGGER.info(f"MCP result type: {type(result)}, content: {str(result)[:200]}...")
            
            # Handle different MCP result formats
            if isinstance(result, list) and len(result) > 0:
                # If result is a list of content items from MCP
                first_item = result[0]
                if hasattr(first_item, 'text'):
                    # Text content - try to parse as JSON
                    try:
                        parsed_content = json.loads(first_item.text)
                        if isinstance(parsed_content, dict):
                            result = parsed_content
                        else:
                            result = {"content": first_item.text}
                    except json.JSONDecodeError:
                        result = {"content": first_item.text}
                else:
                    result = {"content": str(first_item)}
            
            # Now process the structured result
            if isinstance(result, dict):
                if "customers" in result:
                    # Customer list result
                    customer_ids = result["customers"]
                    df = pd.DataFrame({
                        "customer_id": customer_ids,
                        "accessible": [True] * len(customer_ids)
                    })
                elif "results" in result:
                    # Search results
                    search_results = result["results"]
                    if search_results:
                        df = pd.DataFrame(search_results)
                    else:
                        df = pd.DataFrame({"message": ["No results found for the query"]})
                else:
                    # Generic dictionary result
                    df = pd.DataFrame([result])
            elif isinstance(result, list):
                # List result
                if result:
                    df = pd.DataFrame(result)
                else:
                    df = pd.DataFrame({"message": ["No results returned"]})
            else:
                # Fallback: wrap result in a single column
                df = pd.DataFrame({"result": [str(result)]})
                
        except Exception as e:
            # Final fallback: wrap in a single column with error info
            df = pd.DataFrame({
                "result": [str(result)],
                "error": [f"DataFrame conversion failed: {e}"]
            })
            exec_context.set_warning(f"Could not convert MCP result to proper DataFrame format: {e}")

        return knext.Table.from_pandas(df)
