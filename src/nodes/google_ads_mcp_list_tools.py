"""
KNIME node to list available Google Ads MCP tools with descriptions.
"""

import knime.extension as knext
import pandas as pd
from util.common import google_ad_port_type
import logging

LOGGER = logging.getLogger(__name__)


def get_mcp_tools_with_descriptions():
    """
    Get available MCP tools from the Google Ads MCP server with their descriptions.
    """
    try:
        # Import the MCP coordinator to inspect available tools
        from ads_mcp.coordinator import mcp
        
        # Try to get tools and their metadata
        tools_info = []
        
        if hasattr(mcp, 'app') and hasattr(mcp.app, '_tools'):
            tools_dict = mcp.app._tools
            
            for tool_name, tool_info in tools_dict.items():
                # Extract tool information
                description = ""
                parameters = []
                
                # Try to get description from the tool function
                if hasattr(tool_info, 'func') and hasattr(tool_info.func, '__doc__'):
                    description = tool_info.func.__doc__ or ""
                elif hasattr(tool_info, '__doc__'):
                    description = tool_info.__doc__ or ""
                
                # Try to get parameters information
                if hasattr(tool_info, 'func'):
                    func = tool_info.func
                    import inspect
                    sig = inspect.signature(func)
                    parameters = [
                        {
                            "name": param.name,
                            "type": str(param.annotation) if param.annotation != inspect.Parameter.empty else "Any",
                            "required": param.default == inspect.Parameter.empty
                        }
                        for param in sig.parameters.values()
                    ]
                
                tools_info.append({
                    "tool_name": tool_name,
                    "description": description.strip(),
                    "parameters": str(parameters)
                })
        
        else:
            # Fallback: return known tools with descriptions
            tools_info = [
                {
                    "tool_name": "list_accessible_customers",
                    "description": "Returns ids of customers directly accessible by the user authenticating the call.",
                    "parameters": "[]"
                },
                {
                    "tool_name": "search",
                    "description": "Executes GAQL queries to retrieve Google Ads data. Supports searching campaigns, ad groups, keywords, and other resources.",
                    "parameters": "[{\"name\": \"customer_id\", \"type\": \"str\", \"required\": true}, {\"name\": \"fields\", \"type\": \"list\", \"required\": true}, {\"name\": \"resource\", \"type\": \"str\", \"required\": true}, {\"name\": \"conditions\", \"type\": \"list\", \"required\": false}]"
                }
            ]
        
        LOGGER.info(f"Found {len(tools_info)} MCP tools")
        return tools_info
        
    except ImportError as e:
        LOGGER.error(f"Could not import MCP tools: {e}")
        return [{
            "tool_name": "error",
            "description": f"MCP tools not available: {e}",
            "parameters": "[]"
        }]
    
    except Exception as e:
        LOGGER.error(f"Error getting MCP tools: {e}")
        return [{
            "tool_name": "error", 
            "description": f"Error listing tools: {e}",
            "parameters": "[]"
        }]


@knext.node(
    name="Google Ads MCP Tools List",
    node_type=knext.NodeType.SOURCE,
    icon_path="icons/gads-icon.png",
    category="Google Ads",
    keywords=["Google", "Google Ads", "MCP", "Tools", "List"],
)
@knext.input_port(
    name="Google Ads Connection", 
    description="Google Ads connection with authentication details", 
    port_type=google_ad_port_type
)
@knext.output_table(name="MCP Tools", description="Available MCP tools with descriptions")
class GoogleAdsMCPToolsList:
    """
    This node lists all available Google Ads MCP tools with their descriptions and parameters.
    
    Purpose: This node discovers what MCP tools are available from the Google Ads MCP server.
    The output can be used by AI agents or chat systems to understand what operations are possible and how to call them.
    
    Output: Returns a table with tool_name, description, and parameters columns.
    
    Usage in MCP Workflow:
    1. Use this node to discover available tools
    2. Pass the tool list to an AI agent or chat system  
    3. The agent selects which tool to call based on user query
    4. Use the MCP Tools Executor node to run the selected tool
    
    Available Tools: Typical tools from Google Ads MCP server include list_accessible_customers and search tools.
    """

    def configure(self, configuration_context, input_spec):
        return None

    def execute(self, exec_context, google_ads_connection):
        try:
            LOGGER.info("Discovering available MCP tools...")
            
            # Get MCP tools with descriptions
            tools_info = get_mcp_tools_with_descriptions()
            
            # Create DataFrame
            df = pd.DataFrame(tools_info)
            
            # Add connection context
            if google_ads_connection:
                df['customer_id'] = google_ads_connection.spec.account_id
                df['connection_available'] = True
            else:
                df['customer_id'] = None
                df['connection_available'] = False
            
            LOGGER.info(f"Found {len(df)} MCP tools")
            
            return knext.Table.from_pandas(df)
            
        except Exception as e:
            LOGGER.error(f"Error listing MCP tools: {e}")
            # Return error info as a table
            error_df = pd.DataFrame([{
                "tool_name": "error",
                "description": f"Failed to list MCP tools: {e}",
                "parameters": "[]",
                "customer_id": google_ads_connection.spec.account_id if google_ads_connection else None,
                "connection_available": google_ads_connection is not None
            }])
            return knext.Table.from_pandas(error_df)