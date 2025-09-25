import pandas as pd
import logging
from typing import Dict, Any, Optional, List
from langchain_openai import AzureChatOpenAI
from config import Config
import base64
import requests
from xml.etree import ElementTree as ET
import re

# Import Oracle DB utilities and oracledb for error handling
import oracle_db_utils
import oracledb

# Configure logging with file output
logging.basicConfig(
    level=logging.DEBUG,
    filename="chatbot.log",
    filemode="a",
    format="%(asctime)s:%(levelname)s:%(name)s:%(message)s"
)
logger = logging.getLogger("query_tools")
# Ensure console output
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.DEBUG)
logger.addHandler(console_handler)

class ContextMatcher:
    def __init__(self):
        self.llm = AzureChatOpenAI(
            azure_endpoint=Config.AZURE_OPENAI_ENDPOINT,
            api_key=Config.AZURE_OPENAI_KEY,
            api_version="2024-08-01-preview",
            deployment_name="gpt-35-turbo"
        )
        logger.info("ContextMatcher initialized")

    def get_contexts(self, agent_type: str) -> List[Dict[str, Any]]:
        logger.info(f"Fetching contexts for agent_type: {agent_type} from Oracle DB")
        conn = None
        try:
            conn = oracle_db_utils.get_oracle_connection()
            cursor = conn.cursor()
            cursor.execute("SELECT ID, CONTEXT FROM QUERY_CONTEXTS WHERE AGENT_TYPE = :agent_type", agent_type=agent_type)
            contexts = [{"id": row[0], "context": row[1]} for row in cursor.fetchall()]
            logger.debug(f"Fetched {len(contexts)} contexts: {contexts}")
            return contexts
        except oracledb.Error as e:
            error_obj, = e.args
            logger.error(f"Oracle DB error fetching contexts for agent_type {agent_type}: {error_obj.message}", exc_info=True)
            return []
        except ConnectionError as e:
            logger.error(f"Connection error fetching contexts: {str(e)}", exc_info=True)
            return []
        except Exception as e:
            logger.error(f"Unexpected error fetching contexts: {str(e)}", exc_info=True)
            return []
        finally:
            if conn:
                oracle_db_utils.release_oracle_connection(conn)

    def match_context(self, question: str, agent_type: str) -> Optional[int]:
        logger.info(f"Matching context for question: {question}, agent_type: {agent_type}")
        contexts = self.get_contexts(agent_type)
        if not contexts:
            logger.warning(f"No contexts found for agent_type: {agent_type}")
            return None

        # MODIFIED context_list formatting for clarity
        context_list = "\n".join([
            f"Item {i+1}: (Database ID: {ctx['id']}) {ctx['context']}" for i, ctx in enumerate(contexts)
        ])
        # Example: "Item 1: (Database ID: 3) Keywords: employee, person..."

        # MODIFIED prompt instructions
        prompt = f"""
        You are an expert at matching user questions to predefined query contexts. Given the user’s question and a list of contexts, select the context that best matches the question based on keywords and intent.

        User Question: {question}

        Available Contexts:
        {context_list}

        Instructions:
        - Each context is listed with an "Item" number and its unique "Database ID".
        - Compare the question’s keywords and intent to the contexts provided.
        - Your task is to return only the 'Database ID' of the best-matching context.
        - For example, if the best matching context is 'Item 1: (Database ID: 3) ...', you MUST return "3".
        - If no context matches the question, return the word "none".
        - Ensure your response is just the single 'Database ID' number or the word "none", with no other text or prefixes.
        """
        logger.debug(f"Context matching prompt: {prompt}") # Increased length for better debug view
        try:
            response = self.llm.invoke(prompt)
            response_text = response.content.strip()
            logger.debug(f"Raw LLM response for context ID: {response_text}")

            if response_text.lower() == "none":
                logger.info(f"LLM indicated no matching context found for question: {question}")
                return None
            
            match = re.search(r'\d+', response_text)
            if not match:
                logger.warning(f"No valid numeric context ID found in LLM response: {response_text}")
                return None
            
            extracted_id_str = match.group()
            
            try:
                matched_id_int = int(extracted_id_str)
            except ValueError:
                logger.warning(f"Could not convert extracted ID '{extracted_id_str}' to an integer. LLM response: {response_text}")
                return None

            # Verify if the matched ID actually exists in the contexts
            if not any(ctx['id'] == matched_id_int for ctx in contexts):
                logger.warning(f"LLM returned Database ID {matched_id_int} which does not exist in the available contexts: {[c['id'] for c in contexts]}. Question: {question}")
                return None

            logger.info(f"Selected Database ID: {matched_id_int}")
            logger.debug(f"Matched context details: {[ctx for ctx in contexts if ctx['id'] == matched_id_int]}")
            return matched_id_int
        except Exception as e:
            logger.error(f"Error matching context for question '{question}': {str(e)}", exc_info=True)
            return None

    def get_query_by_id(self, context_id: int) -> Optional[str]:
        logger.info(f"Fetching query for context_id: {context_id} from Oracle DB")
        conn = None
        try:
            conn = oracle_db_utils.get_oracle_connection()
            cursor = conn.cursor()
            cursor.execute("SELECT QUERY FROM QUERY_CONTEXTS WHERE ID = :context_id", context_id=context_id)
            result = cursor.fetchone()
            if result:
                # Read the content from the LOB object
                query_content = result[0].read() if isinstance(result[0], oracledb.LOB) else result[0]
                logger.debug(f"Retrieved query: {query_content[:100]}...")
                return query_content
            logger.warning(f"No query found for context_id: {context_id}")
            return None
        except oracledb.Error as e:
            error_obj, = e.args
            logger.error(f"Oracle DB error fetching query for context_id {context_id}: {error_obj.message}", exc_info=True)
            return None
        except ConnectionError as e:
            logger.error(f"Connection error fetching query: {str(e)}", exc_info=True)
            return None
        except Exception as e:
            logger.error(f"Unexpected error fetching query: {str(e)}", exc_info=True)
            return None
        finally:
            if conn:
                oracle_db_utils.release_oracle_connection(conn)

class BaseQueryTools:
    def __init__(self):
        self.llm = AzureChatOpenAI(
            azure_endpoint=Config.AZURE_OPENAI_ENDPOINT,
            api_key=Config.AZURE_OPENAI_KEY,
            api_version="2024-08-01-preview",
            deployment_name="gpt-35-turbo"
        )
        logger.info("BaseQueryTools initialized")

    def modify_query_based_on_input(self, user_input: str, base_query: str, prompt_template: str, columns: Dict[str, str]) -> Dict[str, Any]:
        logger.info(f"Modifying query based on user input: {user_input}")
        logger.debug(f"Base query: {base_query}")
        try:
            prompt = prompt_template.format(
                user_input=user_input,
                original_query=base_query,
                columns=columns
            )
            logger.debug(f"Query modification prompt: {prompt[:200]}...")
            response = self.llm.invoke(prompt)
            modified_query = response.content.strip()
            logger.info("Modified query created")
            logger.debug(f"Modified query: {modified_query}")
            return {
                "success": True,
                "modified_query": modified_query
            }
        except Exception as e:
            logger.error(f"Error in modify_query_based_on_input: {str(e)}", exc_info=True)
            return {
                "success": False,
                "error": str(e),
                "modified_query": None
            }

    def generate_sql(self, user_input: str, base_query: str, prompt_template: str, columns: Dict[str, str]) -> str:
        logger.info("Generating SQL query")
        result = self.modify_query_based_on_input(user_input, base_query, prompt_template, columns)
        if not result.get("success"):
            logger.error(f"Failed to generate SQL: {result.get('error')}")
            raise Exception(result.get("error"))
        return result.get("modified_query")

class SCMQueryTools(BaseQueryTools):
    def __init__(self):
        super().__init__()
        self.prompt_template = """
        You are an expert at translating natural language questions into SQL queries for supply chain management.
        Based on the following conversation history, modify the base query to answer the latest user question.

        Conversation History:
        {user_input}

        Base Query:
        ```sql
        {original_query}
        ```
        The columns in this base query include:
        {columns}

        Modify the query to answer the latest user question in the conversation history.
        The query should return only the data needed to answer the question.
        Add appropriate filters, grouping, or sorting as needed.
        For inventory queries, include columns like:
        - Item Number
        - Item Description
        - Quantity On Hand (SUM aggregated)
        - Primary UOM
        - Subinventory Code
        - Organization Code
        - Locator (if mentioned)
        - Secondary Quantity On Hand (SUM aggregated, if mentioned)
        - Secondary UOM (if mentioned)
        For purchase order queries, include columns like:
        - PO Number
        - Vendor Name
        - Quantity Ordered
        - Quantity Received
        - Quantity Open
        - Order Status
        - ASN (if mentioned)
        - Promised Date (if mentioned)

        Return ONLY the modified SQL query without any explanations or comments.
        """
        self.columns = {
            "Item Number": "esi.item_number",
            "Organization Code": "iop.organization_code",
            "Quantity On Hand": "ioqd.transaction_quantity",
            "Primary UOM": "ioqd.transaction_uom_code",
            "Secondary Quantity On Hand": "ioqd.secondary_transaction_quantity",
            "Secondary UOM": "ioqd.secondary_uom_code",
            "Item Description": "esi.description",
            "Subinventory Code": "ioqd.subinventory_code",
            "Locator": "iil.segment1 || '.' || iil.segment2 || '.' || iil.segment3",
            "PO Number": "pha.segment1",
            "Vendor Name": "psv.vendor_name",
            "Quantity Ordered": "pda.quantity_ordered",
            "Quantity Received": "pda.quantity_delivered",
            "Quantity Open": "nvl(pda.quantity_ordered - pda.quantity_delivered, 0)",
            "Order Status": "CASE WHEN nvl(plla.promised_date, plla.need_by_date) >= trunc(sysdate) THEN 'Due In' ELSE 'Late' END",
            "ASN": "ad.asn",
            "Promised Date": "nvl(plla.promised_date, plla.need_by_date)"
        }
        logger.info("SCMQueryTools initialized")

    def generate_sql(self, user_input: str, base_query: str) -> str:
        logger.info(f"SCMQueryTools: Generating SQL for input: {user_input}")
        logger.debug(f"SCMQueryTools: Base query: {base_query[:100]}...")
        result = super().generate_sql(user_input, base_query, self.prompt_template, self.columns)
        logger.info("SCMQueryTools: SQL generation completed")
        logger.debug(f"SCMQueryTools: Generated SQL: {result[:100]}...")
        return result

class HCMQueryTools(BaseQueryTools):
    def __init__(self):
        super().__init__()
        # REVISED PROMPT TEMPLATE
        self.prompt_template = """
        You are an expert SQL developer specializing in Oracle HCM queries. Your task is to modify a given Base HCM Query based on a user's conversation history to answer their latest question.

        Conversation History:
        {user_input}

        Base HCM Query (This is the foundation, use its tables and aliases):
        ```sql
        {original_query}
        ```

        Available Columns and their SQL Expressions (use these exact SQL expressions for the concepts listed):
        The following dictionary maps conceptual column names (like "Employee Name") to their precise SQL expressions (like "ppnf.display_name"). You MUST use these SQL expressions when the user asks for these concepts. The table aliases (e.g., papf, ppnf) used in these expressions are expected to be defined in the 'Base HCM Query'.
        {columns}

        IMPORTANT INSTRUCTIONS FOR MODIFYING THE QUERY:
        1.  Foundation is the 'Base HCM Query': The 'Base HCM Query' provides the necessary tables and joins. Your primary task is to modify the SELECT list, WHERE clause, GROUP BY clause, and ORDER BY clause of this 'Base HCM Query'.
        2.  Strictly Use Provided SQL Expressions: When the user's question refers to conceptual columns (e.g., "Person ID", "Employee Name"), you MUST use the exact SQL expression provided for that concept in the 'Available Columns and their SQL Expressions' mapping. These expressions correctly reference tables and columns (e.g., `ppnf.display_name`).
        3.  No Invention of Columns or Tables: You MUST NOT invent new table names, column aliases, or field names. All tables and columns used MUST either be:
            a.  Directly present in the 'Base HCM Query' (using the aliases defined there).
            b.  Part of an SQL expression explicitly defined in the 'Available Columns and their SQL Expressions' mapping (these expressions also use aliases expected to be in the 'Base HCM Query').
        4.  Column-Table Integrity (VERY IMPORTANT):
            a.  For any column you use in any part of the query (SELECT, WHERE, JOIN, GROUP BY, ORDER BY, e.g., `table_alias.column_name`), the `table_alias` MUST be currently defined in the FROM clause of your query.
            b.  Crucially, the `column_name` MUST genuinely exist in the table referred to by `table_alias`. Do NOT assume a column exists for a table if it's not a standard column for that table or if its presence isn't confirmed by the 'Base HCM Query' structure or the 'Available Columns' mapping. For example, `name_type` is typically in `per_person_names_f` (often aliased `ppnf`), not `per_all_people_f` (aliased `papf`).
        5.  Filtering (WHERE clause Construction):
            a.  Add or modify WHERE clause conditions based on the user's question and conversation history.
            b.  ALL columns referenced in WHERE clause conditions MUST strictly adhere to Instruction #3 (No Invention) and, most importantly, Instruction #4 (Column-Table Integrity).
            c.  If you are adapting or retaining conditions from the 'Base HCM Query', double-check that these conditions are still valid and correctly refer to table aliases that are present in your *current modified* query's FROM clause. If a table has been removed from the FROM clause during modification (e.g., for simplification), YOU MUST REMOVE any WHERE clause conditions that refer to that removed table. Do not reassign such conditions to other tables incorrectly.
        6.  SELECT Clause Modification: Adjust the SELECT clause to return only the specific data fields required. All selected fields/expressions must adhere to Instructions #2, #3, and #4.
        7.  Aggregations: If the question asks for an aggregation (e.g., "how many employees"), use appropriate SQL aggregate functions (like `COUNT(papf.person_id)`). The columns used within the aggregate must adhere to Instruction #4. Ensure that any accompanying WHERE clauses also strictly follow Instruction #5.
        8.  Example Relevant Conceptual Columns (Always use the 'Available Columns' mapping for their SQL expressions):
            - Person ID
            - Employee Number
            - Employee Name
            - Assignment Number
            - Assignment Status
            - Organization Name
            - Position Name
            - Location Name (if mentioned by the user)
            - Manager Name (if mentioned by the user)
            - Grade Name (if mentioned by the user)
            - Date of Birth (if mentioned by the user)
            - Hire Date (if mentioned by the user)
            - Contract End Date (if mentioned by the user)
            - Primary Email (if mentioned by the user)

        Return ONLY the modified SQL query. Do not include any explanations, comments, or markdown tags like "```sql".
        """
        self.columns = {
            "Person ID": "papf.person_id", #
            "Employee Number": "papf.person_number", #
            "Employee Name": "ppnf.display_name", #
            "Assignment Number": "paam.assignment_number", #
            "Assignment Status": "paam.assignment_status_type", #
            "Organization Name": "haou.name", #
            "Position Name": "hp.name", #
            "Location Name": "hl.location_code", #
            "Manager Name": "ppnf.full_name", # This assumes the base query has appropriate joins for manager name resolution using ppnf alias, or this needs careful handling in base query design.
            "Grade Name": "pg.name", #
            "Date of Birth": "pp.date_of_birth", #
            "Hire Date": "ppos.date_start", #
            "Contract End Date": "pcf.contract_end_date", #
            "Primary Email": "pea.email_address" #
        }
        logger.info("HCMQueryTools initialized") #

    def generate_sql(self, user_input: str, base_query: str) -> str:
        logger.info(f"HCMQueryTools: Generating SQL for input: {user_input}") #
        logger.debug(f"HCMQueryTools: Base query: {base_query[:100]}...") #
        result = super().generate_sql(user_input, base_query, self.prompt_template, self.columns) #
        logger.info("HCMQueryTools: SQL generation completed") #
        logger.debug(f"HCMQueryTools: Generated SQL: {result[:100]}...") #
        return result

class OracleBIPTool:
    def __init__(self, endpoint_url: Optional[str] = None):
        self.endpoint_url = Config.ORACLE_BIP_ENDPOINT
        logger.info("OracleBIPTool initialized")

    def execute_query(self, query: str) -> str:
        logger.info("OracleBIPTool: Encoding query for execution")
        try:
            clean_query = query.replace("```", "").strip()
            if clean_query.endswith(";"):
                clean_query = clean_query.rstrip(";").strip()
            if clean_query.lower().startswith("sql"): # Handle 'sql' prefix case-insensitively
                clean_query = clean_query[3:].strip()
            
            # Replace 'sysdate' with 'SYSDATE' for consistency with Oracle
            # Replace 'fnd_global.timezone' with a fixed timezone like 'UTC' if it's not directly available in the environment
            # or ensure fnd_global.timezone is properly handled by BIP.
            # For simplicity, replacing fnd_global.timezone with 'UTC' in the provided queries,
            # as it's common in BIP queries for consistent timestamps.
            clean_query = clean_query.replace("sysdate", "SYSDATE")
            clean_query = clean_query.replace("fnd_global.timezone", "'UTC'")


            logger.debug(f"OracleBIPTool: Cleaned SQL query for BIP: {clean_query[:100]}...")
            encoded_query = base64.b64encode(clean_query.encode("utf-8")).decode("utf-8")
            logger.debug(f"OracleBIPTool: Base64 encoded query: {encoded_query}")
            xml_payload = f"""
   <soap:Envelope xmlns:soap="http://www.w3.org/2003/05/soap-envelope" xmlns:pub="http://xmlns.oracle.com/oxp/service/PublicReportService">
    <soap:Header/>
    <soap:Body>
        <pub:runReport>
        <pub:reportRequest>
            <pub:attributeFormat>csv</pub:attributeFormat>
            <pub:flattenXML>false</pub:flattenXML>
            <pub:parameterNameValues>
            <pub:item>
                <pub:name>query1</pub:name>
                <pub:values>
                <pub:item>{encoded_query}</pub:item>
                </pub:values>
            </pub:item>
            </pub:parameterNameValues>
            <pub:reportAbsolutePath>/Custom/SCM AI Agent/SQLConnectReportCSV.xdo</pub:reportAbsolutePath>
            <pub:sizeOfDataChunkDownload>-1</pub:sizeOfDataChunkDownload>
        </pub:reportRequest>
        </pub:runReport>
    </soap:Body>
    </soap:Envelope>
            """.strip()
            logger.debug(f"OracleBIPTool: SOAP Request Payload: {xml_payload[:200]}...")
            headers = {
                "Content-Type": "application/soap+xml;charset=UTF-8",
                "SOAPAction": "runReport"
            }
            logger.info("OracleBIPTool: Sending SOAP request to Oracle BIP service")
            response = requests.post(
                self.endpoint_url,
                data=xml_payload,
                auth=(Config.ORACLE_FUSION_USER, Config.ORACLE_FUSION_PASS),
                headers=headers,
                timeout=60  # Set a reasonable timeout for the request
            )



            
            logger.info(f"OracleBIPTool: Received response with status code: {response.status_code}")
            print("OracleBIPTool: SOAP Response Text:"+response.text)
            response.raise_for_status()
            logger.info("OracleBIPTool: Parsing XML response from Oracle BIP service")
            root = ET.fromstring(response.text)
            namespaces = {     "env": "http://www.w3.org/2003/05/soap-envelope",     "ns2": "http://xmlns.oracle.com/oxp/service/PublicReportService" }
            report_bytes_elem = root.find(".//ns2:reportBytes", namespaces)
            if report_bytes_elem is None or report_bytes_elem.text is None:
                logger.error("OracleBIPTool: reportBytes element not found or empty in the response")
                raise ValueError("reportBytes element not found or empty in the response from BIP service.")
            base64_data = report_bytes_elem.text
            csv_data = base64.b64decode(base64_data).decode("utf-8")
            logger.info("OracleBIPTool: Successfully decoded CSV data from Oracle BIP response")
            logger.debug(f"OracleBIPTool: CSV Data: {csv_data[:200]}...")
            return csv_data
        except requests.exceptions.RequestException as req_e:
            logger.error(f"OracleBIPTool: HTTP/Request error executing query: {req_e}", exc_info=True)
            raise RuntimeError(f"Failed to connect to Oracle BIP service: {req_e}")
        except ET.ParseError as parse_e:
            logger.error(f"OracleBIPTool: XML parsing error: {parse_e}. Response: {response.text[:500] if 'response' in locals() else 'No response'}", exc_info=True)
            raise RuntimeError(f"Failed to parse BIP response: {parse_e}")
        except Exception as e:
            logger.error(f"OracleBIPTool: Unexpected error executing query: {str(e)}", exc_info=True)
            raise RuntimeError(f"An unexpected error occurred during BIP query execution: {str(e)}")

oracle_bip_tool = OracleBIPTool()