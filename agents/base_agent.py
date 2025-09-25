# Removed sqlite3 and SqliteSaver imports
from langgraph.graph import StateGraph, END #
# from langgraph.checkpoint.sqlite import SqliteSaver # Removed
from langgraph.graph.message import add_messages #
from typing import TypedDict, Annotated, List, Dict, Optional, Literal #
from langchain_openai import AzureChatOpenAI #
from langchain_core.messages import HumanMessage, AIMessage, BaseMessage #
from tools.base_query_tools import SCMQueryTools, HCMQueryTools, oracle_bip_tool, ContextMatcher #
import logging #
from config import Config #
import pandas as pd #
from io import StringIO, BytesIO #
import base64 #
import requests #
from datetime import datetime #
import uuid #

# Import Oracle DB utilities
import oracle_db_utils #
import oracledb # For Oracle error handling #

# Configure logging with file output
logging.basicConfig( #
    level=logging.DEBUG,  #
    filename="chatbot.log", #
    filemode="a", #
    format="%(asctime)s:%(levelname)s:%(name)s:%(message)s" #
)
logger = logging.getLogger("agent") #
console_handler = logging.StreamHandler() #
console_handler.setLevel(logging.DEBUG) #
logger.addHandler(console_handler) #

class AgentState(TypedDict): #
    messages: Annotated[List[BaseMessage], add_messages] #
    question_type: Optional[str] #
    query: Optional[str] #
    selected_query: Optional[str] #
    error: Optional[str] #
    csv_data: Optional[str] #
    format_preference: Optional[str] #
    agent_type: Optional[str] # This refers to the agent_stream for DB
    attachment: Optional[Dict[str, str]] # ADDED: To hold generated file data

class BaseAgent:
    def __init__(self, query_tools, classification_prompt: str, general_response: str): #
        self.llm = AzureChatOpenAI( #
            azure_endpoint=Config.AZURE_OPENAI_ENDPOINT, #
            api_key=Config.AZURE_OPENAI_KEY, #
            api_version="2024-08-01-preview", #
            deployment_name="gpt-35-turbo" #
        )
        self.query_tools = query_tools #
        self.context_matcher = ContextMatcher() #
        self.oracle_bip_tool = oracle_bip_tool #
        self.classification_prompt = classification_prompt #
        self.general_response = general_response #
        self.graph = self._build_graph().compile() #
        logger.info(f"Initialized {self.__class__.__name__} without persistent graph checkpointer. History managed in Oracle.") #

    def _save_message_to_oracle(self, thread_id: str, sender_role: str, content: str, agent_stream: str) -> Optional[int]: # MODIFIED: to return message ID
        """Saves a single message turn to Oracle and returns the new MESSAGE_ID."""
        conn = None #
        try:
            conn = oracle_db_utils.get_oracle_connection() #
            cursor = conn.cursor() #
            
            # Create a variable to hold the returned ID
            new_id_var = cursor.var(oracledb.NUMBER)
            
            sql = """
                INSERT INTO CHATBOT_CONVERSATION_HISTORY 
                (THREAD_ID, MESSAGE_TIMESTAMP, SENDER_ROLE, MESSAGE_CONTENT, AGENT_STREAM)
                VALUES (:thread_id, CURRENT_TIMESTAMP, :sender_role, :message_content, :agent_stream)
                RETURNING MESSAGE_ID INTO :new_id
            """ #
            logger.debug(f"Saving message to Oracle: Thread ID {thread_id}, Sender Role {sender_role}, Agent Stream {agent_stream}, Content (truncated): {content[:100]}") #
            cursor.execute(sql, thread_id=thread_id, sender_role=sender_role, message_content=content, agent_stream=agent_stream, new_id=new_id_var) #
            
            # Get the returned ID
            message_id = new_id_var.getvalue()[0]
            
            conn.commit() #
            logger.info(f"Message saved to Oracle with MESSAGE_ID: {message_id} for thread_id: {thread_id}") #
            return message_id
        except oracledb.Error as e: #
            error_obj, = e.args #
            logger.error(f"Oracle DB error saving message for thread_id {thread_id}: {error_obj.message}", exc_info=True) #
            return None
        except ConnectionError as e: #
            logger.error(f"Connection error saving message to Oracle: {str(e)}", exc_info=True) #
            return None
        except Exception as e: #
            logger.error(f"Unexpected error saving message to Oracle: {str(e)}", exc_info=True) #
            return None
        finally:
            if conn: #
                oracle_db_utils.release_oracle_connection(conn) #

    def _load_recent_messages_from_oracle(self, thread_id: str, agent_stream: str, limit: int = 20) -> List[BaseMessage]: #
        """Loads the most recent messages for a given thread_id and agent_stream from Oracle."""
        messages: List[BaseMessage] = [] #
        conn = None #
        try:
            conn = oracle_db_utils.get_oracle_connection() #
            cursor = conn.cursor() #
            sql = """
                SELECT SENDER_ROLE, MESSAGE_CONTENT 
                FROM (
                    SELECT SENDER_ROLE, MESSAGE_CONTENT, MESSAGE_TIMESTAMP
                    FROM CHATBOT_CONVERSATION_HISTORY
                    WHERE THREAD_ID = :thread_id AND AGENT_STREAM = :agent_stream
                    ORDER BY MESSAGE_TIMESTAMP DESC
                )
                WHERE ROWNUM <= :limit
            """ #
            cursor.execute(sql, thread_id=thread_id, agent_stream=agent_stream, limit=limit) #
            
            fetched_rows = cursor.fetchall() #
            logger.debug(f"Loaded {len(fetched_rows)} message rows from Oracle for thread {thread_id}, agent_stream {agent_stream}.") #

            for row in reversed(fetched_rows): #
                sender_role, message_content_lob = row #
                if message_content_lob is None: #
                    continue #
                
                content_str = message_content_lob.read() if hasattr(message_content_lob, 'read') else str(message_content_lob) #

                if sender_role == "USER": #
                    messages.append(HumanMessage(content=content_str)) #
                elif sender_role == "AI": #
                    messages.append(AIMessage(content=content_str)) #
            logger.info(f"Reconstructed {len(messages)} messages from Oracle for thread_id: {thread_id}") #
            return messages #
        except oracledb.Error as e: #
            error_obj, = e.args #
            logger.error(f"Oracle DB error loading messages for thread_id {thread_id}: {error_obj.message}", exc_info=True) #
            return [] #
        except ConnectionError as e: #
            logger.error(f"Connection error loading messages from Oracle: {str(e)}", exc_info=True) #
            return [] #
        except Exception as e: #
            logger.error(f"Unexpected error loading messages from Oracle: {str(e)}", exc_info=True) #
            return [] #
        finally:
            if conn: #
                oracle_db_utils.release_oracle_connection(conn) #

    def _build_graph(self) -> StateGraph: #
        logger.info(f"Building LangGraph workflow for {self.__class__.__name__}") #
        workflow = StateGraph(AgentState) #
        workflow.add_node("classify_question", self.classify_question) #
        workflow.add_node("match_context", self.match_context) #
        workflow.add_node("process_query", self.process_query) #
        workflow.add_node("execute_query", self.execute_query) #
        workflow.add_node("format_response", self.format_response) #
        workflow.add_node("answer_general_question", self.answer_general_question) #
        workflow.set_entry_point("classify_question") #
        workflow.add_conditional_edges( #
            "classify_question", #
            self.route_question, #
            {"non-general": "match_context", "general": "answer_general_question"} # Updated "inventory" to "non-general"
        )
        workflow.add_conditional_edges( #
            "match_context", #
            self.route_context, #
            {"process_query": "process_query", "error": "format_response"} #
        )
        workflow.add_edge("process_query", "execute_query") #
        workflow.add_edge("execute_query", "format_response") #
        workflow.add_edge("format_response", END) #
        workflow.add_edge("answer_general_question", END) #
        logger.debug("Built LangGraph workflow with nodes and edges") #
        return workflow #

    def classify_question(self, state: AgentState) -> Dict: #
        messages = state["messages"] #
        latest_message_content = "" #
        if messages and isinstance(messages[-1], BaseMessage): #
             latest_message_content = messages[-1].content #
        elif messages and isinstance(messages[-1], str): #
             latest_message_content = messages[-1] #
        else: #
            logger.warning(f"{self.__class__.__name__}: No valid latest message found for classification. State messages: {messages}") #
        
        agent_stream_from_state = state.get('agent_type')  #

        logger.info(f"{self.__class__.__name__}: Classifying question: {latest_message_content} for agent_stream: {agent_stream_from_state}") #
        try:
            response = self.llm.invoke(self.classification_prompt.format(latest_message=latest_message_content)) #
            question_type = response.content.strip().lower() #
            if question_type not in ["non-general", "general"]: # Updated "inventory" to "non-general"
                logger.warning(f"{self.__class__.__name__}: Invalid question type '{question_type}', defaulting to 'non-general'") # Updated default
                question_type = "non-general" # Updated default
            logger.info(f"{self.__class__.__name__}: Question classified as: {question_type}") #
            return { #
                "question_type": question_type, #
                "format_preference": state.get("format_preference", "natural_language"), #
                "agent_type": agent_stream_from_state #
            }
        except Exception as e: #
            logger.error(f"{self.__class__.__name__}: Error classifying question: {str(e)}", exc_info=True) #
            return { #
                "question_type": "non-general", # Updated default in case of error #
                "error": f"Error classifying question: {str(e)}", #
                "format_preference": state.get("format_preference", "natural_language"), #
                "agent_type": agent_stream_from_state #
            }

    def route_question(self, state: AgentState) -> Literal["non-general", "general"]: # Updated Literal
        question_type = state.get("question_type", "non-general") # Updated default to "non-general"
        logger.debug(f"{self.__class__.__name__}: Routing question to: {question_type}") #
        return question_type #

    def match_context(self, state: AgentState) -> Dict: #
        messages = state["messages"] #
        latest_question_content = "" #
        if messages and isinstance(messages[-1], BaseMessage): #
            latest_question_content = messages[-1].content #
        elif messages and isinstance(messages[-1], str): #
            latest_question_content = messages[-1] #

        agent_stream_from_state = state.get("agent_type", "").lower() # agent_type in state now refers to agent_stream #
        logger.info(f"{self.__class__.__name__}: Matching context for question: {latest_question_content}, agent_stream: {agent_stream_from_state}") #
        
        try:
            contexts = self.context_matcher.get_contexts(agent_stream_from_state) #
            logger.debug(f"{self.__class__.__name__}: Available contexts for {agent_stream_from_state}: {contexts}") #
            if not contexts: #
                logger.warning(f"{self.__class__.__name__}: No contexts available for agent_stream: {agent_stream_from_state}") #
                return {"error": "No contexts found for the specified agent type."} #
            context_id = self.context_matcher.match_context(latest_question_content, agent_stream_from_state) #
            if not context_id: #
                logger.warning(f"{self.__class__.__name__}: No matching context found for question: {latest_question_content}") #
                return {"error": "I couldn't identify the query type. Please clarify your question."} #
            selected_query = self.context_matcher.get_query_by_id(context_id) #
            if not selected_query: #
                logger.error(f"{self.__class__.__name__}: No query found for context_id: {context_id}") #
                return {"error": "Error retrieving query for the matched context."} #
            logger.info(f"{self.__class__.__name__}: Context matched, selected context ID: {context_id}, query starts with: {selected_query[:50]}...") #
            logger.debug(f"{self.__class__.__name__}: Full selected query: {selected_query}") #
            return {"selected_query": selected_query} #
        except Exception as e: #
            logger.error(f"{self.__class__.__name__}: Error matching context: {str(e)}", exc_info=True) #
            return {"error": f"Error matching context: {str(e)}"} #

    def route_context(self, state: AgentState) -> Literal["process_query", "error"]: #
        route = "process_query" if state.get("selected_query") else "error" #
        logger.debug(f"{self.__class__.__name__}: Routing context to: {route}") #
        return route #

    def process_query(self, state: AgentState) -> Dict: #
        messages = state["messages"] #
        conversation_history_str = "\n".join([f"{msg.type}: {msg.content}" for msg in messages if isinstance(msg, BaseMessage)]) #
        
        selected_query = state.get("selected_query") #
        logger.info(f"{self.__class__.__name__}: Processing query based on conversation: {conversation_history_str[:200]}...") #
        if not selected_query: #
            logger.error(f"{self.__class__.__name__}: No query selected to process") #
            return {"error": "No query selected to process."} #
        try:
            logger.debug(f"{self.__class__.__name__}: Base query: {selected_query}") #
            modified_query = self.query_tools.generate_sql(conversation_history_str, selected_query) #
            logger.info(f"{self.__class__.__name__}: Query modified successfully") #
            logger.debug(f"{self.__class__.__name__}: Modified query: {modified_query}") #
            return {"query": modified_query} #
        except Exception as e: #
            logger.error(f"{self.__class__.__name__}: Error processing query: {str(e)}", exc_info=True) #
            return {"error": f"Error processing query: {str(e)}"} #

    def execute_query(self, state: AgentState) -> Dict: #
        if state.get("error") or not state.get("query"): #
            logger.warning(f"{self.__class__.__name__}: Skipping query execution due to error or missing query") #
            return {} #
        query = state.get("query") #
        logger.info(f"{self.__class__.__name__}: Executing query: {query[:100]}...") #
        try:
            csv_data = self.oracle_bip_tool.execute_query(query) #
            logger.info(f"{self.__class__.__name__}: Query executed, CSV data received") #
            logger.debug(f"{self.__class__.__name__}: CSV data: {csv_data[:200]}...") #
            return {"csv_data": csv_data} #
        except Exception as e: #
            logger.error(f"{self.__class__.__name__}: Error executing query: {str(e)}", exc_info=True) #
            return {"error": f"Error executing query: {str(e)}"} #

    def answer_general_question(self, state: AgentState) -> Dict: #
        messages = state["messages"] #
        latest_question_content = messages[-1].content if messages and isinstance(messages[-1], BaseMessage) else "" #
        logger.info(f"{self.__class__.__name__}: Handling general question: {latest_question_content}") #
        logger.debug(f"{self.__class__.__name__}: Responding with general response: {self.general_response[:100]}...") #
        return {"messages": [AIMessage(content=self.general_response)]} #

    def _df_to_markdown(self, df: pd.DataFrame) -> str: #
        logger.debug(f"{self.__class__.__name__}: Converting DataFrame to Markdown table") #
        headers = df.columns.tolist() #
        rows = df.values.tolist() #
        markdown_table = "| " + " | ".join(headers) + " |\n" #
        markdown_table += "| " + " | ".join(["---"] * len(headers)) + " |\n" #
        for row in rows: #
            markdown_table += "| " + " | ".join(str(cell) for cell in row) + " |\n" #
        return markdown_table #

    def _df_to_base64_excel(self, df: pd.DataFrame) -> str: #
        logger.debug(f"{self.__class__.__name__}: Converting DataFrame to base64-encoded Excel") #
        output = BytesIO() #
        with pd.ExcelWriter(output, engine='openpyxl') as writer: #
            df.to_excel(writer, index=False) #
        output.seek(0) #
        return base64.b64encode(output.read()).decode('utf-8') #

    def _generate_natural_language_response(self, user_question: str, df: pd.DataFrame) -> str: #
        logger.info(f"{self.__class__.__name__}: Generating natural language response for question: {user_question}") #
        num_rows = len(df) #
        columns = df.columns.tolist() #
        sample_data = df.head(10).to_string(index=False) #
        data_stats = {} #
        for col in df.columns: #
            if pd.api.types.is_numeric_dtype(df[col]): #
                data_stats[col] = { #
                    "min": df[col].min(), #
                    "max": df[col].max(), #
                    "avg": df[col].mean(), #
                    "sum": df[col].sum() #
                }
            else: #
                unique_values = df[col].nunique() #
                if unique_values <= 10: #
                    value_counts = df[col].value_counts().to_dict() #
                    data_stats[col] = {"unique_values": unique_values, "value_counts": value_counts} #
                else: #
                    data_stats[col] = {"unique_values": unique_values} #
        prompt = f"""
        Based on the data retrieved, answer the user's question with a concise bulleted list in Markdown format.

        User Question: {user_question}

        Data Summary:
        - Total records: {num_rows}
        - Columns: {columns}

        Sample of the data (first 10 rows or less):
        {sample_data}

        Data Statistics:
        {data_stats}

        IMPORTANT INSTRUCTIONS:
        1. Format your entire response as a concise bulleted list using Markdown (using * or - format)
        2. Keep each bullet point short and direct (1-2 lines maximum)
        3. Limit your response to 5-8 bullet points that highlight only the most important information
        4. Include specific numbers and key insights from the data
        5. Start with a brief summary bullet point that directly answers the user's question
        6. If the data shows no results or is empty, explain that clearly in a single bullet point

        DO NOT include any explanatory text outside the bullet points.
        """ #
        response = self.llm.invoke(prompt) #
        response_content = response.content.strip() #
        logger.debug(f"{self.__class__.__name__}: Generated natural language response: {response_content}") #
        return response_content #

    def _save_attachment_to_oracle(self, message_id: int, filename: str, mimetype: str, file_content: bytes) -> Optional[int]:
        """Saves a file to the CHATBOT_ATTACHMENTS table and returns the attachment ID."""
        conn = None
        try:
            conn = oracle_db_utils.get_oracle_connection()
            cursor = conn.cursor()
            
            attachment_id_var = cursor.var(oracledb.NUMBER)
            
            # This SQL is correct
            sql = """
                INSERT INTO CHATBOT_ATTACHMENTS (MESSAGE_ID, FILENAME, MIMETYPE, FILE_CONTENT)
                VALUES (:msg_id, :fname, :mtype, :content)
                RETURNING ATTACHMENT_ID INTO :att_id
            """
            
            # REFINED LOGIC: Explicitly create a LOB object for the file content.
            # This is the most reliable way to handle BLOBs.
            file_blob = conn.createlob(oracledb.DB_TYPE_BLOB)
            file_blob.write(file_content)
            
            # Pass the LOB object in the execute call, not the raw bytes.
            cursor.execute(sql, msg_id=message_id, fname=filename, mtype=mimetype, content=file_blob, att_id=attachment_id_var)
            
            attachment_id = attachment_id_var.getvalue()[0]
            conn.commit()
            logger.info(f"Saved attachment to Oracle with ATTACHMENT_ID: {attachment_id} for MESSAGE_ID: {message_id}")
            return attachment_id
        except oracledb.Error as e:
            error_obj, = e.args
            logger.error(f"Oracle DB error saving attachment for MESSAGE_ID {message_id}: {error_obj.message}", exc_info=True)
            return None
        except Exception as e:
            logger.error(f"Unexpected error saving attachment: {str(e)}", exc_info=True)
            return None
        finally:
            if conn:
                oracle_db_utils.release_oracle_connection(conn)

    def _update_message_content(self, message_id: int, new_content: str):
        """Updates the content of an existing message in the history table."""
        conn = None
        try:
            conn = oracle_db_utils.get_oracle_connection()
            cursor = conn.cursor()
            sql = "UPDATE CHATBOT_CONVERSATION_HISTORY SET MESSAGE_CONTENT = :content WHERE MESSAGE_ID = :msg_id"
            cursor.execute(sql, content=new_content, msg_id=message_id)
            conn.commit()
            logger.info(f"Updated message content for MESSAGE_ID: {message_id}")
        except oracledb.Error as e:
            error_obj, = e.args
            logger.error(f"Oracle DB error updating message {message_id}: {error_obj.message}", exc_info=True)
        finally:
            if conn:
                oracle_db_utils.release_oracle_connection(conn)

    def _get_download_link(self, attachment_id: int) -> str:
        """Generates a download link for an attachment stored in the database."""
        logger.debug(f"{self.__class__.__name__}: Generating DB download link for attachment_id: {attachment_id}")
        base_url = Config.BASE_URL # This should be the URL of your FastAPI server
        download_link = f"{base_url}/download/attachment/{attachment_id}"
        logger.debug(f"{self.__class__.__name__}: Generated download link: {download_link}")
        return download_link

    def format_response(self, state: AgentState) -> Dict: #
        user_question_content = "" #
        if state["messages"] and isinstance(state["messages"][-1], BaseMessage): #
            user_question_content = state["messages"][-1].content #
        elif state["messages"] and isinstance(state["messages"][-1], str): #
             user_question_content = state["messages"][-1] #

        format_preference = state.get("format_preference", "natural_language") #
        logger.info(f"{self.__class__.__name__}: Formatting response for question: {user_question_content}, format_preference: {format_preference}") #
        
        attachment_data = None

        try:
            if state.get("error"): #
                error_message = f"Error processing your question: {state['error']}" #
                if format_preference == "natural_language": #
                    error_message = f"* {error_message}" #
                logger.error(f"{self.__class__.__name__}: Formatting error response: {error_message}") #
                response = AIMessage(content=error_message) #
                return {"messages": [response], "error": state.get("error")} #
            
            csv_data = state.get("csv_data") #
            if not csv_data: #
                no_data_message = "I wasn't able to retrieve data to answer your question." #
                if format_preference == "natural_language": #
                    no_data_message = f"* {no_data_message}" #
                logger.warning(f"{self.__class__.__name__}: No CSV data to format") #
                response = AIMessage(content=no_data_message) #
                return {"messages": [response]} #
            
            df = pd.read_csv(StringIO(csv_data)) #
            num_rows = len(df) #
            logger.info(f"{self.__class__.__name__}: Formatting {num_rows} rows of data") #
            
            response_content = "" #

            DOWNLOAD_LINK_PLACEHOLDER = "[DOWNLOAD_LINK_PLACEHOLDER]"

            if format_preference == "natural_language": #
                response_content = self._generate_natural_language_response(user_question_content, df) #
                if num_rows > 10: #
                    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S") #
                    filename = f"Data_{timestamp}.xlsx" #
                    base64_data = self._df_to_base64_excel(df) #
                    attachment_data = {"filename": filename, "base64_data": base64_data}
                    
                    if not response_content.endswith("\n"): #
                        response_content += "\n" #
                    response_content += f"* {DOWNLOAD_LINK_PLACEHOLDER}" # NOTE: Removed record count from here
            else: # Table format #
                if num_rows <= 10: # MODIFIED: <= 10 for consistency
                    markdown_table = self._df_to_markdown(df) #
                    response_content = f"Here's the data for your question: \"{user_question_content}\"\n\n{markdown_table}" #
                else: #
                    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S") #
                    filename = f"Data_{timestamp}.xlsx" #
                    base64_data = self._df_to_base64_excel(df) #
                    attachment_data = {"filename": filename, "base64_data": base64_data}
                    
                    first_10_rows_df = df.head(10) #
                    first_10_rows_markdown = self._df_to_markdown(first_10_rows_df) #
                    response_content = ( #
                        f"Here's the first 10 rows of the data for your question: \"{user_question_content}\"\n\n" #
                        f"{first_10_rows_markdown}\n\n" #
                        f"{DOWNLOAD_LINK_PLACEHOLDER}" #
                    )
            logger.info(f"{self.__class__.__name__}: Response generated: {response_content[:100]}...") #
            logger.debug(f"{self.__class__.__name__}: Full response: {response_content}") #
            response = AIMessage(content=response_content) #
            
            return {"messages": [response], "error": None, "attachment": attachment_data, "csv_data": csv_data}
        except Exception as e: #
            logger.error(f"{self.__class__.__name__}: Error in format_response: {str(e)}", exc_info=True) #
            error_message = f"Error formatting response: {str(e)}" #
            if format_preference == "natural_language": #
                error_message = f"* {error_message}" #
            response = AIMessage(content=error_message) #
            return {"messages": [response], "error": str(e)} #

    def run(self, question: str, thread_id: Optional[str] = None, format_preference: str = "natural_language", agent_stream: Optional[str] = None) -> Dict: #
        logger.info(f"{self.__class__.__name__}: Starting run for question: {question}, agent_stream: {agent_stream}, thread_id: {thread_id}") #
        
        if not thread_id: #
            thread_id = str(uuid.uuid4()) #
            logger.debug(f"{self.__class__.__name__}: Generated new thread_id: {thread_id}") #
        
        if not agent_stream: #
            logger.error(f"{self.__class__.__name__}: Agent stream not provided for run.") #
            return { "response": "Error: Agent stream is required.", "thread_id": thread_id }

        # ... (rest of the initial setup is the same)
        loaded_history = self._load_recent_messages_from_oracle(thread_id, agent_stream, limit=20) #
        current_human_message = HumanMessage(content=question) #
        initial_messages_for_graph = loaded_history + [current_human_message] #

        input_data = { #
            "messages": initial_messages_for_graph, #
            "format_preference": format_preference, #
            "agent_type": agent_stream #
        }
        
        config = {"configurable": {"thread_id": thread_id}} #
        logger.debug(f"{self.__class__.__name__}: Invoking graph with input: {input_data}, config: {config}") #
        
        try:
            result = self.graph.invoke(input_data, config) #
            
            ai_response_message_content = "No response generated." #
            if result.get("messages") and isinstance(result["messages"][-1], AIMessage): #
                ai_response_message_content = result["messages"][-1].content #
            
            logger.info(f"{self.__class__.__name__}: Run completed, response: {ai_response_message_content[:100]}...") #
            
            final_ai_response = ai_response_message_content

            if not result.get("error"):
                logger.info(f"Operation successful. Saving conversation for thread_id: {thread_id}")
                self._save_message_to_oracle(thread_id, "USER", question, agent_stream)
                ai_message_id = self._save_message_to_oracle(thread_id, "AI", ai_response_message_content, agent_stream)
                
                attachment_info = result.get("attachment")
                if attachment_info and ai_message_id:
                    logger.info(f"Attachment data found for AI Message ID: {ai_message_id}. Attempting to save.")
                    file_content_bytes = base64.b64decode(attachment_info['base64_data'])
                    
                    attachment_id = self._save_attachment_to_oracle(
                        message_id=ai_message_id,
                        filename=attachment_info['filename'],
                        mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                        file_content=file_content_bytes
                    )
                    
                    if attachment_id:
                        logger.info(f"Successfully saved attachment with ID: {attachment_id}. Creating and replacing link.")
                        download_link = self._get_download_link(attachment_id)
                        
                        # Get total number of rows from the original data for the link text
                        df = pd.read_csv(StringIO(result.get("csv_data", "")))
                        num_rows = len(df)

                        link_text = f"Download the full dataset ({num_rows} records)" if format_preference == "natural_language" else f"Download the full Excel file ({num_rows} records)"
                        final_link = f"[{link_text}]({download_link})"
                        
                        final_ai_response = ai_response_message_content.replace("[DOWNLOAD_LINK_PLACEHOLDER]", final_link)
                        self._update_message_content(ai_message_id, final_ai_response)
                        logger.info(f"Replaced placeholder with real download link for message {ai_message_id}")
                    else:
                        logger.error(f"Failed to save attachment to database for AI Message ID: {ai_message_id}. The download link will not be available.")
                        error_text = "(Download is currently unavailable due to a system error.)"
                        final_ai_response = ai_response_message_content.replace("[DOWNLOAD_LINK_PLACEHOLDER]", error_text)
                        self._update_message_content(ai_message_id, final_ai_response)

            else:
                logger.warning(f"Operation resulted in an error. Skipping conversation save for thread_id: {thread_id}. Error: {result.get('error')}")

            return { #
                "response": final_ai_response, #
                "query": result.get("query"), #
                "error": result.get("error"), #
                "thread_id": thread_id, #
                "question_type": result.get("question_type", "unknown"), #
                "format_preference": format_preference #
            }
        except Exception as e: #
            logger.error(f"{self.__class__.__name__}: Unhandled error in agent run: {str(e)}", exc_info=True) #
            error_response_content = f"An unexpected error occurred: {str(e)}" #
            logger.warning(f"Unhandled error caught. Skipping conversation save for thread_id: {thread_id}")
            return { #
                "response": error_response_content, #
                "query": None, #
                "error": str(e), #
                "thread_id": thread_id, #
                "question_type": "unknown", #
                "format_preference": format_preference #
            }

class SCMAgent(BaseAgent): #
    def __init__(self): #
        classification_prompt = """
        You need to classify if the following question is related to supply chain management or if it's a general question.

        Question: {latest_message}

        If the question is about inventory items, stock levels, product availability, warehouse information,
        item locations, purchase orders, suppliers, order statuses, or any other supply chain-related topic,
        classify it as "non-general".

        If the question is a general question not related to supply chain (e.g., about the weather, general knowledge,
        company information not related to supply chain, etc.), classify it as "general".

        Return only one word: either "non-general" or "general".
        """ # Updated prompt
        general_response = ( #
        f"I'm here to help with supply chain-related questions." #
        f"I can provide information about inventory items, stock levels, purchase orders, suppliers, and other data " #
        f"available through our supply chain management system.\n\n" #
        f"I can answer questions related to:\n" #
        f"- Item numbers and descriptions\n" #
        f"- Available quantities and units of measure\n" #
        f"- Purchase order numbers and statuses\n" #
        f"- Supplier information\n" #
        f"Please feel free to ask me any supply chain-related questions, and I'll be happy to assist you." #
    )
        super().__init__( #
            query_tools=SCMQueryTools(), #
            classification_prompt=classification_prompt, #
            general_response=general_response #
        )
        logger.info("SCMAgent initialized successfully") #

class HCMAgent(BaseAgent): #
    def __init__(self): #
        classification_prompt = """
        You need to classify if the following question is related to employee management or if it's a general question.

        Question: {latest_message}

        If the question is about employee details, assignments, organizations, positions, locations, managers,
        grades, hire dates, or any other employee-related topic, classify it as "non-general".

        If the question is a general question not related to employee management (e.g., about the weather, general knowledge,
        company information not related to employees, etc.), classify it as "general".

        Example questions that should be classified as non-general:
        Employee Demographics & Basic Info:

        1-How many employees are in each age group/generation?
        2-Which employees have birthdays this month/quarter?
        3-What's the gender distribution across departments?
        4-List all employees hired in the last 6 months
        
        Organizational Structure & Hierarchy:

        1-How many employees report to each manager?
        2-What's the organizational hierarchy for department X?
        3-Which positions have no current employees?
        4-Show the reporting structure for a specific employee
        
        Assignment & Employment Status:

        1-How many active vs inactive employees do we have?
        2-Which employees are on temporary assignments?
        3-List all employees with pending status changes
        4-What's the distribution of assignment types?
        
        Location & Geographic Analysis:
        
        1-How many employees work at each location?
        2-Which locations have the highest/lowest headcount?
        3-Show remote vs on-site employee distribution
        4-List employees who need to relocate
        
        Compensation & Grading:

        1-What's the grade distribution across the organization?
        2-Which employees are eligible for grade promotions?
        3-Show salary band analysis by department
        4-List employees at the highest/lowest grades
        
        Contract & Employment Terms:
        
        1-Which employees have contracts expiring soon?
        2-How many permanent vs contract employees do we have?
        3-Show contract renewal dates for the next quarter
        4-List employees with specific contract terms
        
        Tenure & Service Analysis:
        
        1-What's the average tenure by department?
        2-Which employees are approaching retirement eligibility?
        3-Show new hire vs veteran employee ratios
        4-List employees by length of service
        
        Communication & Contact:
        
        1-Generate employee contact directory by department
        2-Which employees are missing email addresses?
        3-Create mailing lists for specific groups
        4-Show communication preferences by location
        

        Return only one word: either "non-general" or "general".
        """ # Updated prompt
        general_response = ( #
            f" I'm designed to help with employee-related questions only. " #
            f"I can provide information about employee details, assignments, organizations, positions, and other data " #
            f"available through our human capital management system.\n\n" #
            f"I can answer questions related to:\n" #
            f"- Employee numbers and names\n" #
            f"- Assignments and statuses\n" #
            f"- Organizations and positions\n" #
            f"- Locations, managers, and grades\n" #
            f"Please feel free to ask me any employee-related questions, and I'll be happy to assist you." #
        )
        super().__init__( #
            query_tools=HCMQueryTools(), #
            classification_prompt=classification_prompt, #
            general_response=general_response #
        )
        logger.info("HCMAgent initialized successfully") #