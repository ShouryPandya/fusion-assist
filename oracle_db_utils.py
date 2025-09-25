import oracledb
import logging
from config import Config
import os

# Configure logging
logging.basicConfig(
    level=logging.DEBUG,
    filename="chatbot.log",
    filemode="a",
    format="%(asctime)s:%(levelname)s:%(name)s:%(message)s"
)
logger = logging.getLogger("oracle_db_utils")
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.DEBUG)
logger.addHandler(console_handler)

# Connection pool variable
_connection_pool = None

# REMOVED: Thick mode client initialization is no longer needed for ATP wallet connections.
# The oracledb library can handle this natively in "thin" mode.

def init_oracle_connection_pool():
    """Initializes the Oracle connection pool using ATP Wallet credentials."""
    global _connection_pool
    if _connection_pool is None:
        try:
            logger.info("Initializing Oracle connection pool for ATP database with wallet.")
            # MODIFIED: Connection pool now uses wallet configuration from Config.
            # This implements the connection approach you provided within a resilient connection pool.
            _connection_pool = oracledb.create_pool(
                user=Config.ORACLE_DB_USERNAME,
                password=Config.ORACLE_DB_PASSWORD,
                config_dir=Config.ORACLE_DB_CONFIG_DIR,
                dsn=Config.ORACLE_DB_DSN,
                wallet_location=Config.ORACLE_DB_WALLET_LOCATION,
                wallet_password=Config.ORACLE_DB_WALLET_PASSWORD,
                min=2,
                max=10,
                increment=1,
            )
            logger.info("Oracle ATP connection pool initialized successfully.")
        except oracledb.Error as e:
            error_obj, = e.args
            logger.error(f"Error initializing Oracle ATP connection pool: {error_obj.message}", exc_info=True)
            raise ConnectionError(f"Failed to connect to Oracle ATP database: {error_obj.message}")

def get_oracle_connection():
    """Gets a connection from the pool."""
    if _connection_pool is None:
        init_oracle_connection_pool()
    try:
        conn = _connection_pool.acquire()
        logger.debug("Acquired connection from Oracle ATP pool.")
        return conn
    except oracledb.Error as e:
        error_obj, = e.args
        logger.error(f"Error acquiring connection from Oracle ATP pool: {error_obj.message}", exc_info=True)
        raise ConnectionError(f"Failed to acquire database connection: {error_obj.message}")

def release_oracle_connection(conn):
    """Releases a connection back to the pool."""
    if conn and _connection_pool:
        try:
            _connection_pool.release(conn)
            logger.debug("Released connection back to Oracle ATP pool.")
        except oracledb.Error as e:
            error_obj, = e.args
            logger.error(f"Error releasing connection to Oracle ATP pool: {error_obj.message}", exc_info=True)

def close_oracle_connection_pool():
    """Closes the Oracle connection pool."""
    global _connection_pool
    if _connection_pool:
        try:
            _connection_pool.close()
            _connection_pool = None
            logger.info("Oracle ATP connection pool closed.")
        except oracledb.Error as e:
            error_obj, = e.args
            logger.error(f"Error closing Oracle ATP connection pool: {error_obj.message}", exc_info=True)

# Initialize the pool on module import
try:
    init_oracle_connection_pool()
except ConnectionError:
    logger.critical("Application will not function without Oracle ATP DB connection.")