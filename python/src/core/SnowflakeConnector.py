import os
import snowflake.connector as sc
import base64
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization


class SnowflakeConnector:
    """
    A class to handle Snowflake connections using RSA authentication.
    """

    def __init__(self):
        """
        Initializes the SnowflakeConnector using environment variables.
        """
        self.conn = None
        self.private_key_file = os.getenv("SNOWFLAKE_PRIVATE_KEY_FILE")
        self.private_key_file_pwd = os.getenv(
            "SNOWFLAKE_PRIVATE_KEY_PASSWORD", ""
        )  # Default to empty if not required
        self.account = os.getenv("SNOWFLAKE_ACCOUNT")
        self.user = os.getenv("SNOWFLAKE_USER")
        self.warehouse = os.getenv("SNOWFLAKE_WAREHOUSE")
        self.database = os.getenv("SNOWFLAKE_DATABASE")
        self.schema = os.getenv("SNOWFLAKE_SCHEMA")

        self.conn_params = {
            "account": self.account,
            "user": self.user,
            "private_key": self._decode_private_key(),
            "warehouse": self.warehouse,
            "database": self.database,
            "schema": self.schema,
        }

    def _decode_private_key(self):
        encoded_string = os.getenv("SNOWFLAKE_PRIVATE_KEY")
        return base64.b64decode(encoded_string)

    def _load_private_key(self):
        """
        Loads the private key from the specified file.
        """
        try:
            print(f"Loading private key from {self.private_key_file}")
            with open(self.private_key_file, "rb") as key_file:
                private_key = key_file.read()
                p_key = serialization.load_pem_private_key(
                    private_key, password=None, backend=default_backend()
                )

                pkb = p_key.private_bytes(
                    encoding=serialization.Encoding.DER,
                    format=serialization.PrivateFormat.PKCS8,
                    encryption_algorithm=serialization.NoEncryption(),
                )
            return pkb
        except Exception as e:
            raise Exception(f"Error loading private key: {e}")

    def connect(self):
        """
        Establishes a connection to Snowflake.
        """
        try:
            self.conn = sc.connect(**self.conn_params)
            print(f"Connected to Snowflake ({self.conn.database}) successfully.")
        except Exception as e:
            raise Exception(f"Failed to connect to Snowflake: {e}")

        return self.conn

    def execute_query(self, query):
        """
        Executes a SQL query and returns the results.
        """
        if not self.conn:
            raise Exception("Connection is not established.")

        try:
            with self.conn.cursor() as cursor:
                cursor.execute(query)
                return cursor.fetchall()
        except Exception as e:
            raise Exception(f"Error executing query: {e}")

    def execute_queries(self, query_text):
        """Execute each semi-colon-separated SQL command and return all results."""
        if self.conn is None:
            self.connect()

        cursor = self.conn.cursor()
        try:
            results = []
            queries = [
                q.strip() for q in query_text.split(";") if q.strip()
            ]  # Split by `;` and remove empty queries

            for query in queries:
                cursor.execute(query)
                if cursor.description:  # Only capture results if there is a result set
                    columns = [desc[0] for desc in cursor.description]
                    query_results = [
                        dict(zip(columns, row)) for row in cursor.fetchall()
                    ]
                    results.append({"query": query, "result": query_results})

            return results
        finally:
            cursor.close()

    def close(self):
        """
        Closes the Snowflake connection.
        """
        if self.conn:
            self.conn.close()
            print("Snowflake connection closed.")


# Example usage:
if __name__ == "__main__":
    sf_connector = SnowflakeConnector()
    sf_connector.connect()

    # Example query
    result = sf_connector.execute_query("SELECT CURRENT_TIMESTAMP;")
    print(result)

    sf_connector.close()
