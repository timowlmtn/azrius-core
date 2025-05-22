import traceback
from flask import request, jsonify
from core.handlers import echo_handler
from user import user_handler
from erp import erp_handler

import json

from core.SingletonLogger import SingletonLogger

logger = SingletonLogger("routes").get_logger()


def register_routes(app):

    @app.route("/api/echo", methods=["GET"])
    def echo():
        """
        Echoes the query parameters sent in the request.
        ---
        parameters:
          - name: status
            in: query
            type: string
            required: false
            description: Status message to echo
          - name: user
            in: query
            type: string
            required: false
            description: User identifier
        responses:
          200:
            description: Echoed query parameters
            schema:
              type: object
              additionalProperties:
                type: string
        """
        params = request.args.to_dict()
        return jsonify(echo_handler(params))

    @app.route("/api/users/register", methods=["POST"])
    def register_user():
        """
        Register a new user.
        ---
        tags:
          - Users
        parameters:
          - in: body
            name: body
            required: true
            schema:
              type: object
              required:
                - first_name
                - last_name
                - email
                - phone_number
                - anthropic_api_key
              properties:
                first_name:
                  type: string
                last_name:
                  type: string
                email:
                  type: string
                phone_number:
                  type: string
                llm_api_key:
                  type: string
        responses:
          201:
            description: User registered successfully
            schema:
              type: object
              properties:
                user_id:
                  type: string
                message:
                  type: string
          400:
            description: Missing or invalid fields
        """
        data = request.get_json()
        required = ["first_name", "last_name", "email"]
        missing = [field for field in required if not data.get(field)]

        if missing:
            return jsonify({"message": f'Missing fields: {", ".join(missing)}'}), 400

        logger.info(f"Registering user with data: {json.dumps(data)}")

        user = user_handler.register(data)

        return (
            jsonify(
                {"message": "User registered successfully", "user_id": user["user_id"]}
            ),
            201,
        )

    @app.route("/api/users/append_user_login", methods=["POST"])
    def append_user_login():
        """
        Append Google Login Event
        ---
        tags:
          - Users
        parameters:
          - in: body
            name: body
            required: true
            schema:
              type: object
              required:
                - uid
                - email
                - first_name
                - last_name
              properties:
                uid:
                  type: string
                  description: Google user ID
                  example: "1234567890"
                email:
                  type: string
                  description: User's email
                  example: "user@example.com"
                first_name:
                  type: string
                  example: "Alice"
                last_name:
                  type: string
                  example: "Example"
                provider:
                  type: string
                  default: "google"
                  example: "google"
                is_new:
                  type: boolean
                  default: false
                raw_payload:
                  type: object
                  description: Optional full login payload
                user_agent:
                  type: string
                  description: Optional user agent string
        responses:
          200:
            description: Login event appended successfully
            schema:
              type: object
              properties:
                message:
                  type: string
                  example: "Login appended successfully"
          400:
            description: Missing or invalid fields
            schema:
              type: object
              properties:
                error:
                  type: string
                  example: "Missing required fields: uid, email"
          500:
            description: Internal server error
            schema:
              type: object
              properties:
                error:
                  type: string
                  example: "Failed to append login event"
        """
        try:
            data = request.get_json()
            if not data:
                return jsonify({"error": "No JSON payload provided"}), 400

            required_fields = ["uid", "email", "first_name", "last_name"]
            missing = [field for field in required_fields if field not in data]
            if missing:
                return (
                    jsonify(
                        {"error": f"Missing required fields: {', '.join(missing)}"}
                    ),
                    400,
                )

            is_new = data.get("is_new", False)
            raw_payload = data.get("raw_payload", data)
            user_agent = data.get("user_agent", request.headers.get("User-Agent"))

            user_handler.append_login(
                user_data=data,
                is_new=is_new,
                raw_payload=raw_payload,
                user_agent=user_agent,
            )

            return jsonify({"message": "Login appended successfully"}), 200

        except Exception as e:
            logger.exception("Failed to append login event")
            return jsonify({"error": str(e)}), 500

    @app.route("/api/users", methods=["GET"])
    def get_users():
        """
        Get all users.
        ---
        tags:
          - Users
        responses:
          200:
            description: List of users
            schema:
              type: array
              items:
                type: object
          500:
            description: Internal server error
        """
        try:
            users = user_handler.get_users()
            logger.info(f"{len(users)} users retrieved")
            return json.dumps(users), 200
        except Exception as e:
            print(f"Failed to retrieve users: {str(e)}\n{traceback.format_exc()}")
            return jsonify({"error": "Internal server error"}), 500

    @app.route("/api/users/<user_id>", methods=["GET"])
    def get_user(user_id):
        """
        Get a user by ID.
        ---
        tags:
          - Users
        parameters:
          - name: user_id
            in: path
            type: string
            required: true
            description: The user ID
        responses:
          200:
            description: User data retrieved
            schema:
              type: object
          404:
            description: User not found
        """
        user = user_handler.get_user(user_id)
        if not user:
            return jsonify({"message": "User not found"}), 404
        logger.info(f"User data retrieved: {json.dumps(user)}")
        return json.dumps(user), 200

    @app.route("/api/users/<user_id>", methods=["PUT"])
    def update_user(user_id):
        """
        Update an existing user.
        ---
        tags:
          - Users
        parameters:
          - name: user_id
            in: path
            type: string
            required: true
            description: The user ID
          - in: body
            name: body
            required: true
            schema:
              type: object
              additionalProperties:
                type: string
        responses:
          200:
            description: User updated
          404:
            description: User not found
        """

        user = user_handler.get_user(user_id)

        if user:
            return jsonify({"message": "User not found"}), 404

        updates = request.get_json()
        user.update(updates)

        user_handler.update_user(user_id, user)

        return jsonify({"message": "User updated"}), 200

    @app.route("/api/users/<user_id>", methods=["DELETE"])
    def delete_user(user_id):
        """
        Delete a user by ID.
        ---
        tags:
          - Users
        parameters:
          - name: user_id
            in: path
            type: string
            required: true
            description: The user ID
        responses:
          200:
            description: User deleted
          404:
            description: User not found
        """
        user = user_handler.get_user(user_id)

        if user:
            return jsonify({"message": "User not found"}), 404

        user_handler.delete_user(user_id)

        return jsonify({"message": "User deleted"}), 200

    @app.route("/api/erp/<schema_name>/tables", methods=["GET"])
    def get_erp_tables(schema_name):
        """
        Get all tables in the specified schema for ERP.
        ---
        tags:
          - ERP
        parameters:
          - name: schema_name
            in: path
            type: string
            required: true
            description: The schema name to inspect
        responses:
          200:
            description: List of table names
            schema:
              type: array
              items:
                type: string
          500:
            description: Error retrieving tables
        """
        try:
            tables = erp_handler.get_tables_in_schema(schema_name)
            return jsonify(tables), 200
        except Exception as e:
            logger.error(
                f"Error fetching ERP tables for schema '{schema_name}': {str(e)}"
            )
            return jsonify({"error": "Failed to retrieve tables"}), 500

    @app.route("/api/erp/<schema_name>/tables/<table_name>/rows", methods=["GET"])
    def get_erp_table_rows(schema_name, table_name):
        """
        Get the top 100 rows in the specified table for ERP.
        ---
        tags:
          - ERP
        parameters:
          - name: schema_name
            in: path
            type: string
            required: true
            description: The schema name to inspect
          - name: table_name
            in: path
            type: string
            required: true
            description: The table name to inspect
        responses:
          200:
            description: List of row objects
            schema:
              type: array
              items:
                type: object
          500:
            description: Error retrieving rows
        """
        try:
            # assumes erp_handler.get_table_rows(schema, table, limit) returns a list of dicts
            rows = erp_handler.get_table_rows(schema_name, table_name, limit=100)
            return jsonify(rows), 200
        except Exception as e:
            logger.error(
                f"Error fetching top rows for table '{schema_name}.{table_name}': {e}"
            )
            return jsonify({"error": "Failed to retrieve rows"}), 500
