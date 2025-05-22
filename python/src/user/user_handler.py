import argparse
import os
import requests
import json
from uuid import uuid4
from sqlalchemy import Column, String, BigInteger, text
from core.SingletonLogger import SingletonLogger
from sqlalchemy.inspection import inspect
from pprint import pprint

logger = SingletonLogger("routes").get_logger()

from core.db_loader import load_db

Base, SessionLocal = load_db()


def create_user_model(Base):
    class User(Base):
        __tablename__ = "azrius_user"

        user_id = Column(
            BigInteger, primary_key=True, autoincrement=True, nullable=False
        )
        first_name = Column(String, nullable=False)
        last_name = Column(String, nullable=False)
        email = Column(String, unique=True, nullable=False)
        phone_number = Column(String, nullable=False)
        llm_api_key = Column(String, nullable=False)
        app_api_key = Column(String, nullable=False, default=str(uuid4()))

        def to_dict(self):
            return {
                c.key: getattr(self, c.key) for c in inspect(self).mapper.column_attrs
            }

    return User


User = create_user_model(Base)


def get_db():
    Base, SessionLocal = load_db()
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def get_next_user_id(db):
    result = db.execute(text("SELECT web.azrius_user_seq.NEXTVAL")).fetchone()
    return result[0]


def create_user(data: dict) -> dict:
    """
    Create a new user in the database.
    """
    with SessionLocal() as db:
        if os.getenv("DB_TYPE") == "snowflake":
            # Snowflake doesn't support sequences in the same way as Postgres
            # This will use a sequence
            user_id = get_next_user_id(db)
        else:
            user_id = None

        if "phone_number" not in data:
            data["phone_number"] = None

        if "llm_api_key" not in data:
            data["llm_api_key"] = None

        user = User(
            user_id=user_id,
            first_name=data["first_name"],
            last_name=data["last_name"],
            email=data["email"],
            phone_number=data["phone_number"],
            llm_api_key=data["llm_api_key"],
            app_api_key=str(uuid4()),
        )
        db.add(user)
        print("Adding user to the database: %s", user.to_dict())
        logger.info("Adding user to the database: %s", user.to_dict())
        db.commit()
        db.refresh(user)
        return {"user_id": user.user_id, "message": "User registered successfully"}


def get_users() -> list[dict]:
    with SessionLocal() as db:
        users = db.query(User).all()
        return [user.to_dict() for user in users]


def get_user(user_id: str) -> dict:
    with SessionLocal() as db:
        user = db.query(User).filter(User.user_id == user_id).first()
        if user:
            return user.to_dict()
        return None


def get_user_by_email(email: str) -> dict:
    print("Getting user by email: %s", email)
    with SessionLocal() as db:
        user = db.query(User).filter(User.email == email).first()
        if user:
            return user.to_dict()
        return None


def update_user(user_id: str, updates: dict) -> dict:
    with SessionLocal() as db:
        user = db.query(User).filter(User.user_id == user_id).first()
        if not user:
            return None
        for key, value in updates.items():
            if hasattr(user, key):
                setattr(user, key, value)
        db.commit()
        db.refresh(user)
        return {"message": "User updated"}


def delete_user(user_id: str) -> dict:
    with SessionLocal() as db:
        user = db.query(User).filter(User.user_id == user_id).first()
        if not user:
            return None
        db.delete(user)
        db.commit()
        return {"message": "User deleted"}


def send_slack_notification(user: dict):
    webhook_url = os.getenv("SLACK_WEBHOOK_URL")
    if not webhook_url:
        logger.error("Slack webhook SLACK_WEBHOOK_URL not set.")
        return  # or raise an error/log

    message = {
        "text": f":tada: New user registered!\nName: {user['first_name']} {user['last_name']}\nEmail: {user['email']}"
    }

    try:
        response = requests.post(webhook_url, json=message)
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        print(f"Slack notification failed: {e}")


def register(data):
    """
    Register a new user.
    """
    if not data.get("first_name") or not data.get("last_name"):
        raise ValueError("First and last name are required.")
    if not data.get("email"):
        raise ValueError("Email is required.")

    user = get_user_by_email(data.get("email"))

    # We only create a new user if it doesn't exist
    if user is None:
        result = create_user(data)
        append_login(user_data=data, is_new=True, raw_payload=data)
    else:
        update_user(user["user_id"], data)
        result = user
        append_login(user_data=data, is_new=False, raw_payload=data)

    return result


def append_login(
    user_data: dict, is_new: bool, raw_payload: dict, user_agent: str = None
):
    """
    Appends a login record to the azrius_user_login table.
    """
    with SessionLocal() as db:
        db.execute(
            text(
                """
            INSERT INTO web.azrius_user_login (
                uid, email, first_name, last_name, provider, is_new,
                raw_payload, user_agent
            ) VALUES (
                :uid, :email, :first_name, :last_name, :provider, :is_new,
                :raw_payload, :user_agent
            )
        """
            ),
            {
                "email": user_data["email"],
                "first_name": user_data.get("first_name"),
                "last_name": user_data.get("last_name"),
                "provider": user_data.get("provider", "google"),
                "is_new": is_new,
                "raw_payload": json.dumps(raw_payload),
                "user_agent": user_agent,
            },
        )

        logger.info(f"Logged login for user: {user_data['email']} (is_new={is_new})")
        db.commit()


def main():
    parser = argparse.ArgumentParser(description="Azrius User Management CLI")
    subparsers = parser.add_subparsers(dest="command", required=True)

    # Register
    register_parser = subparsers.add_parser("register", help="Register a new user")

    register_parser.add_argument(
        "--first", default=os.environ.get("USER_FIRST"), help="First name"
    )
    register_parser.add_argument(
        "--last", default=os.environ.get("USER_LAST"), help="Last name"
    )
    register_parser.add_argument(
        "--email", default=os.environ.get("USER_EMAIL"), help="Email"
    )
    register_parser.add_argument(
        "--phone", default=os.environ.get("USER_PHONE"), help="Phone number"
    )
    register_parser.add_argument(
        "--key", default=os.environ.get("USER_LLM_KEY"), help="Anthropic API key"
    )

    # Get
    get_parser = subparsers.add_parser("get", help="Get a user by ID")
    get_parser.add_argument("user_id", help="User ID")

    # Update
    update_parser = subparsers.add_parser("update", help="Update an existing user")
    update_parser.add_argument("user_id", help="User ID")
    update_parser.add_argument(
        "--field",
        action="append",
        nargs=2,
        metavar=("FIELD", "VALUE"),
        help="Field to update (repeatable)",
    )

    # Delete
    delete_parser = subparsers.add_parser("delete", help="Delete a user by ID")
    delete_parser.add_argument("user_id", help="User ID")

    # Lifecycle test
    subparsers.add_parser(
        "test", help="Run full create -> get -> update -> delete lifecycle"
    )

    args = parser.parse_args()

    if args.command == "register":
        user = {
            "first_name": args.first,
            "last_name": args.last,
            "email": args.email,
            "phone_number": args.phone,
            "llm_api_key": args.key,
        }

        result = create_user(user)
        pprint(result)

    elif args.command == "get":
        user = get_user(args.user_id)
        if user:
            pprint(user)
        else:
            print("User not found.")

    elif args.command == "update":
        updates = {field: value for field, value in args.field}
        result = update_user(args.user_id, updates)
        if result:
            pprint(result)
        else:
            print("User not found.")

    elif args.command == "delete":
        result = delete_user(args.user_id)
        if result:
            pprint(result)
        else:
            print("User not found.")

    elif args.command == "test":
        print("\n🔧 Running test lifecycle...")

        # Create
        user_data = {
            "first_name": "Testy",
            "last_name": "McUserface",
            "email": "test@example.com",
            "phone_number": "555-1234",
            "llm_api_key": "test-api-key-123",
        }
        create_result = create_user(user_data)
        user_id = create_result["user_id"]
        print("\n✅ Created:")
        pprint(create_result)

        # Get
        retrieved = get_user(user_id)
        print("\n📥 Retrieved:")
        pprint(retrieved)

        # Update
        update_result = update_user(user_id, {"phone_number": "555-4321"})
        print("\n✏️ Updated:")
        pprint(update_result)

        # Final Get
        final = get_user(user_id)
        print("\n📥 After Update:")
        pprint(final)

        # Delete
        delete_result = delete_user(user_id)
        print("\n❌ Deleted:")
        pprint(delete_result)

        # Verify Deletion
        post_delete = get_user(user_id)
        print("\n🔍 Post-deletion retrieval:")
        pprint(post_delete)


if __name__ == "__main__":
    main()
