import unittest
import os
from unittest.mock import patch, MagicMock

# Ensure mock DB is used
os.environ["DB_TYPE"] = "mock"

from user.user_handler import (
    create_user,
    get_user,
    get_users,
    update_user,
    delete_user,
    register,
)


class TestUserHandler(unittest.TestCase):
    def setUp(self):
        self.mock_user = {
            "first_name": "Alice",
            "last_name": "Wonder",
            "email": "alice@example.com",
            "phone_number": "123-4567",
            "llm_api_key": "fake-key-123",
        }

    @patch("user.user_handler.User")
    @patch("user.user_handler.SessionLocal")
    def test_create_user(self, mock_session, mock_user_class):
        mock_db = MagicMock()
        mock_session.return_value.__enter__.return_value = mock_db

        mock_user_instance = MagicMock()
        mock_user_instance.user_id = 1
        mock_user_instance.to_dict.return_value = {**self.mock_user, "user_id": 1}
        mock_user_class.return_value = mock_user_instance

        result = create_user(self.mock_user)

        self.assertEqual(result["message"], "User registered successfully")
        self.assertEqual(result["user_id"], 1)

    @patch("user.user_handler.SessionLocal")
    def test_get_user_found(self, mock_session):
        mock_db = MagicMock()
        mock_session.return_value.__enter__.return_value = mock_db

        mock_user = MagicMock()
        mock_user.to_dict.return_value = {"user_id": 1, "first_name": "Alice"}
        mock_db.query.return_value.filter.return_value.first.return_value = mock_user

        result = get_user(1)
        self.assertEqual(result["first_name"], "Alice")

    @patch("user.user_handler.SessionLocal")
    def test_get_user_not_found(self, mock_session):
        mock_db = MagicMock()
        mock_session.return_value.__enter__.return_value = mock_db
        mock_db.query.return_value.filter.return_value.first.return_value = None

        result = get_user(999)
        self.assertIsNone(result)

    @patch("user.user_handler.SessionLocal")
    def test_get_users(self, mock_session):
        mock_db = MagicMock()
        mock_session.return_value.__enter__.return_value = mock_db

        mock_user1 = MagicMock()
        mock_user1.to_dict.return_value = {"user_id": 1, "first_name": "Alice"}

        mock_user2 = MagicMock()
        mock_user2.to_dict.return_value = {"user_id": 2, "first_name": "Bob"}

        mock_db.query.return_value.all.return_value = [mock_user1, mock_user2]

        result = get_users()
        self.assertEqual(len(result), 2)
        self.assertEqual(result[0]["first_name"], "Alice")
        self.assertEqual(result[1]["first_name"], "Bob")

    @patch("user.user_handler.SessionLocal")
    def test_update_user_found(self, mock_session):
        mock_db = MagicMock()
        mock_session.return_value.__enter__.return_value = mock_db

        mock_user = MagicMock()
        mock_db.query.return_value.filter.return_value.first.return_value = mock_user

        result = update_user(1, {"phone_number": "987-6543"})
        self.assertEqual(result["message"], "User updated")

    @patch("user.user_handler.SessionLocal")
    def test_update_user_not_found(self, mock_session):
        mock_db = MagicMock()
        mock_session.return_value.__enter__.return_value = mock_db
        mock_db.query.return_value.filter.return_value.first.return_value = None

        result = update_user(999, {"phone_number": "987-6543"})
        self.assertIsNone(result)

    @patch("user.user_handler.SessionLocal")
    def test_delete_user_found(self, mock_session):
        mock_db = MagicMock()
        mock_session.return_value.__enter__.return_value = mock_db

        mock_user = MagicMock()
        mock_db.query.return_value.filter.return_value.first.return_value = mock_user

        result = delete_user(1)
        self.assertEqual(result["message"], "User deleted")
        mock_db.delete.assert_called_once_with(mock_user)

    @patch("user.user_handler.SessionLocal")
    def test_delete_user_not_found(self, mock_session):
        mock_db = MagicMock()
        mock_session.return_value.__enter__.return_value = mock_db
        mock_db.query.return_value.filter.return_value.first.return_value = None

        result = delete_user(999)
        self.assertIsNone(result)

    @patch("user.user_handler.SessionLocal")
    def test_register(self, mock_session):
        mock_db = MagicMock()
        mock_session.return_value.__enter__.return_value = mock_db
        mock_db.query.return_value.filter.return_value.first.return_value = None

        result = register(self.mock_user)
        print(result)


if __name__ == "__main__":
    unittest.main()
