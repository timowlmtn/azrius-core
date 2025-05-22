import os
import traceback
import datetime
import uuid
from client.PromptHandler import PromptHandler


class MockPromptHandler(PromptHandler):
    client = None
    model = None
    system_prompt = None

    def __init__(self, model: str = "mock"):
        self.model = model

    def send_prompt(self, prompt: str) -> str:
        """
        Mocks sending a prompt by writing the prompt, call stack,
        and a mock response to a timestamped log directory, then echoes
        the prompt as the response.
        """
        # Create timestamped log directory
        timestamp = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        log_dir = os.path.join("logs", timestamp)
        os.makedirs(log_dir, exist_ok=True)

        # Unique ID for this call
        call_id = str(uuid.uuid4())
        base_path = os.path.join(log_dir, f"{call_id}")

        # Write the prompt to a file
        with open(f"{base_path}_prompt.txt", "w", encoding="utf-8") as f:
            f.write(prompt)

        # Capture and write the call stack
        stack = "".join(traceback.format_stack())
        with open(f"{base_path}_stack.txt", "w", encoding="utf-8") as f:
            f.write(stack)

        # Write the mocked "response"
        with open(f"{base_path}_response.txt", "w", encoding="utf-8") as f:
            f.write(prompt)

        return prompt  # Echo the prompt back as a mock response


if __name__ == "__main__":
    # Example usage of the MockPromptHandler
    handler = MockPromptHandler()
    test_prompt = "This is a test prompt for the mock AI client."
    response = handler.send_prompt(test_prompt)
    print("Response from MockPromptHandler:")
    print(response)
