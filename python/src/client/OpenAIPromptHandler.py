from client.PromptHandler import PromptHandler
from openai import OpenAI
import os
from dotenv import load_dotenv
from core.read_config import load_config, create_ai_client_from_config


class OpenAIPromptHandler(PromptHandler):
    client = None
    model = None
    system_prompt = None

    def __init__(self, model: str = "gpt-4"):
        self.model = model

        api_key = os.environ.get("OPENAI_API_KEY")
        if not api_key:
            raise EnvironmentError("Missing OPENAI_API_KEY environment variable.")

        self.client = OpenAI(api_key=api_key)

    def send_prompt(self, prompt: str) -> str:
        messages = [
            {
                "role": "system",
                "content": self.system_prompt,
            },
            {
                "role": "user",
                "content": prompt,
            },
        ]

        # Create the chat completion request using the prompt from the JSON file
        response = self.client.chat.completions.create(
            model=self.model, messages=messages
        )
        return response.choices[0].message.content


if __name__ == "__main__":
    # Set up global logging (default INFO, can be overridden externally)
    import logging

    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
    )

    try:
        load_dotenv()  # load environment variables from .env

        # Load configuration from YAML
        config_dir = "../../../ragtime/config/config_openai.yml"
        config = load_config(config_dir)

        # Create an AI client instance based on the configuration for "simple_prompt"
        prompt_handler: PromptHandler = create_ai_client_from_config(
            config_dir, "simple_prompt"
        )

        # Define a test prompt to send
        test_prompt = "This is a test prompt."
        logging.debug(f"Sending test prompt: {test_prompt}")

        # Send the prompt using the selected AI client and print the response
        response = prompt_handler.send_prompt(test_prompt)
        print(f"Response: {response}")

    except Exception as e:
        logging.error(f"An error occurred: {e}")
