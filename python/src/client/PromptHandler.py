from abc import ABC, abstractmethod
import logging


class PromptHandler(ABC):
    system_prompt = None

    @abstractmethod
    def send_prompt(self, prompt: str) -> str:
        """
        Sends a prompt string and returns the AI response as a string.
        """
        pass


def create_ai_client_from_args(model: str, system_prompt: str) -> PromptHandler:
    """
    Creates an AI client instance based on the given model name and
    assigns the provided system prompt.

    Args:
        model (str): The model to use (e.g., "mock", "gpt-4o", "llama3").
        system_prompt (str): The system prompt text to set on the handler.

    Returns:
        PromptHandler: An instance of the selected AI client that implements PromptHandler.

    Raises:
        ValueError: If the specified model is not supported.
    """
    normalized = model.lower()
    logging.debug(f"Creating AI client for model '{normalized}'")

    if normalized == "mock":
        from client.MockPromptHandler import MockPromptHandler

        handler = MockPromptHandler()

    elif normalized == "gpt-4o":
        from client.OpenAIPromptHandler import OpenAIPromptHandler

        handler = OpenAIPromptHandler(normalized)

    elif normalized == "llama3":
        from client.LlamaPromptHandler import LlamaPromptHandler

        handler = LlamaPromptHandler(normalized)

    else:
        raise ValueError(f"Unsupported model '{model}' specified.")

    handler.system_prompt = system_prompt

    logging.debug(
        f"Initialized AI client {handler} with model '{normalized}'\n"
        f"System prompt:\n{handler.system_prompt}"
    )

    return handler
