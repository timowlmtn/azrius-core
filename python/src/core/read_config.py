import os
import yaml
import logging
from client.PromptHandler import PromptHandler


def load_config(config_path: str) -> dict:
    """
    Loads the configuration from a YAML file.

    Args:
        config_path (str): Path to the YAML configuration file.

    Returns:
        dict: Loaded configuration dictionary.

    Raises:
        FileNotFoundError: If the configuration file is not found.
    """
    if not os.path.exists(config_path):
        raise FileNotFoundError(f"Config file not found: {config_path}")

    with open(config_path, "r", encoding="utf-8") as f:
        config = yaml.safe_load(f)

    return config


def create_ai_client_from_config(
    config_dir: str, prompt_name: str = "simple_prompt"
) -> PromptHandler:
    """
    Creates an AI client instance based on the model specified in the configuration for the given prompt name.

    Expected config format:

        prompts:
          simple_prompt:
            model: mock
            metadata:
              tags: [summarization, journalism]
              semantic_cache: true

    Args:
        config (dict): The configuration dictionary.
        prompt_name (str): The key in the 'prompts' section to load the config for.

    Returns:
        PromptHandler: An instance of the selected AI client that implements PromptHandler.

    Raises:
        KeyError: If the specified prompt is not found in the configuration.
        :param prompt_name:
        :param config:
        :param config_dir:
    """
    prompts_config = load_config(config_dir)

    logging.debug(f"Loading AI client from config: {prompts_config}")

    if prompt_name not in prompts_config["prompts"]:
        raise KeyError(f"Prompt '{prompt_name}' not found in the configuration.")

    prompt_config = prompts_config["prompts"][prompt_name]
    model = prompt_config.get("model", "default").lower()

    if model == "mock":
        from client.MockPromptHandler import MockPromptHandler

        result = MockPromptHandler()
    elif model == "gpt-4o":
        from client.OpenAIPromptHandler import OpenAIPromptHandler

        result = OpenAIPromptHandler(model)

    elif model == "llama3":
        from client.LlamaPromptHandler import LlamaPromptHandler

        result = LlamaPromptHandler(model)

    else:
        raise ValueError(f"Unsupported model '{model}' specified in the configuration.")

    result.system_prompt = prompt_config["system_prompt"]

    logging.debug(
        f"Using AI client {result} with model:{model}\n{result.system_prompt}"
    )

    return result


if __name__ == "__main__":
    # Set up global logging (default INFO, can be overridden externally)
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
    )

    try:
        # Load configuration from YAML
        config_dir = "ragtime/config/config.yml"
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
        logging.debug(f"Response received: {response}")
        print("Response from AI client:")
        print(response)

    except Exception as e:
        logging.exception(f"Error creating or using the AI client {e}")
