from abc import ABC, abstractmethod


class PromptHandler(ABC):
    system_prompt = None

    @abstractmethod
    def send_prompt(self, prompt: str) -> str:
        """
        Sends a prompt string and returns the AI response as a string.
        """
        pass
