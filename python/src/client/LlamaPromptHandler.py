import requests
from dotenv import load_dotenv
from client.PromptHandler import PromptHandler
from analytics.AugmentLyricSentiment import AugmentLyricsSentiment

lyrics = """
Lyrics for 'Bonnet of Pins' by Matt Berninger:

7 ContributorsBonnet Of Pins Lyrics
It takes a lot to really disappear
Always leave traces in the leaves
Never thought I'd see her here
Never thought I'd see her again

She sidewinders through the room to me
With a real cigarette and a Styrofoam coffee
She's still wearing her father's feather jacket
She holds out her hands and I stand to receive her
Trying to remember the last time I'd seen her
Somehow she looks younger now

She finishes off my drink and
Puts on her bonnet of pins and
Says I
Thought I'd find you much quicker than this
You must've thought I didn't exist, poor you
I do
We'd better go before your boyfriends cry

She says she takes photos of tractor bones
And sells 'em to model luxury homes
The closest thing she's ever found to love
Is the kind you can't get rid of fast enough

She finishes off my drink and
Puts on her bonnet of pins and
Says I
Thought I'd find you much quicker than this
You must've thought I didn't exist, poor you
I do
We'd better go before your boyfriends cry

Take the stairs to the bottom where the lights are out
And I'll be there with a lighter and a Nabokov cocktail
Forget the questionnaires and the oral histories
I don't care how many times you almost said you missed me
It's a cup trick shell game, it's a puff of smoke
And it gets me every time, it's a pretty good joke
I know that you miss me, I know that you miss me
This stuff takes a lifetime

She finishes off my drink and
Puts on her bonnet of pins and
Says I
Thought I'd find you much quicker than this
You must've thought I didn't exist, poor you
I do
I thought I'd find you much quicker than this
You must've thought I didn't exist, poor you
I do
We'd better go before your boyfriends cry
"""


class LlamaPromptHandler(PromptHandler):
    model = None
    url = None

    def __init__(self, model: str = "llama3"):
        self.url = "http://localhost:11434/api/chat"
        self.model = model

    def send_prompt(self, prompt: str) -> str:
        data = {
            "model": self.model,
            "messages": [{"role": "user", "content": prompt}],
            "stream": False,
        }

        headers = {"Content-Type": "application/json"}

        prompt_result = requests.post(self.url, headers=headers, json=data)
        prompt_result.raise_for_status()
        print("\tresult: ", prompt_result.json())
        return prompt_result.json()["message"]["content"]


if __name__ == "__main__":
    load_dotenv()  # load environment variables from .env

    # Example usage of the LlamaPromptHandler
    handler = LlamaPromptHandler("llama3")

    test_prompt = AugmentLyricsSentiment.lyric_sentiment_prompt(lyrics)
    response = handler.send_prompt(test_prompt)
    print("Response from PromptHandler:")
    print(response)
