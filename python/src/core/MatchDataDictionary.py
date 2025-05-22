from difflib import get_close_matches


class MatchDataDictionary:
    def __init__(self, dictionary, cutoff=0.6):
        self.dictionary = dictionary
        self.cutoff = cutoff

    def match_data_dictionary(self, tokens):
        """
        Matches the input tokens with the data dictionary using fuzzy matching.
        """
        result = {}

        for token in tokens:
            matches = get_close_matches(token, self.dictionary, cutoff=self.cutoff)
            if matches:
                result[token] = matches[0]
            else:
                result[token] = None

        return result
