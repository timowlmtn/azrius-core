import re


class CommandParser:
    object_keys = ["metric", "dimension", "filter", "sort", "group", "limit"]

    """
    This class is responsible for parsing command line arguments and options.
    It uses the argparse library to handle command line arguments and options.
    """

    def parse_raw(self, raw):
        """
        Add command line arguments and options to the parser.
        """
        result = {}
        result["raw"] = raw

        query = raw.strip()

        if raw.startswith("/aa"):
            query = raw[3:].strip()

        result["query"] = query

        match = re.match(r"^show\s+([\w\s,]+)\s+by\s+([\w\s,]+)", query)

        if match:
            metric = match.group(1).split(",")
            dimension = match.group(2).split(",")
            result["metric"] = [m.strip() for m in metric]
            result["dimension"] = [d.strip() for d in dimension]
        else:
            raise ValueError(f"Unrecognized command format: {query}")

        return result

    def extract_parenthesized_clauses(self, input_string):
        result = []
        buffer = []
        start_index = 0
        for i, char in enumerate(input_string):
            if char == "(":
                buffer = []
                start_index = i
            elif char == ")":
                clause = "".join(buffer)
                result.append((clause, start_index, i + 1))
                buffer = []
            else:
                buffer.append(char)

        return result
