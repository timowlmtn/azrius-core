import unittest
from core.CommandParser import CommandParser


class TestCommandParser(unittest.TestCase):

    def test_command_parser(self):
        parser = CommandParser()

        self.assertEqual(
            {
                "dimension": ["brand"],
                "metric": ["total_sales"],
                "query": "show total_sales by brand",
                "raw": "/aa show total_sales by brand",
            },
            parser.parse_raw("/aa show total_sales by brand"),
        )

    def test_match_top_level_parens(self):
        parser = CommandParser()

        result = parser.extract_parenthesized_clauses(
            "(show revenue by product) and (show total sales by region)"
        )

        self.assertEqual(
            [
                ("show revenue by product", 0, 25),
                ("show total sales by region", 30, 58),
            ],
            result,
        )
