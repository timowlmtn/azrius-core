import unittest

from core.MatchDataDictionary import MatchDataDictionary


class TestMatchDataDictionary(unittest.TestCase):

    MOCK_DATA_DICTIONARY = {
        "total_sales",
        "revenue",
        "product",
        "region",
        "customer_id",
        "order_date",
        "quantity",
        "price",
    }

    def test_match_data_dictionary(self):
        target = MatchDataDictionary(self.MOCK_DATA_DICTIONARY, cutoff=0.6)

        tokens = "total_sales by product".split(" ")

        # Assuming you have a function match_data_dictionary to test
        result = target.match_data_dictionary(tokens)
        print(result)
        self.assertEqual(
            {"total_sales": "total_sales", "by": None, "product": "product"}, result
        )

        result = target.match_data_dictionary("avg_sales by brand".split(" "))
        print(result)
        self.assertEqual(
            {"avg_sales": "total_sales", "brand": None, "by": None}, result
        )
