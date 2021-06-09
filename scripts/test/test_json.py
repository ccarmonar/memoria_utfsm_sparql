import unittest
from functions_test import compare_json
from parameterized import parameterized


class TestJson(unittest.TestCase):
    @parameterized.expand([
        ["test_wikidata1", compare_json("test_wikidata1")],
        ["test_wikidata2", compare_json("test_wikidata2")],
        ["test_wikidata3", compare_json("test_wikidata3")],
        ["test_wikidata4", compare_json("test_wikidata4")],
        ["test_wikidata5", compare_json("test_wikidata5")]
    ])
    def test_compare_json(self, name, a):
        self.assertTrue(a)


if __name__ == '__main__':
    unittest.main()
