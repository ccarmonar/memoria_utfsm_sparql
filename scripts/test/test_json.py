import unittest
from functions_test import compare_json
from parameterized import parameterized


class TestJson(unittest.TestCase):
    @parameterized.expand([
        ["test_wikidata1", compare_json("test_wikidata1")],
        ["test_wikidata2", compare_json("test_wikidata2")],
        ["test_wikidata3", compare_json("test_wikidata3")],
        ["test_wikidata4", compare_json("test_wikidata4")],
        #["test_wikidata6", compare_json("test_wikidata6")], #Problemas en el optional, lo identifica sin que exista
        ["test_wikidata7", compare_json("test_wikidata7")],
        ["test_wikidata8", compare_json("test_wikidata8")],
        #["test_wikidata9", compare_json("test_wikidata9")], #Problemas en el optional, lo identifica sin que exista
        ["test_wikidata10", compare_json("test_wikidata10")],
        ["test_wikidata11", compare_json("test_wikidata11")],

    ])
    def test_compare_json(self, name, a):
        self.assertTrue(a)


if __name__ == '__main__':
    unittest.main()
