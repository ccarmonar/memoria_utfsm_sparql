import unittest
from functions_test import compare_json
from parameterized import parameterized


class TestJson(unittest.TestCase):
    @parameterized.expand([
        ["test_wikidata1", compare_json("test_wikidata1")],
        ["test_wikidata2", compare_json("test_wikidata2")],
        ["test_wikidata3", compare_json("test_wikidata3")],
        ["test_wikidata4", compare_json("test_wikidata4")],
        ["test_wikidata5", compare_json("test_wikidata5")],
        ["test_wikidata6", compare_json("test_wikidata6")], #Problemas en el optional, lo identifica sin que exista
        ["test_wikidata7", compare_json("test_wikidata7")],
        ["test_wikidata8", compare_json("test_wikidata8")],
        ["test_wikidata9", compare_json("test_wikidata9")], #Problemas en el optional, lo identifica sin que exista
        ["test_wikidata10", compare_json("test_wikidata10")],
        ["test_wikidata11", compare_json("test_wikidata11")],
        ["test_wikidata12", compare_json("test_wikidata12")],
        ["test_wikidata13", compare_json("test_wikidata13")],
        ["test_wikidata14", compare_json("test_wikidata14")],
        ["test_wikidata15", compare_json("test_wikidata15")],
        ["test_wikidata16", compare_json("test_wikidata16")],
        ["test_wikidata17", compare_json("test_wikidata17")],
        ["test_wikidata18", compare_json("test_wikidata18")],
        ["test_wikidata19", compare_json("test_wikidata19")],
        ["test_wikidata19", compare_json("test_wikidata20")],
    ])
    def test_compare_json(self, name, a):
        self.assertTrue(a)


if __name__ == '__main__':
    unittest.main()
