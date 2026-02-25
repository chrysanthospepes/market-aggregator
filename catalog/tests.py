from django.test import TestCase

from matching.normalizer import extract_quantity, normalize_text


class NormalizationTests(TestCase):
    def test_normalize_text_handles_greek_accents_and_case(self):
        self.assertEqual(normalize_text("ΦΡΕΣΚΟΎΛΗΣ ΙΤΑΛΙΚΉ"), "φρεσκουλης ιταλικη")

    def test_extract_quantity_parses_425gr(self):
        quantity = extract_quantity("Σαλάτα Ιταλική 425gr")
        self.assertIsNotNone(quantity)
        assert quantity is not None
        self.assertEqual(str(quantity.value), "425")
        self.assertEqual(quantity.unit, "g")

    def test_extract_quantity_converts_kg_to_g(self):
        quantity = extract_quantity("Σαλάτα 0.425kg")
        self.assertIsNotNone(quantity)
        assert quantity is not None
        self.assertEqual(str(quantity.value), "425")
        self.assertEqual(quantity.unit, "g")
