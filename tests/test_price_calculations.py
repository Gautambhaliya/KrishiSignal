import unittest


def calculate_price_change(previous_price, current_price):
    if previous_price is None or current_price is None:
        return None, None

    if previous_price <= 0 or current_price <= 0:
        return None, None

    difference = current_price - previous_price
    percentage = (difference / previous_price) * 100

    return round(difference, 2), round(percentage, 2)


class TestPriceCalculations(unittest.TestCase):

    def test_price_increase(self):
        difference, percentage = calculate_price_change(100, 120)
        self.assertEqual(difference, 20)
        self.assertEqual(percentage, 20)

    def test_price_decrease(self):
        difference, percentage = calculate_price_change(100, 80)
        self.assertEqual(difference, -20)
        self.assertEqual(percentage, -20)

    def test_no_price_change(self):
        difference, percentage = calculate_price_change(100, 100)
        self.assertEqual(difference, 0)
        self.assertEqual(percentage, 0)

    def test_missing_previous_price(self):
        difference, percentage = calculate_price_change(None, 120)
        self.assertIsNone(difference)
        self.assertIsNone(percentage)

    def test_zero_previous_price(self):
        difference, percentage = calculate_price_change(0, 120)
        self.assertIsNone(difference)
        self.assertIsNone(percentage)


if __name__ == "__main__":
    unittest.main()
