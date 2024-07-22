from test.expectation.Expectation import Expectation

class TwoColumnsMatchExpectation(Expectation):
    def __init__(self, column, dimension, add_info = {}):
        super().__init__(column, dimension, add_info)

    def test(self, ge_df):
        ge_df.expect_column_pair_values_to_be_equal(column_A = self.add_info["column_A"], column_B = self.add_info["column_B"], meta = {"dimension": self.dimension})

