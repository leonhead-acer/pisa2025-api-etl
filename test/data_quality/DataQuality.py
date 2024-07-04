from test.reader.JSONFileReader import JSONFileReader
from great_expectations.dataset.sparkdf_dataset import SparkDFDataset, PandasDataset
from test.expectation.NotNullExpectation import NotNullExpectation
from test.expectation.UniqueExpectation import UniqueExpectation
from test.expectation.ValuesInListExpectation import ValuesInListExpectation
from test.expectation.TwoColumnsMatchExpectation import TwoColumnsMatchExpectation
from test.expectation.StrLenEqExpectation import StrLenEqExpectation

class DataQuality:

    def __init__(self, df, config_path):
        self.df = df
        self.config_path = config_path

    def rule_mapping(self, dq_rule):
        return{
            "check_if_not_null" : "NotNullExpectation",
            "check_if_unique" : "UniqueExpectation",
            "check_if_values_in_list" : "ValuesInListExpectation",
            "check_if_two_cols_match": "TwoColumnsMatchExpectation",
            "check_if_str_len_eq": "StrLenEqExpectation"
        }[dq_rule]

    def _get_expectation(self):
        class_obj = globals()[self.rule_mapping()]
        return class_obj(self.extractor_args)
    
    def convert_to_ge_df(self):
        return PandasDataset(self.df)
    
    def read_config(self):
        json_reader = JSONFileReader(self.config_path)
        return json_reader.read()
      
    def run_test(self):
        ge_df = self.convert_to_ge_df()
        config = self.read_config()

        for table in config["table"]:
            if table["dq_rules"] is None:
                continue
            for dq_rule in table["dq_rules"]:
                expectation_obj = globals()[self.rule_mapping(dq_rule["rule_name"])]
                expectation_instance = expectation_obj(column = "", dimension = dq_rule["rule_dimension"], add_info = dq_rule["add_info"])
                expectation_instance.test(ge_df)

        for column in config["columns"]:
            if column["dq_rules"] is None:
                continue
            for dq_rule in column["dq_rules"]:
                expectation_obj = globals()[self.rule_mapping(dq_rule["rule_name"])]
                expectation_instance = expectation_obj(column = column["column_name"], dimension = dq_rule["rule_dimension"], add_info = dq_rule["add_info"])
                expectation_instance.test(ge_df)

        dq_results = ge_df.validate()
        return dq_results
        



    

    