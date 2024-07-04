import pandas as pd

def create_df_from_dq_results(dq_results):
    dq_data = []
    for result in dq_results["results"]:
        if result["success"] == True:
            status = 'PASSED'
        else:
            status = 'FAILED'

        kwgs = result["expectation_config"]["kwargs"]
        kwgs_cols = "; ".join([v for k,v in kwgs.items() if k.startswith("column")])

        dq_data.append((
        kwgs_cols,
        result["expectation_config"]["meta"]["dimension"],
        status,
        result["expectation_config"]["expectation_type"],
        result["result"]["unexpected_count"],
        result["result"]["element_count"],
        result["result"]["unexpected_percent"],
        float(100-result["result"]["unexpected_percent"])
        ))
    dq_columns = ["column", "dimension", "status", "expectation_type", "unexpected_count", "element_count", "unexpected_percent", "percent"]
    dq_df = pd.DataFrame(data=dq_data,columns=dq_columns)
    return dq_df