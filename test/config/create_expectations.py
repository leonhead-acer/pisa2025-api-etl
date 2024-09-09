import great_expectations as ge
from great_expectations.core import ExpectationSuite

name = "nc_dat"

# Create an ExpectationSuite without initiating a DataFrame or project
suite = ExpectationSuite(expectation_suite_name=name)

# Add expectations by directly appending to the 'expectations' attribute
suite.expectations.append({
    "expectation_type": "expect_column_to_exist",
    "kwargs": {"column": "isoalpha3"}
})

suite.expectations.append({
    "expectation_type": "expect_column_to_exist",
    "kwargs": {"column": "isocntcd"}
})

# Save the suite to a JSON file
suite_json = suite.to_json_dict()

# Write the JSON output to a file
import json
with open(f"./src/expectations/{name}.json", "w") as f:
    json.dump(suite_json, f, indent=4)

print(f"Expectation suite saved to '{name}.json'")