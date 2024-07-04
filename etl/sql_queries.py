# example queries, will be different across different db platform
from typing import Union, Tuple, List

def fetch_query_create (domain: str, cnt_code: Union[int,str]) -> str:
    
    if(domain == 'SCI'):
        regex_dom = '^(S\\d|SA1).*'
    elif(domain == 'FLA'):
        regex_dom = '^FLA\\-.*'
    else:
        raise ValueError("Domain doesn't exist")
    
    x = f"""
    SELECT
        login, test_qti_id, raw_data
    FROM oat.delivery_results
    WHERE test_qti_id ~ '{regex_dom}'
    AND login ~ '(?<=^.{{1}}){str(cnt_code)}'
    AND login ~ '^[125].*'
    AND login ~ '^((?!9999).)*$'
    AND login ~ '^((?!demo).)*$';
    """

    return x
