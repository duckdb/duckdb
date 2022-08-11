import duckdb
import pytest
from substrait_validator import Config

substrait_validator = pytest.importorskip('substrait_validator')

def run_substrait_validator(con, query):
    try:
        con.table('lineitem')
    except:
        con.execute(f"CALL dbgen(sf=0.01)")
    c = Config()
    # # Function Anchor to YAML file, no clue what is that
    c.override_diagnostic_level(3001, "error", "info")
    # function definition unavailable: cannot check validity of call
    c.override_diagnostic_level(6003, "warning", "info")
    # too few field names
    c.override_diagnostic_level(4003, "error", "info")
    try:
        proto = con.get_substrait(query).fetchone()[0]
    except Exception as err:
        raise ValueError("DuckDB Compilation: " + str(err))
    substrait_validator.check_plan_valid(proto, config=c)
    
def run_tpch_validator(require, query_number):
    con = require('substrait', 'test.db')
    if not con:
        return
    query = con.execute("select query from tpch_queries() where query_nr="+str(query_number)).fetchone()[0]

    run_substrait_validator(con,query)

@pytest.mark.parametrize('query_number', [1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,17,18,19,20])
def test_substrait_tpch_validator(require,query_number):
    run_tpch_validator(require,query_number)

# @pytest.mark.xfail(reason="DuckDB Compilation: INTERNAL Error: INTERNAL Error: CHUNK_GET")
# def test_substrait_tpch_validator_16(require):
#     run_tpch_validator(require,16)

# @pytest.mark.xfail(reason="DuckDB Compilation: INTERNAL Error: INTERNAL Error: DELIM_JOIN")
# def test_substrait_tpch_validator_21(require):
#     run_tpch_validator(require,21)

# @pytest.mark.xfail(reason="DuckDB Compilation: INTERNAL Error: INTERNAL Error: CHUNK_GET")
# def test_substrait_tpch_validator_22(require):
#     run_tpch_validator(require,22)

def test_substrait_tpch_validator_q(require):
    run_tpch_validator(require,2)