import duckdb
import pytest

def test_substrait_json(require):
    connection = require('substrait')
    if connection is None:
        print("substrait test skipped")
        return

    connection.execute('CREATE TABLE integers (i integer)')
    json =  connection.get_substrait_json("select * from integers limit 5").fetchone()[0]
    expected_result = '{"relations":[{"root":{"input":{"fetch":{"input":{"project":{"input":{"read":{"baseSchema":{"names":["i"],"struct":{"types":[{"i32":{"nullability":"NULLABILITY_NULLABLE"}}],"nullability":"NULLABILITY_REQUIRED"}},"projection":{"select":{"structItems":[{}]}},"namedTable":{"names":["integers"]}}},"expressions":[{"selection":{"directReference":{"structField":{}},"rootReference":{}}}]}},"count":"5"}},"names":["i"]}}]}'
    assert json == expected_result

    # Test broken query
    try:
        connection.get_substrait_json("select * from p limit 5").fetchone()[0]
    except Exception as  error:
        print (type(error))

    with pytest.raises(duckdb.Error, match="Table with name p does not exist!"):
        connection.get_substrait_json("select * from p limit 5").fetchone()[0]
        
    # Test closed connection
    connection.close()
    with pytest.raises(duckdb.Error, match="Connection has already been closed"):
        connection.get_substrait_json("select * from integers limit 5")

