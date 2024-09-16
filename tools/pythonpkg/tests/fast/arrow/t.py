import duckdb
import pytest

pa = pytest.importorskip('pyarrow')

# from arrow_canonical_extensions import UHugeIntType


class UHugeIntType(pa.ExtensionType):
    def __init__(self):
        pa.ExtensionType.__init__(self, pa.binary(16), "duckdb.uhugeint")

    def __arrow_ext_serialize__(self):
        return b''

    @classmethod
    def __arrow_ext_deserialize__(self, storage_type, serialized):
        return UHugeIntType()


pa.register_extension_type(UHugeIntType())

con = duckdb.connect()
tbl_name = "t"
data_type = "hugeint"
con.execute(
    f"""
    CREATE TABLE {tbl_name} (
        a {data_type},
        b {data_type},
        c {data_type}
    )
"""
)
con.execute(
    f"""
    INSERT INTO {tbl_name} VALUES
        (1,1,1),
        (10,10,10),
        (100,10,100),
        (NULL,NULL,NULL)
"""
)
# duck_tbl = con.table(tbl_name)
arrow_table = con.execute("FROM T").arrow()

print(con.execute("select count(*) FROM arrow_table where a = 1").fetchall())
