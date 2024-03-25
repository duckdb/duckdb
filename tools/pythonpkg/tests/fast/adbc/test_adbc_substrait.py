import adbc_driver_duckdb.dbapi
from google.protobuf import json_format
import pyarrow
import adbc_driver_manager
from substrait.gen.proto import plan_pb2


PLAN_PROTOTEXT = """
{
   "relations":[
      {
         "root":{
            "input":{
               "project":{
                  "input":{
                     "read":{
                        "baseSchema":{
                           "names":[
                              "mbid",
                              "artist_mb"
                           ],
                           "struct":{
                              "types":[
                                 {
                                    "string":{
                                       "nullability":"NULLABILITY_NULLABLE"
                                    }
                                 },
                                 {
                                    "string":{
                                       "nullability":"NULLABILITY_NULLABLE"
                                    }
                                 }
                              ],
                              "nullability":"NULLABILITY_REQUIRED"
                           }
                        },
                        "projection":{
                           "select":{
                              "structItems":[
                                 {
                                    
                                 },
                                 {
                                    "field":1
                                 }
                              ]
                           },
                           "maintainSingularStruct":true
                        },
                        "localFiles":{
                           "items":[
                              {
                                 "uriFile":"/Users/holanda/Documents/Projects/duckdb/build/release/somefile.parquet",
                                 "parquet":{
                                    
                                 }
                              }
                           ]
                        }
                     }
                  },
                  "expressions":[
                     {
                        "selection":{
                           "directReference":{
                              "structField":{
                                 
                              }
                           },
                           "rootReference":{
                              
                           }
                        }
                     },
                     {
                        "selection":{
                           "directReference":{
                              "structField":{
                                 "field":1
                              }
                           },
                           "rootReference":{
                              
                           }
                        }
                     }
                  ]
               }
            },
            "names":[
               "mbid",
               "artist_mb"
            ]
         }
      }
   ],
   "version":{
      "minorNumber":39,
      "producer":"DuckDB"
   }
}
"""


def _import(handle):
    """Helper to import a C Data Interface handle."""
    return pyarrow.RecordBatchReader._import_from_c(handle.address)


def _bind(stmt, batch):
    array = adbc_driver_manager.ArrowArrayHandle()
    schema = adbc_driver_manager.ArrowSchemaHandle()
    batch._export_to_c(array.address, schema.address)
    stmt.bind(array, schema)


def execute_with_duckdb_over_adbc(plan: 'plan_pb2.Plan') -> pyarrow.lib.Table:
    """Executes the given Substrait plan against DuckDB using ADBC."""
    with adbc_driver_duckdb.dbapi.connect() as conn, conn.cursor() as cur:
        cur.execute(
            "LOAD '/Users/holanda/Documents/Projects/duckdblabs/substrait/duckdb/build/release/extension/substrait/substrait.duckdb_extension';"
        )
        plan_data = plan.SerializeToString()
        cur.adbc_statement.set_substrait_plan(plan_data)
        res = cur.adbc_statement.execute_query()
        table = _import(res[0]).read_all()
        return table


if __name__ == '__main__':
    plan = json_format.Parse(PLAN_PROTOTEXT, plan_pb2.Plan())
    table = execute_with_duckdb_over_adbc(plan)
    print(table)
