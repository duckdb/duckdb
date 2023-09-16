import * as duckdb from '..';
import * as assert from 'assert';

describe('Column Types', function() {
    let db: duckdb.Database;
    before(function(done) { db = new duckdb.Database(':memory:', done); });

    it('should prepare a statement and return the columns and their types', function(done) {
      // we dont include the large_enum and small_enum since they are huge and test the same code path as the small_enum
      var stmt = db.prepare("SELECT * EXCLUDE(medium_enum, large_enum) FROM test_all_types()", function(err: null | Error) {
          if (err) throw err;

          let cols = stmt.columns();
          
          assert.equal(cols.length, 42);

          var expected = [
            { name: 'bool', type: { id: 'BOOLEAN',  sql_type: 'BOOLEAN' } },
            { name: 'tinyint', type: { id: 'TINYINT', sql_type: 'TINYINT' } },
            { name: 'smallint', type: { id: 'SMALLINT', sql_type: 'SMALLINT' } },
            { name: 'int', type: { id: 'INTEGER', sql_type: 'INTEGER' } },
            { name: 'bigint', type: { id: 'BIGINT', sql_type: 'BIGINT' } },
            { name: 'hugeint', type: { id: 'HUGEINT', sql_type: 'HUGEINT' } },
            { name: 'utinyint', type: { id: 'UTINYINT', sql_type: 'UTINYINT' } },
            { name: 'usmallint', type: { id: 'USMALLINT', sql_type: 'USMALLINT' } },
            { name: 'uint', type: { id: 'UINTEGER', sql_type: 'UINTEGER' } },
            { name: 'ubigint', type: { id: 'UBIGINT', sql_type: 'UBIGINT' } },
            { name: 'date', type: { id: 'DATE', sql_type: 'DATE' } },
            { name: 'time', type: { id: 'TIME', sql_type: 'TIME' } },
            { name: 'timestamp', type: { id: 'TIMESTAMP', sql_type: 'TIMESTAMP' } },
            { name: 'timestamp_s', type: { id: 'TIMESTAMP_S', sql_type: 'TIMESTAMP_S' } },
            { name: 'timestamp_ms', type: { id: 'TIMESTAMP_MS', sql_type: 'TIMESTAMP_MS' } },
            { name: 'timestamp_ns', type: { id: 'TIMESTAMP_NS', sql_type: 'TIMESTAMP_NS' } },
            { name: 'time_tz', type: { id: 'TIME WITH TIME ZONE', sql_type: 'TIME WITH TIME ZONE' } },
            { name: 'timestamp_tz', type: { id: 'TIMESTAMP WITH TIME ZONE', sql_type: 'TIMESTAMP WITH TIME ZONE' } },
            { name: 'float', type: { id: 'FLOAT', sql_type: 'FLOAT' } },
            { name: 'double', type: { id: 'DOUBLE', sql_type: 'DOUBLE' } },
            { name: 'dec_4_1', type: { id: 'DECIMAL', sql_type: 'DECIMAL(4,1)', width: 4, scale: 1 } },
            { name: 'dec_9_4', type: { id: 'DECIMAL', sql_type: 'DECIMAL(9,4)', width: 9, scale: 4 } },
            { name: 'dec_18_6', type: { id: 'DECIMAL', sql_type: 'DECIMAL(18,6)', width: 18, scale: 6 } },
            { name: 'dec38_10', type: { id: 'DECIMAL', sql_type: 'DECIMAL(38,10)', width: 38, scale: 10 } },
            { name: 'uuid', type: { id: 'UUID', sql_type: 'UUID' } },
            { name: 'interval', type: { id: 'INTERVAL', sql_type: 'INTERVAL' } },
            { name: 'varchar', type: { id: 'VARCHAR', sql_type: 'VARCHAR' } },
            { name: 'blob', type: { id: 'BLOB', sql_type: 'BLOB' } },
            { name: 'bit', type: { id: 'BIT', sql_type: 'BIT' } },
            {
              name: 'small_enum',
              type: {
                id: 'ENUM',
                sql_type: "ENUM('DUCK_DUCK_ENUM', 'GOOSE')",
                values: [
                  "DUCK_DUCK_ENUM",
                  "GOOSE"
                ]
              }
            },
            {
              name: 'int_array',
              type: { 
                id: 'LIST', 
                sql_type: 'INTEGER[]', 
                child: {
                  id: 'INTEGER',
                  sql_type: 'INTEGER'
                } 
              }
            },
            {
              name: 'double_array',
              type: { 
                id: 'LIST', 
                sql_type: 'DOUBLE[]', 
                child: {
                  id: 'DOUBLE',
                  sql_type: 'DOUBLE'
                } 
              }
            },
            {
              name: 'date_array',
              type: { 
                id: 'LIST', 
                sql_type: 'DATE[]', 
                child: {
                  id: 'DATE',
                  sql_type: 'DATE'
                } 
              }
            },
            {
              name: 'timestamp_array',
              type: { 
                id: 'LIST', 
                sql_type: 'TIMESTAMP[]', 
                child: {
                  id: 'TIMESTAMP',
                  sql_type: 'TIMESTAMP'
                } 
              }
            },
            {
              name: 'timestamptz_array',
              type: {
                id: 'LIST',
                sql_type: 'TIMESTAMP WITH TIME ZONE[]',
                child: {
                  id: 'TIMESTAMP WITH TIME ZONE',
                  sql_type: 'TIMESTAMP WITH TIME ZONE'
                }
              }
            },
            {
              name: 'varchar_array',
              type: { 
                id: 'LIST', 
                sql_type: 'VARCHAR[]', 
                child: {
                  id: 'VARCHAR',
                  sql_type: 'VARCHAR'
                }
              }  
            },
            {
              name: 'nested_int_array',
              type: { 
                id: 'LIST', 
                sql_type: 'INTEGER[][]', 
                child: {
                  id: 'LIST',
                  sql_type: 'INTEGER[]',
                  child: {
                    id: 'INTEGER',
                    sql_type: 'INTEGER'
                  }
                } 
              }
            },
            {
              name: 'struct',
              type: {
                id: 'STRUCT',
                sql_type: 'STRUCT(a INTEGER, b VARCHAR)',
                children: [
                  { 
                    name: 'a',
                    type: {
                      id: 'INTEGER',
                      sql_type: 'INTEGER'
                    }
                  },
                  {
                    name: 'b',
                    type: {
                      id: 'VARCHAR',
                      sql_type: 'VARCHAR'
                    }
                  }
                ]
              }
            },
            {
              name: 'struct_of_arrays',
              type: {
                id: 'STRUCT',
                sql_type: 'STRUCT(a INTEGER[], b VARCHAR[])',
                children: [
                  {
                    name: 'a',
                    type: {
                      id: 'LIST',
                      sql_type: 'INTEGER[]',
                      child: {
                        id: 'INTEGER',
                        sql_type: 'INTEGER'
                      }
                    }
                  },
                  {
                    name: 'b',
                    type: {
                      id: 'LIST',
                      sql_type: 'VARCHAR[]',
                      child: {
                        id: 'VARCHAR',
                        sql_type: 'VARCHAR'
                      }
                    }
                  }
                ]
              }
            },
            {
              name: 'array_of_structs',
              type: {
                id: 'LIST',
                sql_type: 'STRUCT(a INTEGER, b VARCHAR)[]',
                child: {
                  id: 'STRUCT',
                  sql_type: 'STRUCT(a INTEGER, b VARCHAR)',
                  children: [
                    {
                      name: 'a',
                      type: {
                        id: 'INTEGER',
                        sql_type: 'INTEGER'
                      }
                    },
                    {
                      name: 'b',
                      type: {
                        id: 'VARCHAR',
                        sql_type: 'VARCHAR'
                      }
                    }
                  ]
                }
              }
            },
            {
              name: 'map',
              type: {
                id: 'MAP',
                sql_type: 'MAP(VARCHAR, VARCHAR)',
                key: {
                  id: 'VARCHAR',
                  sql_type: 'VARCHAR'
                },
                value: {
                  id: 'VARCHAR',
                  sql_type: 'VARCHAR'
                }
              }
            },
            {
              name: "union",
              type: {
                id: "UNION",
                sql_type: "UNION(name VARCHAR, age SMALLINT)",
                children: [
                  {
                    name: "name",
                    type: {
                      id: "VARCHAR",
                      sql_type: "VARCHAR"
                    }
                  },
                  {
                    name: "age",
                    type: {
                      id: "SMALLINT",
                      sql_type: "SMALLINT",
                    }
                  }
                ],
              }
            }
        ]

        assert.deepEqual(cols, expected);

      });
      stmt.finalize(done);
  });
});