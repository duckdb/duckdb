import os
import subprocess

duckdb_program = '/Users/myth/Programs/duckdb-bugfix/build/release/duckdb'

struct_def = '''struct $STRUCT_NAME {
	static constexpr char *Name = "$NAME";
	static const char *Columns[];
	static constexpr idx_t ColumnCount = $COLUMN_COUNT;
	static const LogicalType Types[];
	static constexpr idx_t PrimaryKeyCount = $PK_COLUMN_COUNT;
	static const char *PrimaryKeyColumns[];
};
'''

initcode = '''
call dsdgen(sf=0);
.mode csv
.header 0
'''

column_count_query = '''
select count(*) from pragma_table_info('$NAME');
'''

pk_column_count_query = '''
select count(*) from pragma_table_info('$NAME') where pk=true;
'''

gen_names = '''
select concat('const char *', '$STRUCT_NAME', '::Columns[] = {', STRING_AGG('"' || name || '"', ', ') || '};') from pragma_table_info('$NAME');
'''

gen_types = '''
select concat('const LogicalType ', '$STRUCT_NAME', '::Types[] = {', STRING_AGG('LogicalType::' || type, ', ') || '};') from pragma_table_info('$NAME');
'''

pk_columns = '''
select concat('const char *', '$STRUCT_NAME', '::PrimaryKeyColumns[] = {', STRING_AGG('"' || name || '"', ', ') || '};') from pragma_table_info('$NAME') where pk=true;
'''


def run_query(sql):
    input_sql = initcode + '\n' + sql
    res = subprocess.run(duckdb_program, input=input_sql.encode('utf8'), stdout=subprocess.PIPE, stderr=subprocess.PIPE)

    stdout = res.stdout.decode('utf8').strip()
    stderr = res.stderr.decode('utf8').strip()
    if res.returncode != 0:
        print("FAILED TO RUN QUERY")
        print(stderr)
        exit(1)
    return stdout


def prepare_query(sql, table_name, struct_name):
    return sql.replace('$NAME', table_name).replace('$STRUCT_NAME', struct_name)


header = '''
#pragma once

#include "duckdb.hpp"

#ifndef DUCKDB_AMALGAMATION
#include "duckdb/common/exception.hpp"
#include "duckdb/common/types/date.hpp"
#include "duckdb/parser/column_definition.hpp"
#include "duckdb/storage/data_table.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/planner/parsed_data/bound_create_table_info.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/parser/constraints/not_null_constraint.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/planner/binder.hpp"
#endif

namespace tpcds {

using duckdb::LogicalType;
using duckdb::idx_t;
'''

footer = '''
}
'''

print(header)

table_list = run_query('show tables')
for table_name in table_list.split('\n'):
    table_name = table_name.strip()
    print(
        '''
//===--------------------------------------------------------------------===//
// $NAME
//===--------------------------------------------------------------------===//'''.replace(
            '$NAME', table_name
        )
    )
    struct_name = str(table_name.title().replace('_', '')) + 'Info'
    column_count = int(run_query(prepare_query(column_count_query, table_name, struct_name)).strip())
    pk_column_count = int(run_query(prepare_query(pk_column_count_query, table_name, struct_name)).strip())
    print(
        prepare_query(struct_def, table_name, struct_name)
        .replace('$COLUMN_COUNT', str(column_count))
        .replace('$PK_COLUMN_COUNT', str(pk_column_count))
    )

    print(run_query(prepare_query(gen_names, table_name, struct_name)).replace('""', '"').strip('"'))
    print("")
    print(run_query(prepare_query(gen_types, table_name, struct_name)).strip('"'))
    print("")
    print(run_query(prepare_query(pk_columns, table_name, struct_name)).replace('""', '"').strip('"'))

print(footer)
