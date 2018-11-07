
#include "dsdgen.hpp"
#include "common/exception.hpp"
#include "main/client_context.hpp"
#include "common/types/data_chunk.hpp"

#include "storage/data_table.hpp"

#include "tpcds_constants.hpp"
#include "append_info.hpp"

using namespace duckdb;
using namespace std;

namespace tpcds {

typedef int64_t ds_key_t;

#define DECLARER
#include "build_support.h"
#include "params.h"

#include "tdefs.h"
#include "scaling.h"
#include "address.h"
#include "dist.h"
#include "genrand.h"

void dbgen(double flt_scale, DuckDB &db, string schema, string suffix) {
	DuckDBConnection con(db);
	ClientContext &context = con.context;

	// FIXME: use a transaction for the load
	// FIXME: no schema/suffix support yet
	for (int t = 0; t < TPCDS_TABLE_COUNT; t++) {
		con.Query(TPCDS_TABLE_DDL[t]);
	}

	if (flt_scale == 0) {
		// schema only
		context.transaction.Commit();
		return;
	}

	init_rand(); // no random numbers without this
	tdef *table_def;

	for (int table_id = CALL_CENTER;
	     (table_def = getSimpleTdefsByNumber(table_id)); table_id++) {
		if (!table_def->name)
			break;

		// child tables are created in parent loaders
		if (table_def->flags & FL_CHILD) {
			continue;
		}

		// TODO: verify this is correct and required here
		/*
		 * small tables use a constrained set of geography information
		 */
		if (table_def->flags & FL_SMALL) {
			resetCountCount();
		}

		tpcds_append_information append_info;
		append_info.row = 0;
		append_info.context = &context;
		append_info.table =
		    db.catalog.GetTable(context.transaction.ActiveTransaction(),
		                        DEFAULT_SCHEMA, table_def->name);

		// get function pointers for this table
		table_func_t *table_funcs = getTdefFunctionsByNumber(table_id);
		for (ds_key_t i = 1, kRowCount = get_rowcount(table_id); kRowCount;
		     i++, kRowCount--) {
			append_info.col = 0;

			/* not all rows that are built should be printed. Use return code to
			 * deterine output */
			if (!table_funcs->builder(NULL, i)) {
				if (table_funcs->loader[1]((void *)&append_info)) {
					throw Exception("Table generation failed");
				}
				append_info.row++;
			}
		}
		append_info.chunk.Print();

		// incomplete chunks
		if (append_info.chunk.size() > 0) {
			append_info.table->storage->Append(*append_info.context,
			                                   append_info.chunk);
		}
		return;
	}
	// context.transaction.Commit();
}

string get_query(int query) {
	if (query <= 0 || query > TPCDS_QUERIES_COUNT) {
		throw SyntaxException("Out of range TPC-DS query number %d", query);
	}
	return TPCDS_QUERIES[query - 1];
}

string get_answer(double sf, int query) {
	if (query <= 0 || query > TPCDS_QUERIES_COUNT) {
		throw SyntaxException("Out of range TPC-DS query number %d", query);
	}
	const char *answer;
	return "";
}

} // namespace tpcds
