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
	con.context.transaction.BeginTransaction();
	auto &transaction = con.context.ActiveTransaction();

	// FIXME: No restart support yet, suspect only fix is init_rand
	// FIXME: no schema/suffix support yet

	for (int t = 0; t < TPCDS_TABLE_COUNT; t++) {
		con.GetQueryResult(con.context, TPCDS_TABLE_DDL_NOKEYS[t]);
	}

	if (flt_scale == 0) {
		// schema only
		con.context.transaction.Commit();
		return;
	}

	init_params(); // among other set random seed
	init_rand();   // no random numbers without this

	// populate append info
	auto append_info = unique_ptr<tpcds_append_information[]>(
	    new tpcds_append_information[DBGEN_VERSION]);

	int tmin = CALL_CENTER, tmax = DBGEN_VERSION; // because fuck dbgen_version

	for (int table_id = tmin; table_id < tmax; table_id++) {
		tdef *table_def = getSimpleTdefsByNumber(table_id);
		assert(table_def);
		assert(table_def->name);

		append_info[table_id].table_def = table_def;
		append_info[table_id].row = 0;
		append_info[table_id].chunk.Reset();
		append_info[table_id].context = &con.context;
		append_info[table_id].table =
		    db.catalog.GetTable(transaction, DEFAULT_SCHEMA, table_def->name);
	}

	// actually generate tables using modified data generator functions
	for (int table_id = tmin; table_id < tmax; table_id++) {
		// child tables are created in parent loaders
		if (append_info[table_id].table_def->flags & FL_CHILD) {
			continue;
		}

		ds_key_t kRowCount = get_rowcount(table_id), kFirstRow = 1;

		// TODO: verify this is correct and required here
		/*
		 * small tables use a constrained set of geography information
		 */
		if (append_info[table_id].table_def->flags & FL_SMALL) {
			resetCountCount();
		}

		table_func_t *table_funcs = getTdefFunctionsByNumber(table_id);
		assert(table_funcs);

		for (ds_key_t i = kFirstRow; kRowCount; i++, kRowCount--) {
			// append happens directly in builders since they dump child tables
			// immediately
			if (table_funcs->builder((void *)append_info.get(), i)) {
				throw Exception("Table generation failed");
			}
		}
	}

	// flush any incomplete chunks
	for (int table_id = tmin; table_id < tmax; table_id++) {
		if (append_info[table_id].table) {
			if (append_info[table_id].chunk.size() > 0) {
				append_info[table_id].chunk.Verify();
				append_info[table_id].table->storage->Append(
				    *append_info[table_id].table,
				    *append_info[table_id].context,
				    append_info[table_id].chunk);
			}
		}
	}

	con.context.transaction.Commit();
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
	return "";
}

} // namespace tpcds
