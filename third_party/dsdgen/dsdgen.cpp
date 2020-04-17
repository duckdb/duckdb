#include "dsdgen.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/storage/data_table.hpp"
#include "tpcds_constants.hpp"
#include "append_info.hpp"
#include "dsdgen_helpers.hpp"

using namespace duckdb;
using namespace std;

namespace tpcds {

void dbgen(double flt_scale, DuckDB &db, string schema, string suffix) {
	Connection con(db);

	con.Query("BEGIN TRANSACTION");
	// FIXME: No restart support yet, suspect only fix is init_rand
	// FIXME: no schema/suffix support yet

	for (int t = 0; t < TPCDS_TABLE_COUNT; t++) {
		con.Query(TPCDS_TABLE_DDL_NOKEYS[t]);
	}

	con.Query("COMMIT");

	if (flt_scale == 0) {
		// schema only
		return;
	}

	InitializeDSDgen();

	// populate append info
	vector<unique_ptr<tpcds_append_information>> append_info;
	append_info.resize(DBGEN_VERSION);

	int tmin = CALL_CENTER, tmax = DBGEN_VERSION;

	for (int table_id = tmin; table_id < tmax; table_id++) {
		auto table_def = GetTDefByNumber(table_id);
		assert(table_def.name);
		auto append = make_unique<tpcds_append_information>(db, schema, table_def.name);
		append->table_def = table_def;
		append_info[table_id] = move(append);
	}

	// actually generate tables using modified data generator functions
	for (int table_id = tmin; table_id < tmax; table_id++) {
		// child tables are created in parent loaders
		if (append_info[table_id]->table_def.fl_child) {
			continue;
		}

		ds_key_t kRowCount = GetRowCount(table_id), kFirstRow = 1;

		// TODO: verify this is correct and required here
		/*
		 * small tables use a constrained set of geography information
		 */
		if (append_info[table_id]->table_def.fl_small) {
			ResetCountCount();
		}

		auto builder_func = GetTDefFunctionByNumber(table_id);
		assert(builder_func);

		for (ds_key_t i = kFirstRow; kRowCount; i++, kRowCount--) {
			// append happens directly in builders since they dump child tables
			// immediately
			if (builder_func((void *)&append_info, i)) {
				throw Exception("Table generation failed");
			}
		}
	}

	// flush any incomplete chunks
	for (int table_id = tmin; table_id < tmax; table_id++) {
		append_info[table_id]->appender.Close();
	}
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
