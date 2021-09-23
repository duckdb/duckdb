#include "dsdgen.hpp"

#include "append_info-c.hpp"
#include "dsdgen_helpers.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/storage/data_table.hpp"
#include "tpcds_constants.hpp"

#include <cassert>

using namespace duckdb;
using namespace std;

namespace tpcds {

void DSDGenWrapper::DSDGen(double scale, ClientContext &context, string schema, string suffix) {
	Connection con(*context.db);

	con.Query("BEGIN TRANSACTION");
	// FIXME: no schema/suffix support yet

	for (int t = 0; t < TPCDS_TABLE_COUNT; t++) {
		con.Query(TPCDS_TABLE_DDL_NOKEYS[t]);
	}

	con.Query("COMMIT");

	if (scale <= 0) {
		// schema only
		return;
	}

	InitializeDSDgen(scale);

	// populate append info
	vector<unique_ptr<tpcds_append_information>> append_info;
	append_info.resize(DBGEN_VERSION);

	int tmin = CALL_CENTER, tmax = DBGEN_VERSION;

	for (int table_id = tmin; table_id < tmax; table_id++) {
		auto table_def = GetTDefByNumber(table_id);
		assert(table_def.name);
		auto append = make_unique<tpcds_append_information>(con, schema, table_def.name);
		append->table_def = table_def;
		append_info[table_id] = move(append);
	}

	// actually generate tables using modified data generator functions
	for (int table_id = tmin; table_id < tmax; table_id++) {
		// child tables are created in parent loaders
		if (append_info[table_id]->table_def.fl_child) {
			continue;
		}

		ds_key_t k_row_count = GetRowCount(table_id), k_first_row = 1;

		// TODO: verify this is correct and required here
		/*
		 * small tables use a constrained set of geography information
		 */
		if (append_info[table_id]->table_def.fl_small) {
			ResetCountCount();
		}

		auto builder_func = GetTDefFunctionByNumber(table_id);
		assert(builder_func);

		for (ds_key_t i = k_first_row; k_row_count; i++, k_row_count--) {
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

uint32_t DSDGenWrapper::QueriesCount() {
	return TPCDS_QUERIES_COUNT;
}

string DSDGenWrapper::GetQuery(int query) {
	if (query <= 0 || query > TPCDS_QUERIES_COUNT) {
		throw SyntaxException("Out of range TPC-DS query number %d", query);
	}
	return TPCDS_QUERIES[query - 1];
}

string DSDGenWrapper::GetAnswer(double sf, int query) {
	if (query <= 0 || query > TPCDS_QUERIES_COUNT) {
		throw SyntaxException("Out of range TPC-DS query number %d", query);
	}

	if (sf == 0.01) {
		return TPCDS_ANSWERS_SF0_01[query - 1];
	} else if (sf == 1) {
		return TPCDS_ANSWERS_SF1[query - 1];
	} else {
		throw NotImplementedException("Don't have TPC-DS answers for SF %llf!", sf);
	}
}

} // namespace tpcds
