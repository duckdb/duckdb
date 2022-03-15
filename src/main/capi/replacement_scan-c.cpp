#include "duckdb/main/capi_internal.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/parser/tableref/table_function_ref.hpp"

namespace duckdb {

struct CAPIReplacementScanInfo : public ReplacementScanData {
	~CAPIReplacementScanInfo() {
		if (delete_callback) {
			delete_callback(extra_data);
		}
	}

	duckdb_replacement_callback_t callback;
	void *extra_data;
	duckdb_delete_callback_t delete_callback;
};

unique_ptr<TableFunctionRef> duckdb_capi_replacement_callback(const string &table_name, ReplacementScanData *data) {
	auto &info = (CAPIReplacementScanInfo &) *data;
	return nullptr;
}

}

void duckdb_add_replacement_scan(duckdb_database db, duckdb_replacement_callback_t replacement, void *extra_data, duckdb_delete_callback_t delete_callback) {
	if (!db || !replacement) {
		return;
	}
	auto wrapper = (duckdb::DatabaseData *) db;
	auto scan_info = duckdb::make_unique<duckdb::CAPIReplacementScanInfo>();
	scan_info->callback = replacement;
	scan_info->extra_data = extra_data;
	scan_info->delete_callback = delete_callback;

	auto &config = duckdb::DBConfig::GetConfig(*wrapper->database->instance);
	config.replacement_scans.push_back(duckdb::ReplacementScan(duckdb::duckdb_capi_replacement_callback, move(scan_info)));
}

void duckdb_replacement_scan_set_function_name(duckdb_replacement_scan_info info, const char *function_name) {
	if (!info || !function_name) {
		return;
	}
}

void duckdb_replacement_scan_add_parameter(duckdb_replacement_scan_info info, duckdb_value parameter) {
	if (!info || !parameter) {
		return;
	}
}

