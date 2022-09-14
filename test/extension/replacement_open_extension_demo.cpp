#define DUCKDB_EXTENSION_MAIN
#include "duckdb.hpp"

using namespace duckdb;

struct MyOpenData : ReplacementOpenData {
	string my_flag;
};

extern "C" {

DUCKDB_EXTENSION_API unique_ptr<ReplacementOpenData>
replacement_open_extension_demo_replacement_open_pre(DBConfig &config) {
	auto res = make_unique<MyOpenData>();
	res->my_flag = config.options.database_path;
	config.options.database_path.clear(); // meaning in-memory
	printf("pre\n");
	return res;
}

DUCKDB_EXTENSION_API void replacement_open_extension_demo_replacement_open_post(DatabaseInstance &db,
                                                                                ReplacementOpenData *open_data_p) {
	D_ASSERT(open_data_p);
	auto open_data = (MyOpenData *)open_data_p;
	auto &config = DBConfig::GetConfig(db);
	config.options.set_variables["my_new_option"] = Value(open_data->my_flag);
	printf("post\n");
}

DUCKDB_EXTENSION_API void replacement_open_extension_demo_init(duckdb::DatabaseInstance &db) {
	// NOP for this extension
}

DUCKDB_EXTENSION_API const char *replacement_open_extension_demo_version() {
	return DuckDB::LibraryVersion();
}
}
