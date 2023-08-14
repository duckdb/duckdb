
#include "duckdb.hpp"
#include "tpch_extension.hpp"

using namespace duckdb;

void handler() {
}

extern "C" {
void set_mallocs_remaining(size_t n);
}

int main(int argc, char *argv[]) {
	DuckDB db;
	db.LoadExtension<TpchExtension>();
	Connection con(db);
	con.Query("CALL dbgen(sf=0.1)");
	std::set_new_handler(handler);

	for (idx_t mallocs = atoi(argv[1]); mallocs < atoi(argv[2]); mallocs++) {
		printf("%llu\n", mallocs);
		set_mallocs_remaining(mallocs);

		try {

			Connection con(db);
			auto result = con.Query(TpchExtension::GetQuery(1));
			if (result->HasError()) {
				printf("%s\n", result->GetError().c_str());
			}

		} catch (std::bad_alloc &e) {
		}
	}
}
