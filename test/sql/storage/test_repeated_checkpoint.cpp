#include "catch.hpp"
#include "duckdb/common/file_system.hpp"
#include "test_helpers.hpp"
#include "duckdb/storage/storage_info.hpp"

#include <fstream>

using namespace duckdb;
using namespace std;

TEST_CASE("Test repeated load and checkpoint of storage", "[storage][.]") {
	duckdb::unique_ptr<MaterializedQueryResult> result;
	auto storage_database = TestCreatePath("repeated_load");
	auto csv_file = TestCreatePath("rload.csv");
	auto config = GetTestConfig();

	duckdb::vector<string> model {"M11", "F22", "U33"};
	duckdb::vector<string> shop {"www.goodshop.com", "www.badshop.com"};
	duckdb::vector<string> name {"Electronics  Something  One",   "Electronics  Something  Two",
	                             "Electronics  Something  Three", "Electronics  Something  Four",
	                             "Electronics  Something  Five",  "Electronics  Something  Six",
	                             "Electronics  Something  Seven", "Electronics  Something  Eight",
	                             "Electronics  Something  Nine",  "Electronics  Something  Ten"};
	duckdb::vector<string> brand {"AAAAA", "BBBBB", "CCCC", "DDDDDD", "PPPP"};
	duckdb::vector<string> color {"violet", "indigo", "blue", "green", "yellow", "orange", "red"};
	idx_t row_count = 1000;
	const idx_t checkpoint_loop_count = 100;

	DeleteDatabase(storage_database);
	for (idx_t counter = 0; counter < checkpoint_loop_count; counter++) {
		DuckDB db(storage_database);
		Connection con(db);

		if (counter > 0) {
			result = con.Query("SELECT COUNT(*) FROM pdata");
			REQUIRE(CHECK_COLUMN(result, 0, {Value::BIGINT(counter * row_count)}));
		}
		// generate the csv file
		ofstream csv_writer(csv_file);
		for (idx_t i = 0; i < row_count; i++) {
			idx_t z = i + counter;
			idx_t record_id = i + (row_count * counter);
			csv_writer << record_id << "|";
			csv_writer << i % 99 << "|";
			csv_writer << shop[z % 2] << "|";
			csv_writer << "electronics"
			           << "|";
			csv_writer << name[z % 10] << "|";
			csv_writer << brand[z % 5] << "|";
			csv_writer << color[z % 7] << "|";
			csv_writer << model[z % 3] << "|";
			csv_writer << "\n";
		}
		csv_writer.close();
		// create and load the table
		REQUIRE_NO_FAIL(
		    con.Query("CREATE TABLE IF NOT EXISTS pdata (record_id BIGINT PRIMARY KEY , price DOUBLE, shop VARCHAR, "
		              "category VARCHAR, name VARCHAR, brand VARCHAR, color VARCHAR, model VARCHAR);"));
		REQUIRE_NO_FAIL(con.Query("COPY pdata(record_id,price,shop,category,name,brand,color,model) FROM '" + csv_file +
		                          "' ( DELIMITER '|' );"));
		result = con.Query("SELECT MIN(record_id), MIN(price), MIN(shop), MIN(category), MIN(name), MIN(brand), "
		                   "MIN(color), MIN(model) FROM pdata");
		REQUIRE(CHECK_COLUMN(result, 0, {0}));
		REQUIRE(CHECK_COLUMN(result, 1, {0}));
		REQUIRE(CHECK_COLUMN(result, 2, {"www.badshop.com"}));
		REQUIRE(CHECK_COLUMN(result, 3, {"electronics"}));
		REQUIRE(CHECK_COLUMN(result, 4, {"Electronics  Something  Eight"}));
		REQUIRE(CHECK_COLUMN(result, 5, {"AAAAA"}));
		REQUIRE(CHECK_COLUMN(result, 6, {"blue"}));
		REQUIRE(CHECK_COLUMN(result, 7, {"F22"}));

		result = con.Query("SELECT COUNT(*) FROM pdata");
		REQUIRE(CHECK_COLUMN(result, 0, {Value::BIGINT((counter + 1) * row_count)}));
	}
}
