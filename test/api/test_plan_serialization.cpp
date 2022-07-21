#include "catch.hpp"
#include "test_helpers.hpp"
#include "duckdb/common/field_writer.hpp"
#include "duckdb/common/serializer/buffered_serializer.hpp"
#include "duckdb/common/serializer/buffered_deserializer.hpp"

#include <set>
#include <map>

using namespace duckdb;
using namespace std;

TEST_CASE("Test plan serialization", "[api]") {
	DuckDB db;
	Connection con(db);
	auto plan = con.ExtractPlan("SELECT email, last_name FROM parquet_scan('data/parquet-testing/userdata1.parquet')");

	BufferedSerializer serializer;
	plan->Serialize(serializer);

	auto data = serializer.GetData();
	auto deserializer = BufferedDeserializer(data.data.get(), data.size);
	auto new_plan = LogicalOperator::Deserialize(deserializer);

	printf("%s\n", new_plan->ToString().c_str());
	new_plan->Verify();
}
