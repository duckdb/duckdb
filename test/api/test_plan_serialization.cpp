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
	con.Query("CREATE TABLE a (i INTEGER)");
	con.Query("INSERT INTO a VALUES (42)");
	auto plan = con.ExtractPlan("SELECT i FROM a WHERE i >= 42");

	BufferedSerializer serializer;
	plan->Serialize(serializer);

	auto data = serializer.GetData();
	auto deserializer = BufferedDeserializer(data.data.get(), data.size);
	auto new_plan = LogicalOperator::Deserialize(deserializer);

	printf("%s\n", new_plan->ToString().c_str());
}
