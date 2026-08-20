#include "catch.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/function/partition_stats.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "test_helpers.hpp"

using namespace duckdb;

namespace {

struct NonPushdownPartitionFunction {
	struct BindData : public TableFunctionData {
		vector<column_t> expected_partition_ids;
	};

	static unique_ptr<FunctionData> Bind(ClientContext &, TableFunctionBindInput &input,
	                                     vector<LogicalType> &return_types, vector<Identifier> &names) {
		return_types = {LogicalType::VARCHAR, LogicalType::BIGINT, LogicalType::DOUBLE};
		names = {"region", "year", "value"};
		auto result = make_uniq<BindData>();
		result->expected_partition_ids.push_back(UnsafeNumericCast<column_t>(input.inputs[0].GetValue<int64_t>()));
		auto second_id = input.inputs[1].GetValue<int64_t>();
		if (second_id >= 0) {
			result->expected_partition_ids.push_back(UnsafeNumericCast<column_t>(second_id));
		}
		return std::move(result);
	}

	static void Scan(ClientContext &, TableFunctionInput &, DataChunk &output) {
		output.SetChildCardinality(0);
	}

	static TablePartitionInfo GetPartitionInfo(ClientContext &, TableFunctionPartitionInput &input) {
		auto &bind_data = input.bind_data->Cast<BindData>();
		if (input.partition_ids != bind_data.expected_partition_ids) {
			throw InternalException("Partition columns did not resolve to the expected base columns");
		}
		return TablePartitionInfo::SINGLE_VALUE_PARTITIONS;
	}

	static void Register(Connection &con) {
		con.BeginTransaction();
		auto &catalog = Catalog::GetSystemCatalog(*con.context);
		TableFunction function("non_pushdown_partitions", {LogicalType::BIGINT, LogicalType::BIGINT}, Scan, Bind);
		function.projection_pushdown = false;
		function.get_partition_info = GetPartitionInfo;
		CreateTableFunctionInfo info(function);
		catalog.CreateTableFunction(*con.context, info);
		con.Commit();
	}
};

} // namespace

TEST_CASE("Resolve partition columns for table functions without projection pushdown", "[tablefunction]") {
	DuckDB db(nullptr);
	Connection con(db);
	NonPushdownPartitionFunction::Register(con);

	auto result = con.Query("EXPLAIN SELECT year, COUNT(*) FROM non_pushdown_partitions(1, -1) GROUP BY year");
	REQUIRE(!result->HasError());
	REQUIRE(StringUtil::Contains(result->ToString(), "Partitioned Aggregate"));

	result = con.Query("EXPLAIN SELECT year, COUNT(*) FROM (SELECT year FROM non_pushdown_partitions(1, -1) "
	                   "WHERE region = 'x') GROUP BY year");
	REQUIRE(!result->HasError());
	REQUIRE(StringUtil::Contains(result->ToString(), "Partitioned Aggregate"));

	result = con.Query("EXPLAIN SELECT year, region, COUNT(*) FROM non_pushdown_partitions(1, 0) "
	                   "GROUP BY year, region");
	REQUIRE(!result->HasError());
	REQUIRE(StringUtil::Contains(result->ToString(), "Partitioned Aggregate"));

	result = con.Query("EXPLAIN SELECT COUNT(*) OVER (PARTITION BY year) FROM non_pushdown_partitions(1, -1)");
	REQUIRE(!result->HasError());
}
