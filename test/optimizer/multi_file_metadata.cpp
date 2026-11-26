#include "catch.hpp"
#include "test_helpers.hpp"

#include "duckdb/common/multi_file/table_function_multi_file.hpp"
#include "duckdb/main/extension_helper.hpp"
#include "duckdb/optimizer/optimizer.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/parser/statement/logical_plan_statement.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/planner.hpp"
#include "duckdb/storage/statistics/numeric_stats.hpp"

using namespace duckdb;

namespace {

LogicalGet &MetadataGet(LogicalOperator &op) {
	if (op.type == LogicalOperatorType::LOGICAL_GET) {
		return op.Cast<LogicalGet>();
	}
	REQUIRE(op.children.size() == 1);
	return MetadataGet(*op.children[0]);
}

optional_ptr<LogicalGet> FilterConditionedStatisticsGet(LogicalOperator &op) {
	if (op.type == LogicalOperatorType::LOGICAL_GET) {
		auto &get = op.Cast<LogicalGet>();
		if (get.function.prepare_filter_conditioned_statistics) {
			return get;
		}
	}
	for (auto &child : op.children) {
		auto result = FilterConditionedStatisticsGet(*child);
		if (result) {
			return result;
		}
	}
	return nullptr;
}

} // namespace

TEST_CASE("Parquet metadata survives consumption of the initial reader", "[optimizer][multi_file_metadata]") {
	DuckDB db;
	if (ExtensionHelper::LoadExtension(db, "parquet") != ExtensionLoadResult::LOADED_EXTENSION) {
		SKIP_TEST("require parquet");
	}
	Connection con(db);
	auto path = "'" + StringUtil::Replace(TestCreatePath("retained_metadata.parquet"), "'", "''") + "'";
	REQUIRE_NO_FAIL(con.Query("COPY (SELECT * FROM (VALUES (1,'a'),(2,'abc'),(3,NULL)) t(k,s)) TO " + path));
	con.BeginTransaction();
	for (bool union_by_name : {false, true}) {
		CAPTURE(union_by_name);
		for (bool consume_reader : {false, true}) {
			CAPTURE(consume_reader);
			Parser parser(*con.context);
			parser.ParseQuery("SELECT strlen(s),k,file_row_number FROM read_parquet(" + path +
			                  ",file_row_number=true,union_by_name=" + (union_by_name ? "true" : "false") +
			                  ") ORDER BY k");
			Planner planner(*con.context);
			planner.CreatePlan(std::move(parser.statements[0]));
			planner.plan->ResolveOperatorTypes();
			auto &get = MetadataGet(*planner.plan);
			auto &data = get.bind_data.write().Cast<MultiFileBindData>();
			REQUIRE(data.initial_reader);
			if (consume_reader) {
				data.initial_reader.reset();
			}
			for (auto column : {ColumnIndex(0), ColumnIndex(data.reader_bind.file_row_number_idx.GetIndex())}) {
				TableFunctionGetStatisticsInput input(get.bind_data.get(), column);
				REQUIRE(get.function.statistics_extended(*con.context, input));
			}
			GetPartitionStatsInput input(get.function, get.bind_data.get());
			auto partitions = get.function.get_partition_stats(*con.context, input);
			REQUIRE(partitions.size() == 1);
			REQUIRE(partitions[0].partition_row_group);
			auto stats = partitions[0].partition_row_group->GetColumnStatistics(StorageIndex(0));
			REQUIRE(stats);
			REQUIRE(NumericStats::Min(*stats) == Value(1));
			REQUIRE(NumericStats::Max(*stats) == Value(3));
			Optimizer optimizer(*planner.binder, *con.context);
			planner.plan = optimizer.Optimize(std::move(planner.plan));
			REQUIRE(MetadataGet(*planner.plan).has_pushed_projection == !union_by_name);
			auto result = con.Query(make_uniq<LogicalPlanStatement>(std::move(planner.plan)));
			REQUIRE_NO_FAIL(*result);
			REQUIRE(CHECK_COLUMN(*result, 0, {1, 3, Value()}));
			REQUIRE(CHECK_COLUMN(*result, 1, {1, 2, 3}));
			REQUIRE(CHECK_COLUMN(*result, 2, {0, 1, 2}));
		}
	}
	con.Rollback();
}

TEST_CASE("Late Parquet footer preparation scopes bind mutation and reader ownership",
          "[optimizer][multi_file_metadata]") {
	DuckDB db;
	if (ExtensionHelper::LoadExtension(db, "parquet") != ExtensionLoadResult::LOADED_EXTENSION) {
		SKIP_TEST("require parquet");
	}
	Connection con(db);
	auto first_path = "'" + StringUtil::Replace(TestCreatePath("late_footer_first.parquet"), "'", "''") + "'";
	auto second_path = "'" + StringUtil::Replace(TestCreatePath("late_footer_second.parquet"), "'", "''") + "'";
	REQUIRE_NO_FAIL(
	    con.Query("COPY (SELECT range::BIGINT k, (range % 2)::INTEGER g FROM range(0, 10)) TO " + first_path));
	REQUIRE_NO_FAIL(
	    con.Query("COPY (SELECT range::BIGINT k, (range % 2)::INTEGER g FROM range(10, 20)) TO " + second_path));
	con.BeginTransaction();

	struct TestCase {
		string filter;
		optional_idx maximum_compressed_bytes;
		idx_t expected_readers;
	};
	vector<TestCase> cases {{"p.g > 0", {}, 1}, {"p.g = 1", {}, 2}, {"p.g = 1", optional_idx(0), 2}};
	for (auto &test : cases) {
		CAPTURE(test.filter, test.expected_readers);
		if (test.maximum_compressed_bytes.IsValid()) {
			REQUIRE_NO_FAIL(con.Query("SET parquet_filter_conditioned_statistics_max_compressed_bytes=" +
			                          to_string(test.maximum_compressed_bytes.GetIndex())));
		} else {
			REQUIRE_NO_FAIL(con.Query("RESET parquet_filter_conditioned_statistics_max_compressed_bytes"));
		}
		Parser parser(*con.context);
		parser.ParseQuery("SELECT count(*) FROM range(20) fact(k) JOIN read_parquet([" + first_path + "," +
		                  second_path + "]) p USING(k) WHERE " + test.filter);
		Planner planner(*con.context);
		planner.CreatePlan(std::move(parser.statements[0]));
		planner.plan->ResolveOperatorTypes();
		auto get = FilterConditionedStatisticsGet(*planner.plan);
		REQUIRE(get);
		auto &before = get->bind_data->Cast<MultiFileBindData>();
		REQUIRE(before.initial_reader);
		REQUIRE(before.union_readers.size() == 1);
		REQUIRE_FALSE(before.union_readers[0]->reader);

		Optimizer optimizer(*planner.binder, *con.context);
		planner.plan = optimizer.Optimize(std::move(planner.plan));
		get = FilterConditionedStatisticsGet(*planner.plan);
		REQUIRE(get);
		auto &after = get->bind_data->Cast<MultiFileBindData>();
		REQUIRE(after.initial_reader);
		REQUIRE(after.union_readers.size() == test.expected_readers);
		REQUIRE_FALSE(after.union_readers[0]->reader);

		auto result = con.Query(make_uniq<LogicalPlanStatement>(std::move(planner.plan)));
		REQUIRE_NO_FAIL(*result);
		REQUIRE(CHECK_COLUMN(*result, 0, {10}));
	}
	con.Rollback();
}

TEST_CASE("Parquet row group statistics survive consumption of the initial reader",
          "[optimizer][multi_file_metadata]") {
	DuckDB db;
	if (ExtensionHelper::LoadExtension(db, "parquet") != ExtensionLoadResult::LOADED_EXTENSION) {
		SKIP_TEST("require parquet");
	}
	Connection con(db);
	auto path = "'" + StringUtil::Replace(TestCreatePath("retained_row_group.parquet"), "'", "''") + "'";
	REQUIRE_NO_FAIL(con.Query("COPY (SELECT * FROM (VALUES (1),(3)) t(k)) TO " + path));
	con.BeginTransaction();
	for (bool union_by_name : {false, true}) {
		CAPTURE(union_by_name);
		Parser parser(*con.context);
		parser.ParseQuery("SELECT min(k),max(k) FROM read_parquet(" + path +
		                  ",union_by_name=" + (union_by_name ? "true" : "false") + ")");
		Planner planner(*con.context);
		planner.CreatePlan(std::move(parser.statements[0]));
		auto &get = MetadataGet(*planner.plan);
		auto &data = get.bind_data.write().Cast<MultiFileBindData>();
		REQUIRE(data.initial_reader);
		data.initial_reader.reset();
		GetPartitionStatsInput input(get.function, get.bind_data.get());
		auto partitions = get.function.get_partition_stats(*con.context, input);
		REQUIRE(partitions.size() == 1);
		REQUIRE(partitions[0].partition_row_group);
		auto &row_group = *partitions[0].partition_row_group;
		auto stats = row_group.GetColumnStatistics(StorageIndex(0));
		REQUIRE(stats);
		REQUIRE(NumericStats::Min(*stats) == Value(1));
		REQUIRE(NumericStats::Max(*stats) == Value(3));
		REQUIRE(row_group.MinMaxIsExact(StorageIndex(0)));
	}
	con.Rollback();
}

TEST_CASE("Multi-file statistics use the live reader when union metadata has no statistics",
          "[optimizer][multi_file_metadata]") {
	DuckDB db;
	if (ExtensionHelper::LoadExtension(db, "parquet") != ExtensionLoadResult::LOADED_EXTENSION) {
		SKIP_TEST("require parquet");
	}
	Connection con(db);
	auto path = "'" + StringUtil::Replace(TestCreatePath("reader_only_metadata.parquet"), "'", "''") + "'";
	REQUIRE_NO_FAIL(con.Query("COPY (SELECT 42 AS i) TO " + path));
	con.BeginTransaction();
	Parser parser(*con.context);
	parser.ParseQuery("SELECT * FROM read_parquet(" + path + ",union_by_name=true)");
	Planner planner(*con.context);
	planner.CreatePlan(std::move(parser.statements[0]));
	auto &get = MetadataGet(*planner.plan);
	auto &data = get.bind_data.write().Cast<MultiFileBindData>();
	REQUIRE(data.initial_reader);
	REQUIRE(data.initial_reader->GetStatistics(*con.context, Identifier("i")));
	// Some providers retain only a live reader and metadata without a statistics callback.
	REQUIRE(data.union_readers.size() == 1);
	data.union_readers[0] = make_shared_ptr<BaseUnionData>(data.union_readers[0]->file);
	TableFunctionGetStatisticsInput input(get.bind_data.get(), ColumnIndex(0));
	REQUIRE(get.function.statistics_extended(*con.context, input));
	con.Rollback();
}
