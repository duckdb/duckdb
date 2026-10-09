#include "catch.hpp"
#include "test_helpers.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/execution/expression_executor_state.hpp"
#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/common/multi_file/multi_file_list.hpp"
#include "duckdb/common/multi_file/multi_file_states.hpp"
#include "duckdb/common/serializer/binary_serializer.hpp"
#include "duckdb/common/serializer/memory_stream.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/optimizer/optimizer_extension.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"
#include "duckdb/planner/filter/dynamic_filter.hpp"
#include "duckdb/planner/filter/null_filter.hpp"
#include "duckdb/planner/filter/table_filter_functions.hpp"
#include "duckdb/planner/operator/logical_top_n.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "duckdb/planner/operator/logical_sample.hpp"
#include "duckdb/planner/operator/logical_dummy_scan.hpp"
#include "duckdb/planner/operator/logical_aggregate.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"

using namespace duckdb;

namespace {

struct BoundCopyProbe : OptimizerExtensionInfo {
	bool enabled = false;
	idx_t copies = 1;
	idx_t copied_scans = 0;
	idx_t pruned_scans = 0;
	idx_t scans_with_normal_filters = 0;
};

static void CheckScanCopies(LogicalOperator &original, LogicalOperator &copy, BoundCopyProbe &probe) {
	REQUIRE(original.type == copy.type);
	REQUIRE(original.children.size() == copy.children.size());
	REQUIRE(original.has_estimated_cardinality == copy.has_estimated_cardinality);
	REQUIRE(original.estimated_cardinality == copy.estimated_cardinality);
	if (original.type == LogicalOperatorType::LOGICAL_GET) {
		auto &left = original.Cast<LogicalGet>();
		auto &right = copy.Cast<LogicalGet>();
		REQUIRE(left.bind_data.get() != right.bind_data.get());
		REQUIRE(left.table_index == right.table_index);
		REQUIRE(left.returned_types == right.returned_types);
		REQUIRE(left.names == right.names);
		REQUIRE(left.parameters == right.parameters);
		REQUIRE(left.named_parameters == right.named_parameters);
		REQUIRE(left.GetColumnIds() == right.GetColumnIds());
		REQUIRE(left.projection_ids == right.projection_ids);
		REQUIRE(left.table_filters.Equals(right.table_filters));
		REQUIRE(left.bind_data->Cast<MultiFileBindData>().file_list.get() !=
		        right.bind_data->Cast<MultiFileBindData>().file_list.get());
		auto left_files = left.bind_data->Cast<MultiFileBindData>().file_list->GetAllFiles();
		auto right_files = right.bind_data->Cast<MultiFileBindData>().file_list->GetAllFiles();
		REQUIRE(left_files.size() == right_files.size());
		for (idx_t i = 0; i < left_files.size(); i++) {
			REQUIRE(left_files[i].path == right_files[i].path);
		}
		if (left.extra_info.total_files.IsValid() && left.extra_info.total_files.GetIndex() > left_files.size()) {
			REQUIRE(left.extra_info.file_filter_expressions);
			REQUIRE_FALSE(left.extra_info.file_filter_expressions->empty());
			probe.pruned_scans++;
		}
		if (left.table_filters.HasFilters()) {
			probe.scans_with_normal_filters++;
		}
		probe.copied_scans++;
	}
	for (idx_t i = 0; i < original.children.size(); i++) {
		CheckScanCopies(*original.children[i], *copy.children[i], probe);
	}
}

static void CopyAfterOptimization(OptimizerExtensionInput &input, unique_ptr<LogicalOperator> &plan) {
	auto &probe = static_cast<BoundCopyProbe &>(*input.info);
	if (!probe.enabled) {
		return;
	}
	for (idx_t i = 0; i < probe.copies; i++) {
		auto copy = plan->CopyPreservingBoundState(input.context);
		CheckScanCopies(*plan, *copy, probe);
		plan = std::move(copy);
	}
}

static void RequireSameResult(QueryResult &original, QueryResult &copy) {
	REQUIRE_NO_FAIL(copy);
	REQUIRE(original.GetTypes() == copy.GetTypes());
	REQUIRE(original.RowCount() == copy.RowCount());
	auto lhs = original.Collection().GetRows();
	auto rhs = copy.Collection().GetRows();
	for (idx_t row = 0; row < lhs.size(); row++) {
		for (idx_t column = 0; column < original.ColumnCount(); column++) {
			REQUIRE(Value::NotDistinctFrom(lhs.GetValue(column, row), rhs.GetValue(column, row)));
		}
	}
}

struct CopyTestData : TableFunctionData {
	explicit CopyTestData(int64_t value_p, bool copyable_p = true) : value(value_p), copyable(copyable_p) {
	}
	int64_t value;
	bool copyable;
	unique_ptr<FunctionData> Copy() const override {
		if (!copyable) {
			throw NotImplementedException("test bind data cannot be copied");
		}
		return make_uniq<CopyTestData>(value, copyable);
	}
};

} // namespace

TEST_CASE("Bound plan copies execute the original pruned Hive file set", "[optimizer][bound_plan_copy]") {
	DuckDB db(nullptr);
	Connection connection(db);
	auto directory = TestCreatePath("bound_plan_copy_hive");
	REQUIRE_NO_FAIL(
	    connection.Query("COPY (SELECT i,CASE WHEN i%3=2 THEN NULL ELSE i%3 END k FROM range(30) r(i)) TO " +
	                     Value(directory).ToSQLString() + " (FORMAT PARQUET, PARTITION_BY(k))"));
	const auto source = "read_parquet(" + Value(directory + "/*/*.parquet").ToSQLString() + ", hive_partitioning=true)";
	auto probe = make_shared_ptr<BoundCopyProbe>();
	OptimizerExtension extension;
	extension.optimizer_info = probe;
	extension.optimize_function = CopyAfterOptimization;
	OptimizerExtension::Register(DBConfig::GetConfig(*db.instance), std::move(extension));
	vector<string> queries {"SELECT count(*),max(k) FROM " + source + " WHERE k IS NULL",
	                        "SELECT sum(i) FROM " + source + " WHERE k IS NULL",
	                        "SELECT count(*),min(i),max(i) FROM " + source + " WHERE k=1",
	                        "SELECT k,count(*) FROM " + source +
	                            " WHERE k IS NULL OR k=1 GROUP BY k ORDER BY k NULLS FIRST",
	                        "SELECT count(*),max(k) FROM " + source + " WHERE k=10",
	                        "SELECT sum(i) FROM (SELECT i FROM " + source +
	                            " WHERE k IS NULL UNION ALL SELECT i FROM " + source + " WHERE k=1) t",
	                        "SELECT i FROM " + source + " WHERE k IS NULL AND i>=10 ORDER BY i"};
	for (auto &query : queries) {
		CAPTURE(query);
		probe->enabled = false;
		auto original = connection.Query(query);
		REQUIRE_NO_FAIL(*original);
		for (idx_t copies : {idx_t(1), idx_t(3)}) {
			probe->enabled = true;
			probe->copies = copies;
			auto copy = connection.Query(query);
			RequireSameResult(*original, *copy);
		}
	}
	REQUIRE(probe->copied_scans > 0);
	REQUIRE(probe->pruned_scans > 0);
	REQUIRE(probe->scans_with_normal_filters > 0);
}

TEST_CASE("Bound plan copy bypasses table-function rebinding and rejects noncopyable state",
          "[optimizer][bound_plan_copy]") {
	DuckDB db(nullptr);
	Connection connection(db);
	connection.BeginTransaction();
	auto &context = *connection.context;
	TableFunction function("bound_copy_test", {}, nullptr);
	function.bind = [](ClientContext &, TableFunctionBindInput &, vector<LogicalType> &types,
	                   vector<Identifier> &names) -> unique_ptr<FunctionData> {
		types.push_back(LogicalType::BIGINT);
		names.emplace_back("v");
		return make_uniq<CopyTestData>(7);
	};
	CreateTableFunctionInfo info(function);
	Catalog::GetSystemCatalog(context).CreateFunction(context, info);
	auto get = make_uniq<LogicalGet>(TableIndex(3), BoundTableFunction(function), make_uniq<CopyTestData>(42),
	                                 vector<LogicalType> {LogicalType::BIGINT}, vector<Identifier> {Identifier("v")});
	get->AddColumnId(0);
	get->SetEstimatedCardinality(0);
	LogicalProjection root(TableIndex(4), vector<unique_ptr<Expression>> {});
	root.children.push_back(std::move(get));
	root.has_estimated_cardinality = false;
	root.estimated_cardinality = NumericLimits<idx_t>::Maximum();
	root.ResolveOperatorTypes();

	SECTION("Bound data and cached zero/absent cardinalities survive repeated nested copies") {
		auto copy = root.CopyPreservingBoundState(context);
		auto repeated = copy->CopyPreservingBoundState(context);
		for (auto &candidate : {copy.get(), repeated.get()}) {
			auto &scan = candidate->children[0]->Cast<LogicalGet>();
			REQUIRE(scan.bind_data->Cast<CopyTestData>().value == 42);
			REQUIRE(scan.has_estimated_cardinality);
			REQUIRE(scan.estimated_cardinality == 0);
			REQUIRE_FALSE(candidate->has_estimated_cardinality);
			REQUIRE(candidate->estimated_cardinality == NumericLimits<idx_t>::Maximum());
		}
		copy->children[0]->Cast<LogicalGet>().bind_data->Cast<CopyTestData>().value = 99;
		REQUIRE(root.children[0]->Cast<LogicalGet>().bind_data->Cast<CopyTestData>().value == 42);
		REQUIRE(repeated->children[0]->Cast<LogicalGet>().bind_data->Cast<CopyTestData>().value == 42);
	}
	SECTION("Ordinary copy still rebinds a function without serialization callbacks") {
		auto copy = root.Copy(context);
		REQUIRE(copy->children[0]->Cast<LogicalGet>().bind_data->Cast<CopyTestData>().value == 7);
	}
	SECTION("A failed FunctionData copy is not replaced by a successful rebind") {
		root.children[0]->Cast<LogicalGet>().bind_data->Cast<CopyTestData>().copyable = false;
		REQUIRE_THROWS_WITH(root.CopyPreservingBoundState(context),
		                    Catch::Matchers::Contains("test bind data cannot be copied"));
		REQUIRE(root.children[0]->Cast<LogicalGet>().bind_data->Cast<CopyTestData>().value == 42);
	}
	SECTION("Table-function serialization callbacks are not used by the preserving copy") {
		auto callbacks = function;
		callbacks.serialize = [](Serializer &, const optional_ptr<FunctionData>, const BoundTableFunction &) {
			throw NotImplementedException("persistent callback invoked");
		};
		callbacks.deserialize = [](Deserializer &, BoundTableFunction &) -> unique_ptr<FunctionData> {
			throw NotImplementedException("persistent deserialize invoked");
		};
		root.children[0]->Cast<LogicalGet>().function = BoundTableFunction(callbacks);
		auto copy = root.CopyPreservingBoundState(context);
		REQUIRE(copy->children[0]->Cast<LogicalGet>().bind_data->Cast<CopyTestData>().value == 42);
		REQUIRE_THROWS_WITH(root.Copy(context), Catch::Matchers::Contains("persistent callback invoked"));
	}
	SECTION("Process-local bind input is rejected before copying") {
		root.children[0]->Cast<LogicalGet>().bind_info = make_shared_ptr<TableFunctionInfo>();
		REQUIRE_THROWS_WITH(root.CopyPreservingBoundState(context),
		                    Catch::Matchers::Contains("process-local bind input"));
	}
	SECTION("Runtime dynamic filter sets are outside the preserving contract") {
		root.children[0]->Cast<LogicalGet>().dynamic_filters = make_shared_ptr<DynamicTableFilterSet>();
		REQUIRE_THROWS_WITH(root.CopyPreservingBoundState(context), Catch::Matchers::Contains("dynamic filters"));
	}
	SECTION("Shared dynamic state inside operator expressions is rejected") {
		root.expressions.push_back(CreateDynamicFilterExpression(
		    make_shared_ptr<DynamicFilterData>(ExpressionType::COMPARE_LESSTHAN, Value::BIGINT(7)),
		    LogicalType::BIGINT));
		REQUIRE_THROWS_WITH(root.CopyPreservingBoundState(context),
		                    Catch::Matchers::Contains("shared dynamic filter state"));
	}
	SECTION("Shared dynamic state in aggregate FILTER and ORDER BY children is rejected") {
		for (bool in_filter : {true, false}) {
			auto dynamic = CreateDynamicFilterExpression(
			    make_shared_ptr<DynamicFilterData>(ExpressionType::COMPARE_LESSTHAN, Value::BIGINT(7)),
			    LogicalType::BIGINT);
			AggregateFunction function("bound_copy_aggregate", {}, LogicalType::BIGINT, nullptr, nullptr, nullptr,
			                           nullptr, nullptr);
			auto aggregate =
			    make_uniq<BoundAggregateExpression>(BoundAggregateFunction(function), vector<unique_ptr<Expression>> {},
			                                        nullptr, nullptr, AggregateType::NON_DISTINCT);
			if (in_filter) {
				aggregate->GetFilterMutable() = std::move(dynamic);
			} else {
				aggregate->GetOrderBysMutable() = make_uniq<BoundOrderModifier>();
				aggregate->GetOrderBysMutable()->orders.emplace_back(OrderType::ASCENDING, OrderByNullType::NULLS_FIRST,
				                                                     std::move(dynamic));
			}
			vector<unique_ptr<Expression>> expressions;
			expressions.push_back(std::move(aggregate));
			LogicalAggregate aggregate_root(TableIndex(10), TableIndex(11), std::move(expressions));
			aggregate_root.children.push_back(make_uniq<LogicalDummyScan>(TableIndex(9)));
			REQUIRE_THROWS_WITH(aggregate_root.CopyPreservingBoundState(context),
			                    Catch::Matchers::Contains("shared dynamic filter state"));
		}
	}
	SECTION("Non-expression table filters are explicitly outside the contract") {
		root.children[0]->Cast<LogicalGet>().table_filters.SetFilterByColumnIndex(ProjectionIndex(0),
		                                                                          make_uniq<LegacyIsNullFilter>());
		REQUIRE_THROWS_WITH(root.CopyPreservingBoundState(context),
		                    Catch::Matchers::Contains("non-expression table filters"));
	}
	SECTION("Shared top-N filter state is rejected") {
		LogicalTopN top_n(vector<BoundOrderByNode> {}, 1, 0);
		top_n.dynamic_filter = make_shared_ptr<DynamicFilterData>(ExpressionType::COMPARE_LESSTHAN, Value::BIGINT(7));
		top_n.children.push_back(make_uniq<LogicalDummyScan>(TableIndex(0)));
		REQUIRE_THROWS_WITH(top_n.CopyPreservingBoundState(context), Catch::Matchers::Contains("shared top-N filters"));
	}
	SECTION("Shared join filter state is rejected") {
		LogicalComparisonJoin join(JoinType::INNER);
		join.filter_pushdown = make_uniq<JoinFilterPushdownInfo>();
		join.children.push_back(make_uniq<LogicalDummyScan>(TableIndex(0)));
		join.children.push_back(make_uniq<LogicalDummyScan>(TableIndex(1)));
		REQUIRE_THROWS_WITH(join.CopyPreservingBoundState(context), Catch::Matchers::Contains("shared join filters"));
	}
	SECTION("Operators outside the bounded inventory are rejected") {
		LogicalSample sample(make_uniq<SampleOptions>(), make_uniq<LogicalDummyScan>(TableIndex(0)));
		REQUIRE_THROWS_WITH(sample.CopyPreservingBoundState(context),
		                    Catch::Matchers::Contains("does not support operator"));
	}
	connection.Rollback();
}

// Scan preservation deliberately retains the ordinary expression reconstruction contract.
// A FunctionData::Copy-only payload is not carried through logical expression serialization.
TEST_CASE("Scan-preserving copies retain ordinary expression reconstruction", "[optimizer][bound_plan_copy]") {
	struct BoundCopyExpressionData : FunctionData {
		explicit BoundCopyExpressionData(int64_t value_p) : value(value_p) {
		}
		int64_t value;
		unique_ptr<FunctionData> Copy() const override {
			return make_uniq<BoundCopyExpressionData>(value);
		}
		bool Equals(const FunctionData &other) const override {
			return value == other.Cast<BoundCopyExpressionData>().value;
		}
	};
	DuckDB db(nullptr);
	Connection connection(db);
	connection.BeginTransaction();
	auto &context = *connection.context;
	ScalarFunction function("bound_copy_expression_test", vector<LogicalType> {}, LogicalType::BIGINT,
	                        [](DataChunk &, ExpressionState &state, Vector &result) {
		                        const auto &call = state.expr.Cast<BoundFunctionExpression>();
		                        result.SetVectorType(VectorType::CONSTANT_VECTOR);
		                        ConstantVector::SetNull(result, false);
		                        ConstantVector::GetData<int64_t>(result)[0] =
		                            call.BindInfo()->Cast<BoundCopyExpressionData>().value;
	                        });
	function.SetBindCallback(
	    [](BindScalarFunctionInput &) -> unique_ptr<FunctionData> { return make_uniq<BoundCopyExpressionData>(7); });
	CreateScalarFunctionInfo info(function);
	Catalog::GetSystemCatalog(context).CreateFunction(context, info);
	vector<unique_ptr<Expression>> expressions;
	expressions.push_back(make_uniq<BoundFunctionExpression>(
	    BoundScalarFunction(function), vector<unique_ptr<Expression>> {}, make_uniq<BoundCopyExpressionData>(42)));
	LogicalProjection root(TableIndex(1), std::move(expressions));
	root.children.push_back(make_uniq<LogicalDummyScan>(TableIndex(0)));
	root.ResolveOperatorTypes();
	auto expression_copy = root.expressions[0]->Copy();
	auto preserving = root.CopyPreservingBoundState(context);
	auto ordinary = root.Copy(context);
	REQUIRE(ExpressionExecutor::EvaluateScalar(context, *root.expressions[0]) == Value::BIGINT(42));
	REQUIRE(ExpressionExecutor::EvaluateScalar(context, *expression_copy) == Value::BIGINT(42));
	REQUIRE(ExpressionExecutor::EvaluateScalar(context, *preserving->expressions[0]) == Value::BIGINT(7));
	REQUIRE(ExpressionExecutor::EvaluateScalar(context, *ordinary->expressions[0]) == Value::BIGINT(7));
	connection.Rollback();
}
