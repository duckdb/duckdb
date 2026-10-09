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
#include "duckdb/common/serializer/binary_deserializer.hpp"
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
#include "duckdb/common/reference_map.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/execution/operator/scan/physical_dummy_scan.hpp"
#include "duckdb/planner/filter/expression_filter.hpp"

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

struct JoinCopyProbe : OptimizerExtensionInfo {
	bool enabled = false;
	bool refusals = false;
	idx_t shared_sets = 0;
	idx_t min_max_only = 0;
	idx_t multiple_targets = 0;
	idx_t casts = 0;
	idx_t refused = 0;
};

struct JoinCopySet {
	optional_ptr<DynamicTableFilterSet> copy;
	idx_t producers = 0;
	idx_t scans = 0;
};
using join_copy_sets_t = reference_map_t<DynamicTableFilterSet, JoinCopySet>;

static void CheckJoinCopies(LogicalOperator &original, LogicalOperator &copy, join_copy_sets_t &sets,
                            JoinCopyProbe &probe, ClientContext &context) {
	REQUIRE(original.type == copy.type);
	REQUIRE(original.children.size() == copy.children.size());
	auto check_set = [&](DynamicTableFilterSet &source, DynamicTableFilterSet &target, bool scan) {
		REQUIRE(&source != &target);
		REQUIRE_FALSE(source.HasFilters());
		REQUIRE_FALSE(target.HasFilters());
		auto &entry = sets[source];
		if (entry.copy) {
			REQUIRE(entry.copy.get() == &target);
		} else {
			entry.copy = target;
		}
		scan ? entry.scans++ : entry.producers++;
	};
	if (original.type == LogicalOperatorType::LOGICAL_GET) {
		auto &lhs = original.Cast<LogicalGet>();
		auto &rhs = copy.Cast<LogicalGet>();
		if (lhs.dynamic_filters) {
			REQUIRE(rhs.dynamic_filters);
			check_set(*lhs.dynamic_filters, *rhs.dynamic_filters, true);
		}
	}
	if (original.type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN) {
		auto &lhs = original.Cast<LogicalComparisonJoin>();
		auto &rhs = copy.Cast<LogicalComparisonJoin>();
		if (lhs.filter_pushdown) {
			REQUIRE(rhs.filter_pushdown);
			REQUIRE(lhs.filter_pushdown.get() != rhs.filter_pushdown.get());
			auto &a = *lhs.filter_pushdown;
			auto &b = *rhs.filter_pushdown;
			REQUIRE(a.join_condition == b.join_condition);
			REQUIRE(a.probe_info.size() == b.probe_info.size());
			REQUIRE(a.min_max_aggregates.size() == b.min_max_aggregates.size());
			if (a.probe_info.empty()) {
				REQUIRE_FALSE(b.build_side_has_filter);
				probe.min_max_only++;
			} else {
				REQUIRE(a.build_side_has_filter == b.build_side_has_filter);
			}
			probe.multiple_targets += a.probe_info.size() > 1;
			for (idx_t i = 0; i < a.min_max_aggregates.size(); i++) {
				REQUIRE(a.min_max_aggregates[i].get() != b.min_max_aggregates[i].get());
				// Ordinary reconstruction can replace a statistics-specialized cast implementation.
				MemoryStream stream(Allocator::Get(context));
				SerializationOptions options;
				options.storage_compatibility = StorageCompatibility::Latest();
				BinarySerializer::Serialize(*a.min_max_aggregates[i], stream, options);
				stream.Rewind();
				bound_parameter_map_t parameters;
				auto ordinary = BinaryDeserializer::Deserialize<Expression>(stream, context, parameters);
				REQUIRE(ordinary->Equals(*b.min_max_aggregates[i]));
				auto &aggregate = b.min_max_aggregates[i]->Cast<BoundAggregateExpression>();
				REQUIRE(aggregate.GetChildren().size() == 1);
				REQUIRE(aggregate.GetChildren()[0]->Equals(rhs.conditions[b.join_condition[i / 2]].GetRHS()));
			}
			for (idx_t i = 0; i < a.probe_info.size(); i++) {
				auto &left = a.probe_info[i];
				auto &right = b.probe_info[i];
				check_set(*left.dynamic_filters, *right.dynamic_filters, false);
				REQUIRE(left.columns.size() == right.columns.size());
				for (idx_t j = 0; j < left.columns.size(); j++) {
					auto &lc = left.columns[j];
					auto &rc = right.columns[j];
					REQUIRE(lc.join_filter_idx == rc.join_filter_idx);
					REQUIRE(lc.probe_column_index == rc.probe_column_index);
					REQUIRE(lc.storage_type == rc.storage_type);
					REQUIRE(lc.mode == rc.mode);
					REQUIRE(lc.runtime_filter_casts.size() == rc.runtime_filter_casts.size());
					probe.casts += lc.runtime_filter_casts.size();
					for (idx_t k = 0; k < lc.runtime_filter_casts.size(); k++) {
						REQUIRE(lc.runtime_filter_casts[k].target_type == rc.runtime_filter_casts[k].target_type);
						REQUIRE(lc.runtime_filter_casts[k].mode == rc.runtime_filter_casts[k].mode);
					}
				}
			}
		}
	}
	for (idx_t i = 0; i < original.children.size(); i++) {
		CheckJoinCopies(*original.children[i], *copy.children[i], sets, probe, context);
	}
}

static optional_ptr<LogicalComparisonJoin> FindJoinProducer(LogicalOperator &op) {
	if (op.type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN) {
		auto &join = op.Cast<LogicalComparisonJoin>();
		if (join.filter_pushdown && !join.filter_pushdown->probe_info.empty()) {
			return join;
		}
	}
	for (auto &child : op.children) {
		auto found = FindJoinProducer(*child);
		if (found) {
			return found;
		}
	}
	return nullptr;
}

static void CopyJoinAfterOptimization(OptimizerExtensionInput &input, unique_ptr<LogicalOperator> &plan) {
	auto &probe = static_cast<JoinCopyProbe &>(*input.info);
	if (!probe.enabled) {
		return;
	}
	auto copy = plan->CopyPreservingBoundState(input.context);
	auto sibling = plan->CopyPreservingBoundState(input.context);
	auto repeated = copy->CopyPreservingBoundState(input.context);
	join_copy_sets_t sets, siblings, repeats;
	CheckJoinCopies(*plan, *copy, sets, probe, input.context);
	CheckJoinCopies(*plan, *sibling, siblings, probe, input.context);
	CheckJoinCopies(*copy, *repeated, repeats, probe, input.context);
	PhysicalPlan physical(Allocator::Get(input.context));
	auto &identity = physical.Make<PhysicalDummyScan>(vector<LogicalType> {LogicalType::BIGINT}, 1);
	for (auto &entry : sets) {
		REQUIRE(entry.second.scans == 1);
		REQUIRE(entry.second.producers > 0);
		probe.shared_sets += entry.second.producers > 1;
		auto &target = *entry.second.copy;
		target.PushFilter(identity, ProjectionIndex(0),
		                  ExpressionFilter::CreateComparisonFilter(ExpressionType::COMPARE_EQUAL, Value::BIGINT(7)));
		REQUIRE(target.HasFilters());
		REQUIRE_FALSE(entry.first.get().HasFilters());
		REQUIRE_FALSE(siblings.at(entry.first).copy->HasFilters());
		REQUIRE_FALSE(repeats.at(target).copy->HasFilters());
		target.ClearFilters(identity);
		REQUIRE_FALSE(target.HasFilters());
	}
	if (probe.refusals) {
		auto join = FindJoinProducer(*plan);
		REQUIRE(join);
		auto &target = join->filter_pushdown->probe_info[0].dynamic_filters;
		auto original_target = target;
		target = make_shared_ptr<DynamicTableFilterSet>();
		REQUIRE_THROWS_WITH(plan->CopyPreservingBoundState(input.context),
		                    Catch::Matchers::Contains("connected join dynamic filters"));
		target = original_target;
		auto &column = join->filter_pushdown->probe_info[0].columns[0].probe_column_index.column_index;
		auto original_column = column;
		column = ProjectionIndex();
		REQUIRE_THROWS_WITH(plan->CopyPreservingBoundState(input.context),
		                    Catch::Matchers::Contains("bound join filter target columns"));
		column = original_column;
		join->join_type = JoinType::LEFT;
		REQUIRE_THROWS_WITH(plan->CopyPreservingBoundState(input.context),
		                    Catch::Matchers::Contains("native INNER descriptors"));
		join->join_type = JoinType::INNER;
		target->PushFilter(identity, ProjectionIndex(0),
		                   ExpressionFilter::CreateComparisonFilter(ExpressionType::COMPARE_EQUAL, Value::BIGINT(7)));
		REQUIRE_THROWS_WITH(plan->CopyPreservingBoundState(input.context),
		                    Catch::Matchers::Contains("unexecuted, connected"));
		target->ClearFilters(identity);
		probe.refused += 4;
		// Refusal must leave the original graph reusable.
		copy = plan->CopyPreservingBoundState(input.context);
	}
	plan = std::move(copy);
}

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
	SECTION("Unconnected runtime dynamic filter sets are outside the preserving contract") {
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
			                           nullptr, nullptr, FunctionNullHandling::DEFAULT_NULL_HANDLING);
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
	SECTION("Incomplete join filter state is rejected") {
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

// SQL cannot invoke this API or observe whether two producers alias the same copy-local set.
TEST_CASE("Bound join copies preserve filter graph aliasing and isolate runtime state",
          "[optimizer][bound_plan_copy]") {
	DuckDB db(nullptr);
	Connection connection(db);
	auto directory = TestCreatePath("bound_join_copy");
	REQUIRE_NO_FAIL(connection.Query("COPY (SELECT i::INTEGER k,i v FROM range(12) r(i)) TO " +
	                                 Value(directory + ".parquet").ToSQLString() + " (FORMAT PARQUET)"));
	const auto source = "read_parquet(" + Value(directory + ".parquet").ToSQLString() + ")";
	auto probe = make_shared_ptr<JoinCopyProbe>();
	OptimizerExtension extension;
	extension.optimizer_info = probe;
	extension.optimize_function = CopyJoinAfterOptimization;
	OptimizerExtension::Register(DBConfig::GetConfig(*db.instance), std::move(extension));
	// Fix the topology for the shared-set assertion. Join-filter pushdown remains enabled.
	REQUIRE_NO_FAIL(connection.Query("SET disabled_optimizers='join_order,build_side_probe_side'"));
	vector<string> queries {
	    "SELECT l.k,l.v,r.v FROM " + source + " l JOIN " + source + " r ON l.k=r.k ORDER BY ALL",
	    "SELECT l.k,l.v,r.v,s.v FROM " + source + " l JOIN " + source + " r ON l.k=r.k JOIN " + source +
	        " s ON l.k=s.k ORDER BY ALL",
	    "SELECT l.k,r.v FROM (SELECT * FROM " + source + " UNION ALL SELECT * FROM " + source + ") l JOIN " + source +
	        " r ON l.k=r.k ORDER BY ALL",
	    "SELECT l.k,r.v FROM (SELECT * FROM " + source + " LIMIT 4) l JOIN " + source + " r ON l.k=r.k ORDER BY ALL",
	    "SELECT l.k,r.v FROM " + source + " l JOIN " + source + " r ON l.k::BIGINT=r.k::BIGINT ORDER BY ALL"};
	for (auto &query : queries) {
		CAPTURE(query);
		probe->enabled = false;
		auto original = connection.Query(query);
		REQUIRE_NO_FAIL(*original);
		probe->enabled = true;
		auto copy = connection.Query(query);
		RequireSameResult(*original, *copy);
	}
	REQUIRE(probe->shared_sets > 0);
	REQUIRE(probe->multiple_targets > 0);
	REQUIRE(probe->min_max_only > 0);
	REQUIRE(probe->casts > 0);
}

// Populated physical-identity maps and broken logical endpoints are not constructible through SQL.
TEST_CASE("Bound join copies refuse external populated and non-INNER filter graphs", "[optimizer][bound_plan_copy]") {
	DuckDB db(nullptr);
	Connection connection(db);
	auto path = TestCreatePath("bound_join_copy_refusal.parquet");
	REQUIRE_NO_FAIL(
	    connection.Query("COPY (SELECT i k FROM range(8) r(i)) TO " + Value(path).ToSQLString() + " (FORMAT PARQUET)"));
	auto probe = make_shared_ptr<JoinCopyProbe>();
	probe->refusals = true;
	OptimizerExtension extension;
	extension.optimizer_info = probe;
	extension.optimize_function = CopyJoinAfterOptimization;
	OptimizerExtension::Register(DBConfig::GetConfig(*db.instance), std::move(extension));
	auto source = "read_parquet(" + Value(path).ToSQLString() + ")";
	auto query = "SELECT l.k FROM " + source + " l JOIN " + source + " r ON l.k=r.k ORDER BY ALL";
	auto original = connection.Query(query);
	REQUIRE_NO_FAIL(*original);
	probe->enabled = true;
	auto copy = connection.Query(query);
	RequireSameResult(*original, *copy);
	REQUIRE(probe->refused == 4);
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
