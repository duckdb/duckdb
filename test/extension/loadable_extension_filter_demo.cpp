#include "duckdb.hpp"
#include "duckdb/optimizer/optimizer_extension.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/table_filter.hpp"
#include "duckdb/planner/filter/expression_filter.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/common/constants.hpp"
#include "duckdb/storage/statistics/numeric_stats.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/column_index.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/function/scalar_function.hpp"

using namespace duckdb;

struct RowIdFilterState : public FunctionLocalState {
	duckdb::vector<int64_t> allowed_ids;
	idx_t next_allowed_id = 0;

	SelectionVector filter_sel;
	idx_t current_capacity = 0;
};

struct RowIdFilterBindData : public FunctionData {
	duckdb::vector<int64_t> allowed_ids;

	RowIdFilterBindData(duckdb::vector<int64_t> allowed_ids) : allowed_ids(std::move(allowed_ids)) {
	}

	unique_ptr<FunctionData> Copy() const override {
		return make_uniq<RowIdFilterBindData>(allowed_ids);
	}

	bool Equals(const FunctionData &other_p) const override {
		auto &other = other_p.Cast<RowIdFilterBindData>();
		return allowed_ids == other.allowed_ids;
	}
};

static unique_ptr<FunctionData> RowIdFilterBind(ClientContext &context, ScalarFunction &bound_function,
                                                vector<unique_ptr<Expression>> &arguments) {
	throw InternalException("rowid_filter bind reached unexpectedly.");
}

static void RowIdFilterFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	throw InternalException("rowid_filter execution reached unexpectedly.");
}

static FilterPropagateResult RowIdFilterGroupPrune(const FunctionData *bind_data_p, const BaseStatistics &stats) {
	auto &bind_data = bind_data_p->Cast<RowIdFilterBindData>();
	auto &allowed_ids = bind_data.allowed_ids;

	if (!NumericStats::HasMinMax(stats)) {
		return FilterPropagateResult::NO_PRUNING_POSSIBLE;
	}
	auto min = NumericStats::GetMin<int64_t>(stats);
	auto max = NumericStats::GetMax<int64_t>(stats);

	auto it = std::lower_bound(allowed_ids.begin(), allowed_ids.end(), min);
	if (it != allowed_ids.end() && *it <= max) {
		return FilterPropagateResult::NO_PRUNING_POSSIBLE;
	}
	return FilterPropagateResult::FILTER_ALWAYS_FALSE;
}

static idx_t RowIdFilterRowPrune(const FunctionData *bind_data_p, FunctionLocalState &state_p, Vector &vector,
                                 SelectionVector &sel, idx_t count) {
	auto &state = state_p.Cast<RowIdFilterState>();
	// The bind data has the master list, state tracks progress
	auto &bind_data = bind_data_p->Cast<RowIdFilterBindData>();
	auto &allowed = bind_data.allowed_ids;

	if (state.current_capacity < count) {
		state.filter_sel.Initialize(count);
		state.current_capacity = count;
	}

	UnifiedVectorFormat vdata;
	vector.ToUnifiedFormat(count, vdata);
	auto data = UnifiedVectorFormat::GetData<int64_t>(vdata);

	idx_t found_count = 0;
	for (idx_t i = 0; i < count; i++) {
		auto idx = sel.get_index(i);
		auto row_id = data[vdata.sel->get_index(idx)];

		// Advance next_allowed_id until it's >= current row_id
		while (state.next_allowed_id < allowed.size() && allowed[state.next_allowed_id] < row_id) {
			state.next_allowed_id++;
		}

		if (state.next_allowed_id < allowed.size() && allowed[state.next_allowed_id] == row_id) {
			state.filter_sel.set_index(found_count++, i);
		}
	}

	if (found_count == count) {
		return count;
	}

	if (sel.IsSet()) {
		for (idx_t i = 0; i < found_count; i++) {
			const idx_t flat_idx = state.filter_sel.get_index(i);
			const idx_t original_idx = sel.get_index(flat_idx);
			sel.set_index(i, original_idx);
		}
	} else {
		sel.Initialize(state.filter_sel);
	}

	return found_count;
}

static unique_ptr<FunctionLocalState> RowIdFilterInit(ExpressionState &state, const BoundFunctionExpression &expr,
                                                      FunctionData *bind_data_p) {
	auto res = make_uniq<RowIdFilterState>();
	auto &bind_data = bind_data_p->Cast<RowIdFilterBindData>();
	res->allowed_ids = bind_data.allowed_ids;
	return std::move(res);
}

class RowIdOptimizerExtension : public OptimizerExtension {
public:
	RowIdOptimizerExtension() {
		optimize_function = RowIdOptimizeFunction;
	}

	static void RowIdOptimizeFunction(OptimizerExtensionInput &input, duckdb::unique_ptr<LogicalOperator> &plan) {
		if (plan->type == LogicalOperatorType::LOGICAL_GET) {
			auto &get = plan->Cast<LogicalGet>();
			auto table = get.GetTable();
			if (table && table->name == "rowid_test_table") {
				// Inject filter: keep 3, 4, 5, 7, 9
				duckdb::vector<int64_t> allowed = {3, 4, 5, 7, 9};

				// Create ScalarFunction
				ScalarFunction rowid_func("rowid_filter", {LogicalType::BIGINT}, LogicalType::BOOLEAN,
				                          RowIdFilterFunction, RowIdFilterBind);
				rowid_func.SetInitStateCallback(RowIdFilterInit);
				rowid_func.SetFilterGroupPruneCallback(RowIdFilterGroupPrune);
				rowid_func.SetFilterRowPruneCallback(RowIdFilterRowPrune);

				// Create BoundFunctionExpression
				auto bind_data = make_uniq<RowIdFilterBindData>(std::move(allowed));
				vector<unique_ptr<Expression>> children;
				children.push_back(make_uniq<BoundReferenceExpression>(LogicalType::BIGINT, COLUMN_IDENTIFIER_ROW_ID));

				auto bound_expr = make_uniq<BoundFunctionExpression>(LogicalType::BOOLEAN, rowid_func,
				                                                     std::move(children), std::move(bind_data));

				// Wrap in ExpressionFilter
				auto filter = make_uniq<ExpressionFilter>(std::move(bound_expr));

				get.table_filters.PushFilter(ColumnIndex(COLUMN_IDENTIFIER_ROW_ID), std::move(filter));

				// Add ROW ID to column_ids if not present
				bool found = false;
				for (auto &col : get.GetColumnIds()) {
					if (col.IsRowIdColumn()) {
						found = true;
						break;
					}
				}
				if (!found) {
					if (get.projection_ids.empty()) {
						for (idx_t i = 0; i < get.GetColumnIds().size(); i++) {
							get.projection_ids.push_back(i);
						}
					}
					get.AddColumnId(COLUMN_IDENTIFIER_ROW_ID);
				}
			}
		}

		for (auto &child : plan->children) {
			RowIdOptimizeFunction(input, child);
		}
	}
};

extern "C" {
DUCKDB_CPP_EXTENSION_ENTRY(loadable_extension_filter_demo, loader) {
	auto &db = loader.GetDatabaseInstance();
	auto &config = DBConfig::GetConfig(db);
	config.optimizer_extensions.push_back(RowIdOptimizerExtension());
}
}
