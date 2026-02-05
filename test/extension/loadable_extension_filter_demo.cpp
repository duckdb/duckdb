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
	vector<int64_t> allowed_ids;
	idx_t next_allowed_id = 0;

	SelectionVector filter_sel;
	idx_t current_capacity = 0;
};

struct RowIdFilterBindData : public FunctionData {
	vector<int64_t> allowed_ids;

	explicit RowIdFilterBindData(vector<int64_t> allowed_ids) : allowed_ids(std::move(allowed_ids)) {
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
	// we do not use this function for extensible filter
	throw InternalException("rowid_filter bind reached unexpectedly.");
}

static void RowIdFilterFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	// we do not use this function for extensible filter
	throw InternalException("rowid_filter execution reached unexpectedly.");
}

static unique_ptr<FunctionLocalState> RowIdFilterInit(ExpressionState &state, const BoundFunctionExpression &expr,
                                                      FunctionData *bind_data_p) {
	auto res = make_uniq<RowIdFilterState>();
	auto &bind_data = bind_data_p->Cast<RowIdFilterBindData>();
	res->allowed_ids = bind_data.allowed_ids;
	return std::move(res);
}

static FilterPropagateResult RowIdFilterRowGroupPrune(const FunctionData *bind_data_p, const BaseStatistics &stats) {
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

static idx_t RowIdFilterVectorPrune(const FunctionData *bind_data_p, FunctionLocalState &state_p, Vector &vector,
                                    SelectionVector &sel, idx_t &approved_tuple_count) {
	auto &state = state_p.Cast<RowIdFilterState>();
	// The bind data has the master list, state tracks progress
	auto &bind_data = bind_data_p->Cast<RowIdFilterBindData>();
	auto &allowed = bind_data.allowed_ids;

	if (state.current_capacity < approved_tuple_count) {
		state.filter_sel.Initialize(approved_tuple_count);
		state.current_capacity = approved_tuple_count;
	}

	UnifiedVectorFormat vdata;
	vector.ToUnifiedFormat(approved_tuple_count, vdata);
	auto data = UnifiedVectorFormat::GetData<int64_t>(vdata);

	idx_t found_count = 0;
	for (idx_t i = 0; i < approved_tuple_count; i++) {
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

	if (found_count == approved_tuple_count) {
		return approved_tuple_count;
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

	approved_tuple_count = found_count;
	return approved_tuple_count;
}

static bool RowIdFilterValuePrune(const FunctionData *bind_data_p, const Value &value) {
	D_ASSERT(value.type().id() == LogicalTypeId::BIGINT);

	auto &bind_data = bind_data_p->Cast<RowIdFilterBindData>();
	auto &allowed = bind_data.allowed_ids;

	const auto row_id = value.GetValue<int64_t>();

	auto it = std::lower_bound(allowed.begin(), allowed.end(), row_id);
	return it != allowed.end() && *it == row_id;
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
				vector<int64_t> allowed = {3, 4, 5, 7, 9};

				// Create ScalarFunction
				ScalarFunction rowid_func("rowid_filter", {LogicalType::BIGINT}, LogicalType::BOOLEAN,
				                          RowIdFilterFunction, RowIdFilterBind);
				rowid_func.SetInitStateCallback(RowIdFilterInit);
				rowid_func.SetRowGroupPrunerCallback(RowIdFilterRowGroupPrune);
				rowid_func.SetVectorPrunerCallback(RowIdFilterVectorPrune);
				rowid_func.SetValuePrunerCallback(RowIdFilterValuePrune);

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
	config.GetCallbackManager().Register(RowIdOptimizerExtension());
}
}
