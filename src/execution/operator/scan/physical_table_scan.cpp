#include "duckdb/execution/operator/scan/physical_table_scan.hpp"

#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"
#include "duckdb/transaction/transaction.hpp"
#include "duckdb/parallel/parallel_state.hpp"

#include <utility>

namespace duckdb {

PhysicalTableScan::PhysicalTableScan(vector<LogicalType> types, TableFunction function_p,
                                     unique_ptr<FunctionData> bind_data_p, vector<column_t> column_ids_p,
                                     vector<string> names_p, unique_ptr<TableFilterSet> table_filters_p,
                                     idx_t estimated_cardinality)
    : PhysicalOperator(PhysicalOperatorType::TABLE_SCAN, move(types), estimated_cardinality),
      function(move(function_p)), bind_data(move(bind_data_p)), column_ids(move(column_ids_p)), names(move(names_p)),
      table_filters(move(table_filters_p)) {
}

class TableScanGlobalState : public GlobalSourceState {
public:
	TableScanGlobalState(ClientContext &context, const PhysicalTableScan &op) {
		if (!op.function.max_threads || !op.function.init_parallel_state) {
			// table function cannot be parallelized
			return;
		}
		// table function can be parallelized
		// check how many threads we can have
		max_threads = op.function.max_threads(context, op.bind_data.get());
		if (max_threads <= 1) {
			return;
		}
		if (op.function.init_parallel_state) {
			TableFilterCollection collection(op.table_filters.get());
			parallel_state = op.function.init_parallel_state(context, op.bind_data.get(), op.column_ids, &collection);
		}
	}

	idx_t max_threads = 0;
	unique_ptr<ParallelState> parallel_state;

	idx_t MaxThreads() override {
		return max_threads;
	}
};

class TableScanLocalState : public LocalSourceState {
public:
	TableScanLocalState(ExecutionContext &context, TableScanGlobalState &gstate, const PhysicalTableScan &op) {
		TableFilterCollection filters(op.table_filters.get());
		if (gstate.parallel_state) {
			// parallel scan init
			operator_data = op.function.parallel_init(context.client, op.bind_data.get(), gstate.parallel_state.get(),
			                                          op.column_ids, &filters);
		} else if (op.function.init) {
			// sequential scan init
			operator_data = op.function.init(context.client, op.bind_data.get(), op.column_ids, &filters);
		}
	}

	unique_ptr<FunctionOperatorData> operator_data;
};

unique_ptr<LocalSourceState> PhysicalTableScan::GetLocalSourceState(ExecutionContext &context,
                                                                    GlobalSourceState &gstate) const {
	return make_unique<TableScanLocalState>(context, (TableScanGlobalState &)gstate, *this);
}

unique_ptr<GlobalSourceState> PhysicalTableScan::GetGlobalSourceState(ClientContext &context) const {
	return make_unique<TableScanGlobalState>(context, *this);
}

void PhysicalTableScan::GetData(ExecutionContext &context, DataChunk &chunk, GlobalSourceState &gstate_p,
                                LocalSourceState &lstate) const {
	D_ASSERT(!column_ids.empty());
	auto &gstate = (TableScanGlobalState &)gstate_p;
	auto &state = (TableScanLocalState &)lstate;

	if (!gstate.parallel_state) {
		// sequential scan
		function.function(context.client, bind_data.get(), state.operator_data.get(), chunk);
		if (chunk.size() != 0) {
			return;
		}
	} else {
		// parallel scan
		do {
			if (function.parallel_function) {
				function.parallel_function(context.client, bind_data.get(), state.operator_data.get(), chunk,
				                           gstate.parallel_state.get());
			} else {
				function.function(context.client, bind_data.get(), state.operator_data.get(), chunk);
			}

			if (chunk.size() == 0) {
				D_ASSERT(function.parallel_state_next);
				if (function.parallel_state_next(context.client, bind_data.get(), state.operator_data.get(),
				                                 gstate.parallel_state.get())) {
					continue;
				} else {
					break;
				}
			} else {
				return;
			}
		} while (true);
	}
	D_ASSERT(chunk.size() == 0);
	if (function.cleanup) {
		function.cleanup(context.client, bind_data.get(), state.operator_data.get());
	}
}

string PhysicalTableScan::GetName() const {
	return StringUtil::Upper(function.name);
}

string PhysicalTableScan::ParamsToString() const {
	string result;
	if (function.to_string) {
		result = function.to_string(bind_data.get());
		result += "\n[INFOSEPARATOR]\n";
	}
	if (function.projection_pushdown) {
		for (idx_t i = 0; i < column_ids.size(); i++) {
			if (column_ids[i] < names.size()) {
				if (i > 0) {
					result += "\n";
				}
				result += names[column_ids[i]];
			}
		}
	}
	if (function.filter_pushdown && table_filters) {
		result += "\n[INFOSEPARATOR]\n";
		result += "Filters: ";
		for (auto &f : table_filters->filters) {
			auto &column_index = f.first;
			auto &filter = f.second;
			if (column_index < names.size()) {
				result += filter->ToString(names[column_ids[column_index]]);
				result += "\n";
			}
		}
	}
	return result;
}

bool PhysicalTableScan::Equals(const PhysicalOperator &other_p) const {
	if (type != other_p.type) {
		return false;
	}
	auto &other = (PhysicalTableScan &)other_p;
	if (function.function != other.function.function) {
		return false;
	}
	if (column_ids != other.column_ids) {
		return false;
	}
	if (!FunctionData::Equals(bind_data.get(), other.bind_data.get())) {
		return false;
	}
	return true;
}

} // namespace duckdb
