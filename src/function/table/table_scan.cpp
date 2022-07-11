#include "duckdb/function/table/table_scan.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"

#include "duckdb/storage/data_table.hpp"
#include "duckdb/transaction/transaction.hpp"
#include "duckdb/transaction/local_storage.hpp"

#include "duckdb/optimizer/matcher/expression_matcher.hpp"

#include "duckdb/planner/expression/bound_between_expression.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/main/client_config.hpp"

#include "duckdb/common/mutex.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// Table Scan
//===--------------------------------------------------------------------===//
bool TableScanParallelStateNext(ClientContext &context, const FunctionData *bind_data_p,
                                LocalTableFunctionState *local_state, GlobalTableFunctionState *gstate);

struct TableScanLocalState : public LocalTableFunctionState {
	//! The current position in the scan
	TableScanState scan_state;
	vector<column_t> column_ids;
};

static storage_t GetStorageIndex(TableCatalogEntry &table, column_t column_id) {
	if (column_id == DConstants::INVALID_INDEX) {
		return column_id;
	}
	auto &col = table.columns[column_id];
	return col.StorageOid();
}

struct TableScanGlobalState : public GlobalTableFunctionState {
	TableScanGlobalState(ClientContext &context, const FunctionData *bind_data_p) {
		D_ASSERT(bind_data_p);
		auto &bind_data = (const TableScanBindData &)*bind_data_p;
		max_threads = bind_data.table->storage->MaxThreads(context);
	}

	ParallelTableScanState state;
	mutex lock;
	idx_t max_threads;

	idx_t MaxThreads() const override {
		return max_threads;
	}
};

static unique_ptr<LocalTableFunctionState> TableScanInitLocal(ExecutionContext &context, TableFunctionInitInput &input,
                                                              GlobalTableFunctionState *gstate) {
	auto result = make_unique<TableScanLocalState>();
	auto &bind_data = (TableScanBindData &)*input.bind_data;
	result->column_ids = input.column_ids;
	for (auto &col : result->column_ids) {
		auto storage_idx = GetStorageIndex(*bind_data.table, col);
		col = storage_idx;
	}
	result->scan_state.table_filters = input.filters;
	TableScanParallelStateNext(context.client, input.bind_data, result.get(), gstate);
	return move(result);
}

unique_ptr<GlobalTableFunctionState> TableScanInitGlobal(ClientContext &context, TableFunctionInitInput &input) {
	D_ASSERT(input.bind_data);
	auto &bind_data = (const TableScanBindData &)*input.bind_data;
	auto result = make_unique<TableScanGlobalState>(context, input.bind_data);
	bind_data.table->storage->InitializeParallelScan(context, result->state);
	return move(result);
}

static unique_ptr<BaseStatistics> TableScanStatistics(ClientContext &context, const FunctionData *bind_data_p,
                                                      column_t column_id) {
	auto &bind_data = (const TableScanBindData &)*bind_data_p;
	auto &transaction = Transaction::GetTransaction(context);
	if (transaction.storage.Find(bind_data.table->storage.get())) {
		// we don't emit any statistics for tables that have outstanding transaction-local data
		return nullptr;
	}
	auto storage_idx = GetStorageIndex(*bind_data.table, column_id);
	return bind_data.table->storage->GetStatistics(context, storage_idx);
}

static void TableScanFunc(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &bind_data = (TableScanBindData &)*data_p.bind_data;
	auto &state = (TableScanLocalState &)*data_p.local_state;
	auto &transaction = Transaction::GetTransaction(context);
	do {
		bind_data.table->storage->Scan(transaction, output, state.scan_state, state.column_ids);
		if (output.size() > 0) {
			return;
		}
		if (!TableScanParallelStateNext(context, data_p.bind_data, data_p.local_state, data_p.global_state)) {
			return;
		}
	} while (true);
}

bool TableScanParallelStateNext(ClientContext &context, const FunctionData *bind_data_p,
                                LocalTableFunctionState *local_state, GlobalTableFunctionState *global_state) {
	auto &bind_data = (const TableScanBindData &)*bind_data_p;
	auto &parallel_state = (TableScanGlobalState &)*global_state;
	auto &state = (TableScanLocalState &)*local_state;

	lock_guard<mutex> parallel_lock(parallel_state.lock);
	return bind_data.table->storage->NextParallelScan(context, parallel_state.state, state.scan_state,
	                                                  state.column_ids);
}

double TableScanProgress(ClientContext &context, const FunctionData *bind_data_p,
                         const GlobalTableFunctionState *gstate) {
	auto &bind_data = (TableScanBindData &)*bind_data_p;
	idx_t total_rows = bind_data.table->storage->GetTotalRows();
	if (total_rows == 0 || total_rows < STANDARD_VECTOR_SIZE) {
		//! Table is either empty or smaller than a vector size, so it is finished
		return 100;
	}
	auto percentage = double(bind_data.chunk_count * STANDARD_VECTOR_SIZE * 100.0) / total_rows;
	if (percentage > 100) {
		//! In case the last chunk has less elements than STANDARD_VECTOR_SIZE, if our percentage is over 100
		//! It means we finished this table.
		return 100;
	}
	return percentage;
}

idx_t TableScanGetBatchIndex(ClientContext &context, const FunctionData *bind_data_p,
                             LocalTableFunctionState *local_state, GlobalTableFunctionState *global_state) {
	auto &bind_data = (const TableScanBindData &)*bind_data_p;
	auto &state = (TableScanLocalState &)*local_state;
	if (state.scan_state.row_group_scan_state.row_group) {
		return state.scan_state.row_group_scan_state.row_group->start;
	}
	if (state.scan_state.local_state.max_index > 0) {
		return bind_data.table->storage->GetTotalRows() + state.scan_state.local_state.chunk_index;
	}
	return 0;
}

void TableScanDependency(unordered_set<CatalogEntry *> &entries, const FunctionData *bind_data_p) {
	auto &bind_data = (const TableScanBindData &)*bind_data_p;
	entries.insert(bind_data.table);
}

unique_ptr<NodeStatistics> TableScanCardinality(ClientContext &context, const FunctionData *bind_data_p) {
	auto &bind_data = (const TableScanBindData &)*bind_data_p;
	auto &transaction = Transaction::GetTransaction(context);
	idx_t estimated_cardinality =
	    bind_data.table->storage->info->cardinality + transaction.storage.AddedRows(bind_data.table->storage.get());
	return make_unique<NodeStatistics>(bind_data.table->storage->info->cardinality, estimated_cardinality);
}

//===--------------------------------------------------------------------===//
// Index Scan
//===--------------------------------------------------------------------===//
struct IndexScanGlobalState : public GlobalTableFunctionState {
	explicit IndexScanGlobalState(data_ptr_t row_id_data) : row_ids(LogicalType::ROW_TYPE, row_id_data) {
	}

	Vector row_ids;
	ColumnFetchState fetch_state;
	LocalScanState local_storage_state;
	vector<column_t> column_ids;
	bool finished;
};

static unique_ptr<GlobalTableFunctionState> IndexScanInitGlobal(ClientContext &context, TableFunctionInitInput &input) {
	auto &bind_data = (const TableScanBindData &)*input.bind_data;
	data_ptr_t row_id_data = nullptr;
	if (!bind_data.result_ids.empty()) {
		row_id_data = (data_ptr_t)&bind_data.result_ids[0];
	}
	auto result = make_unique<IndexScanGlobalState>(row_id_data);
	auto &transaction = Transaction::GetTransaction(context);
	result->column_ids = input.column_ids;
	transaction.storage.InitializeScan(bind_data.table->storage.get(), result->local_storage_state, input.filters);

	result->finished = false;
	return move(result);
}

static void IndexScanFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &bind_data = (const TableScanBindData &)*data_p.bind_data;
	auto &state = (IndexScanGlobalState &)*data_p.global_state;
	auto &transaction = Transaction::GetTransaction(context);
	if (!state.finished) {
		bind_data.table->storage->Fetch(transaction, output, state.column_ids, state.row_ids,
		                                bind_data.result_ids.size(), state.fetch_state);
		state.finished = true;
	}
	if (output.size() == 0) {
		transaction.storage.Scan(state.local_storage_state, state.column_ids, output);
	}
}

static void RewriteIndexExpression(Index &index, LogicalGet &get, Expression &expr, bool &rewrite_possible) {
	if (expr.type == ExpressionType::BOUND_COLUMN_REF) {
		auto &bound_colref = (BoundColumnRefExpression &)expr;
		// bound column ref: rewrite to fit in the current set of bound column ids
		bound_colref.binding.table_index = get.table_index;
		column_t referenced_column = index.column_ids[bound_colref.binding.column_index];
		// search for the referenced column in the set of column_ids
		for (idx_t i = 0; i < get.column_ids.size(); i++) {
			if (get.column_ids[i] == referenced_column) {
				bound_colref.binding.column_index = i;
				return;
			}
		}
		// column id not found in bound columns in the LogicalGet: rewrite not possible
		rewrite_possible = false;
	}
	ExpressionIterator::EnumerateChildren(
	    expr, [&](Expression &child) { RewriteIndexExpression(index, get, child, rewrite_possible); });
}

void TableScanPushdownComplexFilter(ClientContext &context, LogicalGet &get, FunctionData *bind_data_p,
                                    vector<unique_ptr<Expression>> &filters) {
	auto &bind_data = (TableScanBindData &)*bind_data_p;
	auto table = bind_data.table;
	auto &storage = *table->storage;

	auto &config = ClientConfig::GetConfig(context);
	if (!config.enable_optimizer) {
		// we only push index scans if the optimizer is enabled
		return;
	}
	if (bind_data.is_index_scan) {
		return;
	}
	if (filters.empty()) {
		// no indexes or no filters: skip the pushdown
		return;
	}
	// behold
	storage.info->indexes.Scan([&](Index &index) {
		// first rewrite the index expression so the ColumnBindings align with the column bindings of the current table
		if (index.unbound_expressions.size() > 1) {
			return false;
		}
		auto index_expression = index.unbound_expressions[0]->Copy();
		bool rewrite_possible = true;
		RewriteIndexExpression(index, get, *index_expression, rewrite_possible);
		if (!rewrite_possible) {
			// could not rewrite!
			return false;
		}

		Value low_value, high_value, equal_value;
		ExpressionType low_comparison_type = ExpressionType::INVALID, high_comparison_type = ExpressionType::INVALID;
		// try to find a matching index for any of the filter expressions
		for (auto &filter : filters) {
			auto expr = filter.get();

			// create a matcher for a comparison with a constant
			ComparisonExpressionMatcher matcher;
			// match on a comparison type
			matcher.expr_type = make_unique<ComparisonExpressionTypeMatcher>();
			// match on a constant comparison with the indexed expression
			matcher.matchers.push_back(make_unique<ExpressionEqualityMatcher>(index_expression.get()));
			matcher.matchers.push_back(make_unique<ConstantExpressionMatcher>());

			matcher.policy = SetMatcher::Policy::UNORDERED;

			vector<Expression *> bindings;
			if (matcher.Match(expr, bindings)) {
				// range or equality comparison with constant value
				// we can use our index here
				// bindings[0] = the expression
				// bindings[1] = the index expression
				// bindings[2] = the constant
				auto comparison = (BoundComparisonExpression *)bindings[0];
				D_ASSERT(bindings[0]->GetExpressionClass() == ExpressionClass::BOUND_COMPARISON);
				D_ASSERT(bindings[2]->type == ExpressionType::VALUE_CONSTANT);

				auto constant_value = ((BoundConstantExpression *)bindings[2])->value;
				auto comparison_type = comparison->type;
				if (comparison->left->type == ExpressionType::VALUE_CONSTANT) {
					// the expression is on the right side, we flip them around
					comparison_type = FlipComparisionExpression(comparison_type);
				}
				if (comparison_type == ExpressionType::COMPARE_EQUAL) {
					// equality value
					// equality overrides any other bounds so we just break here
					equal_value = constant_value;
					break;
				} else if (comparison_type == ExpressionType::COMPARE_GREATERTHANOREQUALTO ||
				           comparison_type == ExpressionType::COMPARE_GREATERTHAN) {
					// greater than means this is a lower bound
					low_value = constant_value;
					low_comparison_type = comparison_type;
				} else {
					// smaller than means this is an upper bound
					high_value = constant_value;
					high_comparison_type = comparison_type;
				}
			} else if (expr->type == ExpressionType::COMPARE_BETWEEN) {
				// BETWEEN expression
				auto &between = (BoundBetweenExpression &)*expr;
				if (!between.input->Equals(index_expression.get())) {
					// expression doesn't match the current index expression
					continue;
				}
				if (between.lower->type != ExpressionType::VALUE_CONSTANT ||
				    between.upper->type != ExpressionType::VALUE_CONSTANT) {
					// not a constant comparison
					continue;
				}
				low_value = ((BoundConstantExpression &)*between.lower).value;
				low_comparison_type = between.lower_inclusive ? ExpressionType::COMPARE_GREATERTHANOREQUALTO
				                                              : ExpressionType::COMPARE_GREATERTHAN;
				high_value = ((BoundConstantExpression &)*between.upper).value;
				high_comparison_type = between.upper_inclusive ? ExpressionType::COMPARE_LESSTHANOREQUALTO
				                                               : ExpressionType::COMPARE_LESSTHAN;
				break;
			}
		}
		if (!equal_value.IsNull() || !low_value.IsNull() || !high_value.IsNull()) {
			// we can scan this index using this predicate: try a scan
			auto &transaction = Transaction::GetTransaction(context);
			unique_ptr<IndexScanState> index_state;
			if (!equal_value.IsNull()) {
				// equality predicate
				index_state =
				    index.InitializeScanSinglePredicate(transaction, equal_value, ExpressionType::COMPARE_EQUAL);
			} else if (!low_value.IsNull() && !high_value.IsNull()) {
				// two-sided predicate
				index_state = index.InitializeScanTwoPredicates(transaction, low_value, low_comparison_type, high_value,
				                                                high_comparison_type);
			} else if (!low_value.IsNull()) {
				// less than predicate
				index_state = index.InitializeScanSinglePredicate(transaction, low_value, low_comparison_type);
			} else {
				D_ASSERT(!high_value.IsNull());
				index_state = index.InitializeScanSinglePredicate(transaction, high_value, high_comparison_type);
			}
			if (index.Scan(transaction, storage, *index_state, STANDARD_VECTOR_SIZE, bind_data.result_ids)) {
				// use an index scan!
				bind_data.is_index_scan = true;
				get.function.name = "index_scan";
				get.function.init_local = nullptr;
				get.function.init_global = IndexScanInitGlobal;
				get.function.function = IndexScanFunction;
				get.function.table_scan_progress = nullptr;
				get.function.get_batch_index = nullptr;
				get.function.filter_pushdown = false;
			} else {
				bind_data.result_ids.clear();
			}
			return true;
		}
		return false;
	});
}

string TableScanToString(const FunctionData *bind_data_p) {
	auto &bind_data = (const TableScanBindData &)*bind_data_p;
	string result = bind_data.table->name;
	return result;
}

TableFunction TableScanFunction::GetFunction() {
	TableFunction scan_function("seq_scan", {}, TableScanFunc);
	scan_function.init_local = TableScanInitLocal;
	scan_function.init_global = TableScanInitGlobal;
	scan_function.statistics = TableScanStatistics;
	scan_function.dependency = TableScanDependency;
	scan_function.cardinality = TableScanCardinality;
	scan_function.pushdown_complex_filter = TableScanPushdownComplexFilter;
	scan_function.to_string = TableScanToString;
	scan_function.table_scan_progress = TableScanProgress;
	scan_function.get_batch_index = TableScanGetBatchIndex;
	scan_function.projection_pushdown = true;
	scan_function.filter_pushdown = true;
	return scan_function;
}

TableCatalogEntry *TableScanFunction::GetTableEntry(const TableFunction &function, const FunctionData *bind_data_p) {
	if (function.function != TableScanFunc || !bind_data_p) {
		return nullptr;
	}
	auto &bind_data = (TableScanBindData &)*bind_data_p;
	return bind_data.table;
}

} // namespace duckdb
