#include "duckdb/function/table/table_scan.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"

#include "duckdb/parallel/task_context.hpp"
#include "duckdb/storage/data_table.hpp"
#include "duckdb/transaction/transaction.hpp"
#include "duckdb/transaction/local_storage.hpp"

#include "duckdb/optimizer/matcher/expression_matcher.hpp"

#include "duckdb/planner/expression/bound_between_expression.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/parallel/parallel_state.hpp"

#include "duckdb/common/mutex.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// Table Scan
//===--------------------------------------------------------------------===//
bool table_scan_parallel_state_next(ClientContext &context, const FunctionData *bind_data,
                                    FunctionOperatorData *operator_state, ParallelState *parallel_state_);

struct TableScanOperatorData : public FunctionOperatorData {
	//! The current position in the scan
	TableScanState scan_state;
	TableFilterSet *table_filters;
	vector<column_t> column_ids;
};

static unique_ptr<FunctionOperatorData> table_scan_init(ClientContext &context, const FunctionData *bind_data_,
                                                        vector<column_t> &column_ids, TableFilterSet *table_filters) {
	auto result = make_unique<TableScanOperatorData>();
	auto &transaction = Transaction::GetTransaction(context);
	auto &bind_data = (const TableScanBindData &)*bind_data_;
	result->column_ids = column_ids;
	result->table_filters = table_filters;
	bind_data.table->storage->InitializeScan(transaction, result->scan_state, result->column_ids, table_filters);
	return move(result);
}

static unique_ptr<BaseStatistics> table_scan_statistics(ClientContext &context, const FunctionData *bind_data_, column_t column_id) {
	auto &bind_data = (const TableScanBindData &)*bind_data_;
	return bind_data.table->storage->GetStatistics(context, column_id);
}

static unique_ptr<FunctionOperatorData> table_scan_parallel_init(ClientContext &context, const FunctionData *bind_data_,
                                                                 ParallelState *state, vector<column_t> &column_ids,
                                                                 TableFilterSet *table_filters) {
	auto result = make_unique<TableScanOperatorData>();
	result->column_ids = column_ids;
	result->table_filters = table_filters;
	if (!table_scan_parallel_state_next(context, bind_data_, result.get(), state)) {
		return nullptr;
	}
	return move(result);
}

static void table_scan_function(ClientContext &context, const FunctionData *bind_data_,
                                FunctionOperatorData *operator_state, DataChunk &output) {
	auto &bind_data = (const TableScanBindData &)*bind_data_;
	auto &state = (TableScanOperatorData &)*operator_state;
	auto &transaction = Transaction::GetTransaction(context);
	bind_data.table->storage->Scan(transaction, output, state.scan_state, state.column_ids);
}

struct ParallelTableFunctionScanState : public ParallelState {
	ParallelTableScanState state;
	std::mutex lock;
};

idx_t table_scan_max_threads(ClientContext &context, const FunctionData *bind_data_) {
	auto &bind_data = (const TableScanBindData &)*bind_data_;
	return bind_data.table->storage->MaxThreads(context);
}

unique_ptr<ParallelState> table_scan_init_parallel_state(ClientContext &context, const FunctionData *bind_data_) {
	auto &bind_data = (const TableScanBindData &)*bind_data_;
	auto result = make_unique<ParallelTableFunctionScanState>();
	bind_data.table->storage->InitializeParallelScan(result->state);
	return move(result);
}

bool table_scan_parallel_state_next(ClientContext &context, const FunctionData *bind_data_,
                                    FunctionOperatorData *operator_state, ParallelState *parallel_state_) {
	auto &bind_data = (const TableScanBindData &)*bind_data_;
	auto &parallel_state = (ParallelTableFunctionScanState &)*parallel_state_;
	auto &state = (TableScanOperatorData &)*operator_state;

	lock_guard<mutex> parallel_lock(parallel_state.lock);
	return bind_data.table->storage->NextParallelScan(context, parallel_state.state, state.scan_state,
	                                                  state.column_ids);
}

void table_scan_dependency(unordered_set<CatalogEntry *> &entries, const FunctionData *bind_data_) {
	auto &bind_data = (const TableScanBindData &)*bind_data_;
	entries.insert(bind_data.table);
}

idx_t table_scan_cardinality(const FunctionData *bind_data_) {
	auto &bind_data = (const TableScanBindData &)*bind_data_;
	return bind_data.table->storage->info->cardinality;
}

//===--------------------------------------------------------------------===//
// Index Scan
//===--------------------------------------------------------------------===//
struct IndexScanOperatorData : public FunctionOperatorData {
	Vector row_ids;
	ColumnFetchState fetch_state;
	LocalScanState local_storage_state;
	vector<column_t> column_ids;
	bool finished;
};

static unique_ptr<FunctionOperatorData> index_scan_init(ClientContext &context, const FunctionData *bind_data_,
                                                        vector<column_t> &column_ids, TableFilterSet *table_filters) {
	auto result = make_unique<IndexScanOperatorData>();
	auto &transaction = Transaction::GetTransaction(context);
	auto &bind_data = (const TableScanBindData &)*bind_data_;
	result->column_ids = column_ids;
	result->row_ids.type = LOGICAL_ROW_TYPE;
	if (bind_data.result_ids.size() > 0) {
		FlatVector::SetData(result->row_ids, (data_ptr_t)&bind_data.result_ids[0]);
	}
	transaction.storage.InitializeScan(bind_data.table->storage.get(), result->local_storage_state, table_filters);

	result->finished = false;
	return move(result);
}

static void index_scan_function(ClientContext &context, const FunctionData *bind_data_,
                                FunctionOperatorData *operator_state, DataChunk &output) {
	auto &bind_data = (const TableScanBindData &)*bind_data_;
	auto &state = (IndexScanOperatorData &)*operator_state;
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

void table_scan_pushdown_complex_filter(ClientContext &context, LogicalGet &get, FunctionData *bind_data_,
                                        vector<unique_ptr<Expression>> &filters) {
	auto &bind_data = (TableScanBindData &)*bind_data_;
	auto table = bind_data.table;
	auto &storage = *table->storage;

	if (bind_data.is_index_scan) {
		return;
	}
	if (filters.size() == 0 || storage.info->indexes.size() == 0) {
		// no indexes or no filters: skip the pushdown
		return;
	}
	// check all the indexes
	for (size_t j = 0; j < storage.info->indexes.size(); j++) {
		auto &index = storage.info->indexes[j];

		// first rewrite the index expression so the ColumnBindings align with the column bindings of the current table
		if (index->unbound_expressions.size() > 1) {
			continue;
		}
		auto index_expression = index->unbound_expressions[0]->Copy();
		bool rewrite_possible = true;
		RewriteIndexExpression(*index, get, *index_expression, rewrite_possible);
		if (!rewrite_possible) {
			// could not rewrite!
			continue;
		}

		Value low_value, high_value, equal_value;
		ExpressionType low_comparison_type = ExpressionType::INVALID, high_comparison_type = ExpressionType::INVALID;
		// try to find a matching index for any of the filter expressions
		for (idx_t i = 0; i < filters.size(); i++) {
			auto expr = filters[i].get();

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
				assert(bindings[0]->GetExpressionClass() == ExpressionClass::BOUND_COMPARISON);
				assert(bindings[2]->type == ExpressionType::VALUE_CONSTANT);

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
		if (!equal_value.is_null || !low_value.is_null || !high_value.is_null) {
			// we can scan this index using this predicate: try a scan
			auto &transaction = Transaction::GetTransaction(context);
			unique_ptr<IndexScanState> index_state;
			if (!equal_value.is_null) {
				// equality predicate
				index_state =
				    index->InitializeScanSinglePredicate(transaction, equal_value, ExpressionType::COMPARE_EQUAL);
			} else if (!low_value.is_null && !high_value.is_null) {
				// two-sided predicate
				index_state = index->InitializeScanTwoPredicates(transaction, low_value, low_comparison_type,
				                                                 high_value, high_comparison_type);
			} else if (!low_value.is_null) {
				// less than predicate
				index_state = index->InitializeScanSinglePredicate(transaction, low_value, low_comparison_type);
			} else {
				assert(!high_value.is_null);
				index_state = index->InitializeScanSinglePredicate(transaction, high_value, high_comparison_type);
			}
			if (index->Scan(transaction, storage, *index_state, STANDARD_VECTOR_SIZE, bind_data.result_ids)) {
				// use an index scan!
				bind_data.is_index_scan = true;
				get.function.init = index_scan_init;
				get.function.function = index_scan_function;
				get.function.max_threads = nullptr;
				get.function.init_parallel_state = nullptr;
				get.function.parallel_state_next = nullptr;
				get.function.filter_pushdown = false;
			} else {
				bind_data.result_ids.clear();
			}
			return;
		}
	}
}

string table_scan_to_string(const FunctionData *bind_data_) {
	auto &bind_data = (const TableScanBindData &)*bind_data_;
	string result = "SEQ_SCAN(" + bind_data.table->name + ")";
	return result;
}

TableFunction TableScanFunction::GetFunction() {
	TableFunction scan_function("seq_scan", {}, table_scan_function);
	scan_function.init = table_scan_init;
	scan_function.statistics = table_scan_statistics;
	scan_function.dependency = table_scan_dependency;
	scan_function.cardinality = table_scan_cardinality;
	scan_function.pushdown_complex_filter = table_scan_pushdown_complex_filter;
	scan_function.to_string = table_scan_to_string;
	scan_function.max_threads = table_scan_max_threads;
	scan_function.init_parallel_state = table_scan_init_parallel_state;
	scan_function.parallel_init = table_scan_parallel_init;
	scan_function.parallel_state_next = table_scan_parallel_state_next;
	scan_function.projection_pushdown = true;
	scan_function.filter_pushdown = true;
	return scan_function;
}

} // namespace duckdb
