#include "duckdb/function/table/table_scan.hpp"

#include "duckdb/catalog/catalog_entry/duck_table_entry.hpp"
#include "duckdb/catalog/dependency_list.hpp"
#include "duckdb/common/enums/expression_type.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/common/serializer/deserializer.hpp"
#include "duckdb/common/serializer/serializer.hpp"
#include "duckdb/common/typedefs.hpp"
#include "duckdb/common/types/value_map.hpp"
#include "duckdb/common/unique_ptr.hpp"
#include "duckdb/execution/index/art/art.hpp"
#include "duckdb/function/function_set.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/client_config.hpp"
#include "duckdb/main/client_data.hpp"
#include "duckdb/main/settings.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/planner/expression.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_operator_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/filter/conjunction_filter.hpp"
#include "duckdb/planner/filter/expression_filter.hpp"
#include "duckdb/planner/filter/optional_filter.hpp"
#include "duckdb/transaction/duck_transaction.hpp"
#include "duckdb/transaction/local_storage.hpp"
#include "duckdb/storage/data_table.hpp"
#include "duckdb/storage/storage_index.hpp"
#include "duckdb/storage/table/data_table_info.hpp"
#include "duckdb/storage/table/scan_state.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/transaction/duck_transaction_manager.hpp"

namespace duckdb {

struct TableScanLocalState : public LocalTableFunctionState {
	//! The current position in the scan.
	TableScanState scan_state;
	//! The DataChunk containing all read columns.
	//! This includes filter columns, which are immediately removed.
	DataChunk all_columns;

	idx_t rows_scanned = 0;
	idx_t rows_in_current_row_group = 0;
};

struct IndexScanLocalState : public LocalTableFunctionState {
	//! The batch index, which determines the offset in the row ID vector.
	idx_t batch_index;
	//! The DataChunk containing all read columns.
	//! This includes filter columns, which are immediately removed.
	DataChunk all_columns;
	//! The row fetch state.
	ColumnFetchState fetch_state;
	//! The current position in the local storage scan.
	TableScanState scan_state;
	//! The column IDs of the local storage scan.
	vector<StorageIndex> column_ids;
	bool in_charge_of_final_stretch {false};
	idx_t rows_scanned = 0;
};

class TableScanGlobalState : public GlobalTableFunctionState {
public:
	TableScanGlobalState(ClientContext &context, const FunctionData *bind_data_p) {
		D_ASSERT(bind_data_p);
		auto &bind_data = bind_data_p->Cast<TableScanBindData>();
		auto &duck_table = bind_data.table.Cast<DuckTableEntry>();
		max_threads = duck_table.GetStorage().MaxThreads(context);
	}

	//! The maximum number of threads for this table scan.
	idx_t max_threads;
	//! The projected columns of this table scan.
	vector<idx_t> projection_ids;
	//! The types of all scanned columns.
	vector<LogicalType> scanned_types;

public:
	virtual unique_ptr<LocalTableFunctionState> InitLocalState(ExecutionContext &context,
	                                                           TableFunctionInitInput &input) = 0;
	virtual void TableScanFunc(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) = 0;
	virtual double TableScanProgress(ClientContext &context, const FunctionData *bind_data_p) const = 0;
	virtual OperatorPartitionData TableScanGetPartitionData(ClientContext &context,
	                                                        TableFunctionGetPartitionInput &input) = 0;
	virtual idx_t TableScanRowsScanned(LocalTableFunctionState &state) = 0;

	idx_t MaxThreads() const override {
		return max_threads;
	}
	bool CanRemoveFilterColumns() const {
		return !projection_ids.empty();
	}
};

class DuckIndexScanState : public TableScanGlobalState {
public:
	DuckIndexScanState(ClientContext &context, const FunctionData *bind_data_p)
	    : TableScanGlobalState(context, bind_data_p), next_batch_index(0), arena(Allocator::Get(context)),
	      row_ids(nullptr), row_id_count(0), finished_first_phase(false), started_last_phase(false) {
	}

	//! The batch index of the next Sink.
	//! Also determines the offset of the next chunk. I.e., offset = next_batch_index * STANDARD_VECTOR_SIZE.
	atomic<idx_t> next_batch_index;
	//! The arena allocator containing the memory of the row IDs.
	ArenaAllocator arena;
	//! A pointer to the row IDs.
	row_t *row_ids;
	//! The number of scanned row IDs.
	idx_t row_id_count;
	//! The column IDs of the to-be-scanned columns.
	vector<StorageIndex> column_ids;
	//! True, if no more row IDs must be scanned.
	bool finished_first_phase;
	bool started_last_phase;
	//! Synchronize changes to the global index scan state.
	mutex index_scan_lock;
	//! Synchronize <ART version, SegmentTree<RowGroup>> when vacuum_rebuild_indexes is enabled (since
	//! ART indexes are rebuilt during vacuuming with this setting).
	unique_ptr<StorageLockKey> vacuum_lock;

public:
	unique_ptr<LocalTableFunctionState> InitLocalState(ExecutionContext &context,
	                                                   TableFunctionInitInput &input) override {
		auto l_state = make_uniq<IndexScanLocalState>();
		if (input.CanRemoveFilterColumns()) {
			l_state->all_columns.Initialize(context.client, scanned_types);
		}
		l_state->scan_state.options.force_fetch_row = ClientConfig::GetConfig(context.client).force_fetch_row;

		// Initialize the local storage scan.
		auto &bind_data = input.bind_data->Cast<TableScanBindData>();
		auto &duck_table = bind_data.table.Cast<DuckTableEntry>();
		auto &storage = duck_table.GetStorage();
		auto &local_storage = LocalStorage::Get(context.client, duck_table.catalog);

		for (const auto &col_idx : input.column_indexes) {
			l_state->column_ids.push_back(bind_data.table.GetStorageIndex(col_idx));
		}
		l_state->scan_state.Initialize(l_state->column_ids, context.client, input.filters.get());
		local_storage.InitializeScan(storage, l_state->scan_state.local_state, input.filters);
		return std::move(l_state);
	}

	void TableScanFunc(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) override {
		auto &bind_data = data_p.bind_data->Cast<TableScanBindData>();
		auto &duck_table = bind_data.table.Cast<DuckTableEntry>();
		auto &tx = DuckTransaction::Get(context, duck_table.catalog);
		auto &storage = duck_table.GetStorage();
		auto &l_state = data_p.local_state->Cast<IndexScanLocalState>();

		enum class ExecutionPhase { NONE = 0, STORAGE = 1, LOCAL_STORAGE = 2 };

		// We might need to loop back, so while (true)
		while (true) {
			idx_t scan_count = 0;
			idx_t offset = 0;

			// Phase selection
			auto phase_to_be_performed = ExecutionPhase::NONE;
			{
				// Synchronize changes to the shared global state.
				lock_guard<mutex> l(index_scan_lock);
				if (!finished_first_phase) {
					l_state.batch_index = next_batch_index;
					next_batch_index++;

					offset = l_state.batch_index * STANDARD_VECTOR_SIZE;
					auto remaining = row_id_count - offset;
					scan_count = remaining <= STANDARD_VECTOR_SIZE ? remaining : STANDARD_VECTOR_SIZE;
					finished_first_phase = remaining <= STANDARD_VECTOR_SIZE ? true : false;
					phase_to_be_performed = ExecutionPhase::STORAGE;
				} else if (!started_last_phase) {
					// First thread to get last phase, great, set l_state's in_charge_of_final_stretch, so same thread
					// will be on again
					started_last_phase = true;
					l_state.in_charge_of_final_stretch = true;
					phase_to_be_performed = ExecutionPhase::LOCAL_STORAGE;
				} else if (l_state.in_charge_of_final_stretch) {
					phase_to_be_performed = ExecutionPhase::LOCAL_STORAGE;
				}
			}

			switch (phase_to_be_performed) {
			case ExecutionPhase::NONE: {
				// No work to be picked up
				return;
			}
			case ExecutionPhase::STORAGE: {
				// Scan (in parallel) storage
				auto row_id_data = reinterpret_cast<data_ptr_t>(row_ids + offset);
				Vector local_vector(LogicalType::ROW_TYPE, row_id_data, scan_count);

				if (CanRemoveFilterColumns()) {
					l_state.all_columns.Reset();
					storage.Fetch(tx, l_state.all_columns, column_ids, local_vector, scan_count, l_state.fetch_state);
					output.ReferenceColumns(l_state.all_columns, projection_ids);
				} else {
					storage.Fetch(tx, output, column_ids, local_vector, scan_count, l_state.fetch_state);
				}

				l_state.rows_scanned += scan_count;

				if (output.size() == 0) {
					if (data_p.results_execution_mode == AsyncResultsExecutionMode::TASK_EXECUTOR) {
						// We can avoid looping, and just return as appropriate
						data_p.async_result = AsyncResultType::HAVE_MORE_OUTPUT;
						return;
					}

					// output is empty, loop back, since there might be results to be picked up from LOCAL_STORAGE phase
					continue;
				}
				return;
			}
			case ExecutionPhase::LOCAL_STORAGE: {
				// Scan (sequentially, always same logical thread) local_storage
				auto &local_storage = LocalStorage::Get(tx);
				{
					if (CanRemoveFilterColumns()) {
						l_state.all_columns.Reset();
						local_storage.Scan(l_state.scan_state.local_state, column_ids, l_state.all_columns);
						output.ReferenceColumns(l_state.all_columns, projection_ids);
					} else {
						local_storage.Scan(l_state.scan_state.local_state, column_ids, output);
					}
					l_state.rows_scanned += output.size();
				}
				return;
			}
			}
		}
	}

	double TableScanProgress(ClientContext &context, const FunctionData *bind_data_p) const override {
		if (row_id_count == 0) {
			return 100;
		}
		auto scanned_rows = next_batch_index * STANDARD_VECTOR_SIZE;
		auto percentage = 100 * (static_cast<double>(scanned_rows) / static_cast<double>(row_id_count));
		return percentage > 100 ? 100 : percentage;
	}

	OperatorPartitionData TableScanGetPartitionData(ClientContext &context,
	                                                TableFunctionGetPartitionInput &input) override {
		auto &l_state = input.local_state->Cast<IndexScanLocalState>();
		return OperatorPartitionData(l_state.batch_index);
	}

	idx_t TableScanRowsScanned(LocalTableFunctionState &state) override {
		auto &l_state = state.Cast<IndexScanLocalState>();
		return l_state.rows_scanned;
	}
};

class DuckTableScanState : public TableScanGlobalState {
public:
	DuckTableScanState(ClientContext &context, const FunctionData *bind_data_p)
	    : TableScanGlobalState(context, bind_data_p), bind_data(bind_data_p->Cast<TableScanBindData>()),
	      duck_table(bind_data.table.Cast<DuckTableEntry>()), tx(DuckTransaction::Get(context, duck_table.catalog)),
	      storage(duck_table.GetStorage()), total_rows(storage.GetTotalRows()) {
	}

public:
	ParallelTableScanState state;

private:
	const TableScanBindData &bind_data;
	DuckTableEntry &duck_table;
	DuckTransaction &tx;
	DataTable &storage;
	const idx_t total_rows;

public:
	unique_ptr<LocalTableFunctionState> InitLocalState(ExecutionContext &context,
	                                                   TableFunctionInitInput &input) override {
		auto l_state = make_uniq<TableScanLocalState>();

		vector<StorageIndex> storage_ids;
		for (auto &col : input.column_indexes) {
			storage_ids.push_back(bind_data.table.GetStorageIndex(col));
		}

		if (bind_data.order_options) {
			l_state->scan_state.table_state.reorderer = make_uniq<RowGroupReorderer>(*bind_data.order_options);
			l_state->scan_state.local_state.reorderer = make_uniq<RowGroupReorderer>(*bind_data.order_options);
		}

		l_state->scan_state.Initialize(std::move(storage_ids), context.client, input.filters, input.sample_options);

		l_state->rows_in_current_row_group = storage.NextParallelScan(context.client, state, l_state->scan_state);
		if (input.CanRemoveFilterColumns()) {
			l_state->all_columns.Initialize(context.client, scanned_types);
		}

		l_state->scan_state.options.force_fetch_row = ClientConfig::GetConfig(context.client).force_fetch_row;
		return std::move(l_state);
	}

	void TableScanFunc(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) override {
		auto &l_state = data_p.local_state->Cast<TableScanLocalState>();
		l_state.scan_state.options.force_fetch_row = ClientConfig::GetConfig(context).force_fetch_row;

		do {
			if (bind_data.is_create_index) {
				storage.CreateIndexScan(l_state.scan_state, output);
			} else if (CanRemoveFilterColumns()) {
				l_state.all_columns.Reset();
				storage.Scan(tx, l_state.all_columns, l_state.scan_state);
				output.ReferenceColumns(l_state.all_columns, projection_ids);
			} else {
				storage.Scan(tx, output, l_state.scan_state);
			}
			if (output.size() > 0) {
				return;
			}

			// We have fully processed a row group. Add to scanned_rows
			l_state.rows_scanned += l_state.rows_in_current_row_group;
			l_state.rows_in_current_row_group = storage.NextParallelScan(context, state, l_state.scan_state);

			if (data_p.results_execution_mode == AsyncResultsExecutionMode::TASK_EXECUTOR) {
				// We can avoid looping, and just return as appropriate
				if (l_state.rows_in_current_row_group == 0) {
					data_p.async_result = AsyncResultType::FINISHED;
				} else {
					data_p.async_result = AsyncResultType::HAVE_MORE_OUTPUT;
				}
				return;
			}
			if (l_state.rows_in_current_row_group == 0) {
				return;
			}

			// Before looping back, check if we are interrupted
			context.InterruptCheck();
		} while (true);
	}

	double TableScanProgress(ClientContext &context, const FunctionData *bind_data_p) const override {
		// The table is empty or smaller than the standard vector size.
		if (total_rows == 0) {
			return 100;
		}

		idx_t scanned_rows = state.scan_state.processed_rows;
		scanned_rows += state.local_state.processed_rows;
		auto percentage = 100 * (static_cast<double>(scanned_rows) / static_cast<double>(total_rows));
		if (percentage > 100) {
			// If the last chunk has fewer elements than STANDARD_VECTOR_SIZE, and if our percentage is over 100,
			// then we finished this table.
			return 100;
		}
		return percentage;
	}

	OperatorPartitionData TableScanGetPartitionData(ClientContext &context,
	                                                TableFunctionGetPartitionInput &input) override {
		auto &l_state = input.local_state->Cast<TableScanLocalState>();
		if (l_state.scan_state.table_state.row_group) {
			return OperatorPartitionData(l_state.scan_state.table_state.batch_index);
		}
		if (l_state.scan_state.local_state.row_group) {
			return OperatorPartitionData(l_state.scan_state.table_state.batch_index +
			                             l_state.scan_state.local_state.batch_index);
		}
		return OperatorPartitionData(0);
	}

	idx_t TableScanRowsScanned(LocalTableFunctionState &state) override {
		auto &l_state = state.Cast<TableScanLocalState>();
		return l_state.rows_scanned;
	}
};

static unique_ptr<LocalTableFunctionState> TableScanInitLocal(ExecutionContext &context, TableFunctionInitInput &input,
                                                              GlobalTableFunctionState *g_state) {
	auto &cast_g_state = g_state->Cast<TableScanGlobalState>();
	return cast_g_state.InitLocalState(context, input);
}

unique_ptr<GlobalTableFunctionState> DuckTableScanInitGlobal(ClientContext &context, TableFunctionInitInput &input,
                                                             DataTable &storage, const TableScanBindData &bind_data) {
	auto g_state = make_uniq<DuckTableScanState>(context, input.bind_data.get());
	if (bind_data.order_options) {
		g_state->state.scan_state.reorderer = make_uniq<RowGroupReorderer>(*bind_data.order_options);
		g_state->state.local_state.reorderer = make_uniq<RowGroupReorderer>(*bind_data.order_options);
	}

	// Check if row_number column is requested and initialize row_number_base
	for (idx_t i = 0; i < input.column_ids.size(); i++) {
		if (input.column_ids[i] == COLUMN_IDENTIFIER_ROW_NUMBER) {
			g_state->state.scan_state.row_number_base = 0;
			break;
		}
	}
	storage.InitializeParallelScan(context, g_state->state, input.column_indexes);
	if (!input.CanRemoveFilterColumns()) {
		return std::move(g_state);
	}

	g_state->projection_ids = input.projection_ids;
	auto &duck_table = bind_data.table.Cast<DuckTableEntry>();
	const auto &columns = duck_table.GetColumns();
	for (const auto &col_idx : input.column_indexes) {
		if (col_idx.IsRowIdColumn() || col_idx.IsRowNumberColumn()) {
			g_state->scanned_types.emplace_back(LogicalType::ROW_TYPE);
		} else if (col_idx.HasType()) {
			g_state->scanned_types.push_back(col_idx.GetScanType());
		} else {
			g_state->scanned_types.push_back(columns.GetColumn(col_idx.ToLogical()).Type());
		}
	}
	return std::move(g_state);
}

unique_ptr<GlobalTableFunctionState> DuckIndexScanInitGlobal(ClientContext &context, TableFunctionInitInput &input,
                                                             const TableScanBindData &bind_data, set<row_t> &row_ids,
                                                             unique_ptr<StorageLockKey> vacuum_lock) {
	auto g_state = make_uniq<DuckIndexScanState>(context, input.bind_data.get());
	g_state->vacuum_lock = std::move(vacuum_lock);
	g_state->finished_first_phase = row_ids.empty() ? true : false;
	g_state->started_last_phase = false;

	if (!row_ids.empty()) {
		auto row_id_ptr = g_state->arena.AllocateAligned(row_ids.size() * sizeof(row_t));
		g_state->row_ids = reinterpret_cast<row_t *>(row_id_ptr);
		g_state->row_id_count = row_ids.size();

		idx_t row_id_count = 0;
		for (const auto row_id : row_ids) {
			g_state->row_ids[row_id_count++] = row_id;
		}
	}

	auto &duck_table = bind_data.table.Cast<DuckTableEntry>();
	if (input.CanRemoveFilterColumns()) {
		g_state->projection_ids = input.projection_ids;
	}

	const auto &columns = duck_table.GetColumns();
	for (const auto &col_idx : input.column_indexes) {
		g_state->column_ids.push_back(bind_data.table.GetStorageIndex(col_idx));
		if (col_idx.IsRowIdColumn()) {
			g_state->scanned_types.emplace_back(LogicalType::ROW_TYPE);
		} else if (col_idx.HasType()) {
			g_state->scanned_types.emplace_back(col_idx.GetScanType());
		} else {
			g_state->scanned_types.push_back(columns.GetColumn(col_idx.ToLogical()).Type());
		}
	}

	// Const-cast to indicate an index scan.
	// We need this information in the bind data so that we can access it during ANALYZE.
	auto &no_const_bind_data = bind_data.CastNoConst<TableScanBindData>();
	no_const_bind_data.is_index_scan = true;

	return std::move(g_state);
}

struct ComparisonCondition {
	ExpressionType type;
	Value constant;
};

static bool CollectValuesAndComparisonsFromExpression(const Expression &expr, value_set_t &in_values,
                                                      vector<ComparisonCondition> &comparisons) {
	if (expr.GetExpressionClass() == ExpressionClass::BOUND_OPERATOR && expr.type == ExpressionType::COMPARE_IN) {
		auto &op = expr.Cast<BoundOperatorExpression>();
		if (op.children.empty() || op.children[0]->GetExpressionClass() != ExpressionClass::BOUND_REF) {
			return false;
		}
		for (idx_t i = 1; i < op.children.size(); i++) {
			if (op.children[i]->type != ExpressionType::VALUE_CONSTANT) {
				return false;
			}
			auto &value = op.children[i]->Cast<BoundConstantExpression>().value;
			if (!value.IsNull()) {
				in_values.insert(value);
			}
		}
		return true;
	}
	if (expr.GetExpressionClass() == ExpressionClass::BOUND_COMPARISON) {
		auto &comp = expr.Cast<BoundComparisonExpression>();
		Value val;
		bool left_is_ref = comp.left->GetExpressionClass() == ExpressionClass::BOUND_REF;
		bool right_is_ref = comp.right->GetExpressionClass() == ExpressionClass::BOUND_REF;
		if (comp.right->type == ExpressionType::VALUE_CONSTANT && left_is_ref) {
			val = comp.right->Cast<BoundConstantExpression>().value;
		} else if (comp.left->type == ExpressionType::VALUE_CONSTANT && right_is_ref) {
			val = comp.left->Cast<BoundConstantExpression>().value;
		} else {
			return false;
		}
		if (val.IsNull()) {
			return false;
		}
		if (comp.type == ExpressionType::COMPARE_EQUAL) {
			in_values.insert(val);
		}
		comparisons.push_back({comp.type, std::move(val)});
		return true;
	}
	if (expr.GetExpressionClass() == ExpressionClass::BOUND_CONJUNCTION &&
	    expr.type == ExpressionType::CONJUNCTION_AND) {
		auto &conj = expr.Cast<BoundConjunctionExpression>();
		for (auto &child : conj.children) {
			if (!CollectValuesAndComparisonsFromExpression(*child, in_values, comparisons)) {
				return false;
			}
		}
		return true;
	}
	return false;
}

//! Check if a value qualifies against all extracted comparison conditions.
static bool ValueQualifies(const Value &value, const vector<ComparisonCondition> &comparisons) {
	for (auto &comp : comparisons) {
		bool passes;
		switch (comp.type) {
		case ExpressionType::COMPARE_EQUAL:
			passes = ValueOperations::Equals(value, comp.constant);
			break;
		case ExpressionType::COMPARE_NOTEQUAL:
			passes = ValueOperations::NotEquals(value, comp.constant);
			break;
		case ExpressionType::COMPARE_GREATERTHAN:
			passes = ValueOperations::GreaterThan(value, comp.constant);
			break;
		case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
			passes = ValueOperations::GreaterThanEquals(value, comp.constant);
			break;
		case ExpressionType::COMPARE_LESSTHAN:
			passes = ValueOperations::LessThan(value, comp.constant);
			break;
		case ExpressionType::COMPARE_LESSTHANOREQUALTO:
			passes = ValueOperations::LessThanEquals(value, comp.constant);
			break;
		default:
			return true;
		}
		if (!passes) {
			return false;
		}
	}
	return true;
}

static bool CollectValuesAndComparisonsFromTableFilter(const TableFilter &filter, value_set_t &in_values,
                                                       vector<ComparisonCondition> &comparisons) {
	switch (filter.filter_type) {
	case TableFilterType::EXPRESSION_FILTER:
		return CollectValuesAndComparisonsFromExpression(*filter.Cast<ExpressionFilter>().expr, in_values, comparisons);
	case TableFilterType::OPTIONAL_FILTER: {
		auto &optional_filter = filter.Cast<OptionalFilter>();
		if (!optional_filter.child_filter) {
			return true; // No child filters, always OK
		}
		return CollectValuesAndComparisonsFromTableFilter(*optional_filter.child_filter, in_values, comparisons);
	}
	case TableFilterType::CONJUNCTION_AND: {
		auto &conjunction_and = filter.Cast<ConjunctionAndFilter>();
		for (auto &child_filter : conjunction_and.child_filters) {
			if (!CollectValuesAndComparisonsFromTableFilter(*child_filter, in_values, comparisons)) {
				return false;
			}
		}
		return true;
	}
	case TableFilterType::BLOOM_FILTER:
	case TableFilterType::PERFECT_HASH_JOIN_FILTER:
	case TableFilterType::PREFIX_RANGE_FILTER:
		return true;
	default:
		return false;
	}
}

static bool ExtractValuesFromTableFilter(const TableFilter &filter, value_set_t &values) {
	value_set_t in_values;
	vector<ComparisonCondition> comparisons;
	if (!CollectValuesAndComparisonsFromTableFilter(filter, in_values, comparisons) || in_values.empty()) {
		return false;
	}
	for (auto &value : in_values) {
		if (ValueQualifies(value, comparisons)) {
			values.insert(value);
		}
	}
	return !values.empty();
}

void ExtractExpressionsFromValues(const value_set_t &unique_values, BoundColumnRefExpression &bound_ref,
                                  vector<unique_ptr<Expression>> &expressions) {
	for (const auto &value : unique_values) {
		auto bound_constant = make_uniq<BoundConstantExpression>(value);
		auto filter_expr = make_uniq<BoundComparisonExpression>(ExpressionType::COMPARE_EQUAL, bound_ref.Copy(),
		                                                        std::move(bound_constant));
		expressions.push_back(std::move(filter_expr));
	}
}

vector<unique_ptr<Expression>> ExtractFilterExpressions(const ColumnDefinition &col, const TableFilter &filter,
                                                        idx_t storage_idx) {
	ColumnBinding binding(TableIndex(0), ProjectionIndex(storage_idx));
	auto bound_ref = make_uniq<BoundColumnRefExpression>(col.Name(), col.Type(), binding);

	// Extract all exact values we can derive from the filter tree.
	vector<unique_ptr<Expression>> expressions;
	value_set_t values;
	if (ExtractValuesFromTableFilter(filter, values)) {
		ExtractExpressionsFromValues(values, *bound_ref, expressions);
	}

	// Attempt matching the top-level filter to the index expression.
	if (expressions.empty()) {
		auto &expr_filter = ExpressionFilter::GetExpressionFilter(filter, "ExtractFilterExpressions");
		auto filter_expr = expr_filter.ToExpression(*bound_ref);
		expressions.push_back(std::move(filter_expr));
	}

	return expressions;
}

bool TryScanIndex(ART &art, IndexEntry &entry, const ColumnList &column_list, TableFunctionInitInput &input,
                  TableFilterSet &filter_set, idx_t max_count, set<row_t> &row_ids) {
	// FIXME: No support for index scans on compound ARTs.
	// See note above on multi-filter support.
	if (art.unbound_expressions.size() > 1) {
		return false;
	}

	auto index_expr = art.unbound_expressions[0]->Copy();
	auto &indexed_columns = art.GetColumnIds();

	// NOTE: We do not push down multi-column filters, e.g., 42 = a + b.
	if (indexed_columns.size() != 1) {
		return false;
	}

	// Resolve bound column references in the index_expr against the current input projection
	ProjectionIndex updated_index_column;
	bool found_index_column_in_input = false;

	// Find the indexed column amongst the input columns
	for (idx_t i = 0; i < input.column_ids.size(); ++i) {
		if (input.column_ids[i] == indexed_columns[0]) {
			updated_index_column = ProjectionIndex(i);
			found_index_column_in_input = true;
			break;
		}
	}

	// If found, update the bound column ref within index_expr
	if (found_index_column_in_input) {
		ExpressionIterator::EnumerateExpression(index_expr, [&](Expression &expr) {
			if (expr.GetExpressionClass() != ExpressionClass::BOUND_COLUMN_REF) {
				return;
			}

			auto &bound_column_ref_expr = expr.Cast<BoundColumnRefExpression>();

			// If the bound column references the index column, use updated_index_column
			if (bound_column_ref_expr.binding.column_index == indexed_columns[0]) {
				bound_column_ref_expr.binding.column_index = updated_index_column;
			}
		});
	}

	// Get ART column.
	auto &col = column_list.GetColumn(LogicalIndex(indexed_columns[0]));

	// The indexes of the filters match input.column_indexes, which are: i -> column_index.
	// Try to find a filter on the ART column.
	ProjectionIndex storage_index;
	for (idx_t i = 0; i < input.column_indexes.size(); i++) {
		if (input.column_indexes[i].ToLogical() == col.Logical()) {
			storage_index = ProjectionIndex(i);
			break;
		}
	}

	// No filter matches the ART column.
	if (!storage_index.IsValid()) {
		return false;
	}

	// Try to find a matching filter for the column.
	auto filter = filter_set.TryGetFilterByColumnIndex(storage_index);
	if (!filter) {
		return false;
	}

	lock_guard<mutex> guard(entry.lock);
	vector<reference<ART>> arts_to_scan;
	arts_to_scan.push_back(art);
	if (entry.deleted_rows_in_use) {
		if (entry.deleted_rows_in_use->GetIndexType() != ART::TYPE_NAME) {
			throw InternalException("Concurrent changes made to a non-ART index");
		}
		arts_to_scan.push_back(entry.deleted_rows_in_use->Cast<ART>());
	}
	if (entry.added_data_during_checkpoint) {
		if (entry.added_data_during_checkpoint->GetIndexType() != ART::TYPE_NAME) {
			throw InternalException("Concurrent changes made to a non-ART index");
		}
		arts_to_scan.push_back(entry.added_data_during_checkpoint->Cast<ART>());
	}

	auto expressions = ExtractFilterExpressions(col, *filter, storage_index.GetIndex());
	for (const auto &filter_expr : expressions) {
		for (auto &art_ref : arts_to_scan) {
			auto &art_to_scan = art_ref.get();
			auto scan_state = art_to_scan.TryInitializeScan(*index_expr, *filter_expr);
			if (!scan_state) {
				return false;
			}

			// Check if we can use an index scan, and already retrieve the matching row ids.
			if (!art_to_scan.Scan(*scan_state, max_count, row_ids)) {
				row_ids.clear();
				return false;
			}
		}
	}
	return true;
}

unique_ptr<GlobalTableFunctionState> TableScanInitGlobal(ClientContext &context, TableFunctionInitInput &input) {
	D_ASSERT(input.bind_data);

	auto &bind_data = input.bind_data->Cast<TableScanBindData>();
	auto &duck_table = bind_data.table.Cast<DuckTableEntry>();
	auto &storage = duck_table.GetStorage();

	// Can't index scan without filters.
	if (!input.filters) {
		return DuckTableScanInitGlobal(context, input, storage, bind_data);
	}
	auto &filter_set = *input.filters;

	// FIXME: We currently only support scanning one ART with one filter.
	// If multiple filters exist, i.e., a = 11 AND b = 24, we need to
	// 1.	1.1. Find + scan one ART for a = 11.
	//		1.2. Find + scan one ART for b = 24.
	//		1.3. Return the intersecting row IDs.
	// 2. (Reorder and) scan a single ART with a compound key of (a, b).
	if (filter_set.FilterCount() != 1) {
		return DuckTableScanInitGlobal(context, input, storage, bind_data);
	}

	auto &info = storage.GetDataTableInfo();
	auto &indexes = info->GetIndexes();
	if (indexes.Empty()) {
		return DuckTableScanInitGlobal(context, input, storage, bind_data);
	}

	auto scan_percentage = Settings::Get<IndexScanPercentageSetting>(context);
	auto scan_max_count = Settings::Get<IndexScanMaxCountSetting>(context);

	auto total_rows = storage.GetTotalRows();
	auto total_rows_from_percentage = LossyNumericCast<idx_t>(double(total_rows) * scan_percentage);
	auto max_count = MaxValue(scan_max_count, total_rows_from_percentage);

	auto &column_list = duck_table.GetColumns();
	bool index_scan = false;
	set<row_t> row_ids;

	// If vacuum_rebuild_indexes is enabled, grab a shared vacuum lock before
	// scanning the index. This prevents the checkpoint from rebuilding the index and swapping
	// row groups while we hold row IDs from the ART, ensuring we always see a consistent
	// <ART index, SegmentTree<RowGroup> pairing.
	unique_ptr<StorageLockKey> vacuum_lock;
	auto &db = DatabaseInstance::GetDatabase(context);
	if (Settings::Get<VacuumRebuildIndexesSetting>(db) > 0) {
		auto &transaction_manager = DuckTransactionManager::Get(storage.GetAttached());
		vacuum_lock = transaction_manager.SharedVacuumLock();
	}

	info->BindIndexes(context, ART::TYPE_NAME);
	for (auto &entry : indexes.IndexEntries()) {
		auto &index = *entry.index;
		if (index.GetIndexType() != ART::TYPE_NAME) {
			continue;
		}
		D_ASSERT(index.IsBound());
		auto &art = index.Cast<ART>();
		index_scan = TryScanIndex(art, entry, column_list, input, filter_set, max_count, row_ids);
		if (index_scan) {
			// found an index - break
			break;
		}
	}

	if (!index_scan) {
		return DuckTableScanInitGlobal(context, input, storage, bind_data);
	}
	return DuckIndexScanInitGlobal(context, input, bind_data, row_ids, std::move(vacuum_lock));
}

static unique_ptr<BaseStatistics> TableScanStatistics(ClientContext &context, TableFunctionGetStatisticsInput &input) {
	auto &column_id = input.column_index;
	auto &bind_data = input.bind_data->Cast<TableScanBindData>();
	auto &duck_table = bind_data.table.Cast<DuckTableEntry>();
	auto &local_storage = LocalStorage::Get(context, duck_table.catalog);

	// Don't emit statistics for tables with outstanding transaction-local data.
	if (local_storage.Find(duck_table.GetStorage())) {
		return nullptr;
	}

	if (column_id.IsRowIdColumn() || column_id.IsRowNumberColumn()) {
		return nullptr;
	}
	auto &column = duck_table.GetColumn(LogicalIndex(column_id.GetPrimaryIndex()));
	if (column.Generated()) {
		return nullptr;
	}

	auto storage_index = duck_table.GetStorageIndex(column_id);
	return duck_table.GetStatistics(context, storage_index);
}

static void TableScanFunc(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &g_state = data_p.global_state->Cast<TableScanGlobalState>();
	g_state.TableScanFunc(context, data_p, output);
}

double TableScanProgress(ClientContext &context, const FunctionData *bind_data_p,
                         const GlobalTableFunctionState *g_state_p) {
	auto &g_state = g_state_p->Cast<TableScanGlobalState>();
	return g_state.TableScanProgress(context, bind_data_p);
}

OperatorPartitionData TableScanGetPartitionData(ClientContext &context, TableFunctionGetPartitionInput &input) {
	if (input.partition_info.RequiresPartitionColumns()) {
		throw InternalException("TableScan::GetPartitionData: partition columns not supported");
	}

	auto &g_state = input.global_state->Cast<TableScanGlobalState>();
	return g_state.TableScanGetPartitionData(context, input);
}

vector<PartitionStatistics> TableScanGetPartitionStats(ClientContext &context, GetPartitionStatsInput &input) {
	auto &bind_data = input.bind_data->Cast<TableScanBindData>();
	auto &duck_table = bind_data.table.Cast<DuckTableEntry>();
	auto &storage = duck_table.GetStorage();
	return storage.GetPartitionStats(context);
}

BindInfo TableScanGetBindInfo(const optional_ptr<FunctionData> bind_data_p) {
	auto &bind_data = bind_data_p->Cast<TableScanBindData>();
	return BindInfo(bind_data.table);
}

void TableScanDependency(LogicalDependencyList &entries, const FunctionData *bind_data_p) {
	auto &bind_data = bind_data_p->Cast<TableScanBindData>();
	entries.AddDependency(bind_data.table);
}

unique_ptr<NodeStatistics> TableScanCardinality(ClientContext &context, const FunctionData *bind_data_p) {
	auto &bind_data = bind_data_p->Cast<TableScanBindData>();
	auto &duck_table = bind_data.table.Cast<DuckTableEntry>();
	auto &local_storage = LocalStorage::Get(context, duck_table.catalog);
	auto &storage = duck_table.GetStorage();
	idx_t table_rows = storage.GetTotalRows();
	idx_t estimated_cardinality = table_rows + local_storage.AddedRows(duck_table.GetStorage());
	return make_uniq<NodeStatistics>(estimated_cardinality, estimated_cardinality);
}

idx_t TableScanRowsScanned(GlobalTableFunctionState &gstate_p, LocalTableFunctionState &local_state) {
	auto &gstate = gstate_p.Cast<TableScanGlobalState>();
	return gstate.TableScanRowsScanned(local_state);
}

InsertionOrderPreservingMap<string> TableScanToString(TableFunctionToStringInput &input) {
	InsertionOrderPreservingMap<string> result;
	auto &bind_data = input.bind_data->Cast<TableScanBindData>();
	result["Table"] = ParseInfo::QualifierToString(bind_data.table.schema.catalog.GetName(),
	                                               bind_data.table.schema.name, bind_data.table.name);
	result["Type"] = bind_data.is_index_scan ? "Index Scan" : "Sequential Scan";
	return result;
}

static void TableScanSerialize(Serializer &serializer, const optional_ptr<FunctionData> bind_data_p,
                               const TableFunction &function) {
	auto &bind_data = bind_data_p->Cast<TableScanBindData>();
	serializer.WriteProperty(100, "catalog", bind_data.table.schema.catalog.GetName());
	serializer.WriteProperty(101, "schema", bind_data.table.schema.name);
	serializer.WriteProperty(102, "table", bind_data.table.name);
	serializer.WriteProperty(103, "is_index_scan", bind_data.is_index_scan);
	serializer.WriteProperty(104, "is_create_index", bind_data.is_create_index);
	serializer.WritePropertyWithDefault(105, "result_ids", unsafe_vector<row_t>());
}

static unique_ptr<FunctionData> TableScanDeserialize(Deserializer &deserializer, TableFunction &function) {
	auto catalog = deserializer.ReadProperty<string>(100, "catalog");
	auto schema = deserializer.ReadProperty<string>(101, "schema");
	auto table = deserializer.ReadProperty<string>(102, "table");
	auto &catalog_entry =
	    Catalog::GetEntry<TableCatalogEntry>(deserializer.Get<ClientContext &>(), catalog, schema, table);
	if (catalog_entry.type != CatalogType::TABLE_ENTRY) {
		throw SerializationException("Cant find table for %s.%s", schema, table);
	}
	auto result = make_uniq<TableScanBindData>(catalog_entry.Cast<DuckTableEntry>());
	deserializer.ReadProperty(103, "is_index_scan", result->is_index_scan);
	deserializer.ReadProperty(104, "is_create_index", result->is_create_index);
	deserializer.ReadDeletedProperty<unsafe_vector<row_t>>(105, "result_ids");
	return std::move(result);
}

static bool TableSupportsPushdownExtract(const FunctionData &bind_data_ref, const LogicalIndex &column_idx) {
	auto &bind_data = bind_data_ref.Cast<TableScanBindData>();
	auto &column = bind_data.table.GetColumn(column_idx);
	if (column.Generated()) {
		return false;
	}
	auto column_type = column.GetType();
	if (column_type.id() != LogicalTypeId::STRUCT && column_type.id() != LogicalTypeId::VARIANT) {
		return false;
	}
	return true;
}

bool TableScanPushdownExpression(ClientContext &context, const LogicalGet &get, Expression &expr) {
	return true;
}

virtual_column_map_t TableScanGetVirtualColumns(ClientContext &context, optional_ptr<FunctionData> bind_data_p) {
	auto &bind_data = bind_data_p->Cast<TableScanBindData>();
	return bind_data.table.GetVirtualColumns();
}

vector<column_t> TableScanGetRowIdColumns(ClientContext &context, optional_ptr<FunctionData> bind_data) {
	vector<column_t> result;
	result.emplace_back(COLUMN_IDENTIFIER_ROW_ID);
	return result;
}

void SetScanOrder(unique_ptr<RowGroupOrderOptions> order_options, optional_ptr<FunctionData> bind_data_p) {
	auto &bind_data = bind_data_p->Cast<TableScanBindData>();
	bind_data.order_options = std::move(order_options);
}

TableFunction TableScanFunction::GetFunction() {
	TableFunction scan_function("seq_scan", {}, TableScanFunc);
	scan_function.init_local = TableScanInitLocal;
	scan_function.init_global = TableScanInitGlobal;
	scan_function.statistics_extended = TableScanStatistics;
	scan_function.dependency = TableScanDependency;
	scan_function.cardinality = TableScanCardinality;
	scan_function.rows_scanned = TableScanRowsScanned;
	scan_function.pushdown_complex_filter = nullptr;
	scan_function.to_string = TableScanToString;
	scan_function.table_scan_progress = TableScanProgress;
	scan_function.get_partition_data = TableScanGetPartitionData;
	scan_function.get_partition_stats = TableScanGetPartitionStats;
	scan_function.get_bind_info = TableScanGetBindInfo;
	scan_function.projection_pushdown = true;
	scan_function.filter_pushdown = true;
	scan_function.filter_prune = true;
	scan_function.sampling_pushdown = true;
	scan_function.late_materialization = true;
	scan_function.serialize = TableScanSerialize;
	scan_function.deserialize = TableScanDeserialize;
	scan_function.pushdown_expression = TableScanPushdownExpression;
	scan_function.get_virtual_columns = TableScanGetVirtualColumns;
	scan_function.get_row_id_columns = TableScanGetRowIdColumns;
	scan_function.set_scan_order = SetScanOrder;
	scan_function.supports_pushdown_extract = TableSupportsPushdownExtract;
	return scan_function;
}

void TableScanFunction::RegisterFunction(BuiltinFunctions &set) {
	TableFunctionSet table_scan_set("seq_scan");
	table_scan_set.AddFunction(GetFunction());
	set.AddFunction(std::move(table_scan_set));
}

void BuiltinFunctions::RegisterTableScanFunctions() {
	TableScanFunction::RegisterFunction(*this);
}

} // namespace duckdb
