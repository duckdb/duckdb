#include "duckdb/function/table/table_scan.hpp"

#include "duckdb/catalog/catalog_entry/duck_table_entry.hpp"
#include "duckdb/catalog/dependency_list.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/common/serializer/deserializer.hpp"
#include "duckdb/common/serializer/serializer.hpp"
#include "duckdb/execution/index/art/art.hpp"
#include "duckdb/function/function_set.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/client_config.hpp"
#include "duckdb/optimizer/matcher/expression_matcher.hpp"
#include "duckdb/planner/expression/bound_between_expression.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/storage/data_table.hpp"
#include "duckdb/storage/table/scan_state.hpp"
#include "duckdb/transaction/duck_transaction.hpp"
#include "duckdb/transaction/local_storage.hpp"
#include "duckdb/storage/storage_index.hpp"
#include "duckdb/main/client_data.hpp"
#include "duckdb/common/algorithm.hpp"
#include "duckdb/planner/filter/optional_filter.hpp"
#include "duckdb/planner/filter/in_filter.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/filter/conjunction_filter.hpp"
#include "duckdb/common/types/value_map.hpp"

namespace duckdb {

struct TableScanLocalState : public LocalTableFunctionState {
	//! The current position in the scan.
	TableScanState scan_state;
	//! The DataChunk containing all read columns.
	//! This includes filter columns, which are immediately removed.
	DataChunk all_columns;
};

struct IndexScanLocalState : public LocalTableFunctionState {
	//! The batch index, which determines the offset in the row ID vector.
	idx_t batch_index;
	//! The DataChunk containing all read columns.
	//! This includes filter columns, which are immediately removed.
	DataChunk all_columns;
	//! Fetch state
	ColumnFetchState fetch_state;
};

static StorageIndex TransformStorageIndex(const ColumnIndex &column_id) {
	vector<StorageIndex> result;
	for (auto &child_id : column_id.GetChildIndexes()) {
		result.push_back(TransformStorageIndex(child_id));
	}
	return StorageIndex(column_id.GetPrimaryIndex(), std::move(result));
}

static StorageIndex GetStorageIndex(TableCatalogEntry &table, const ColumnIndex &column_id) {
	if (column_id.IsRowIdColumn()) {
		return StorageIndex();
	}

	// The index of the base ColumnIndex is equal to the physical column index in the table
	// for any child indices because the indices are already the physical indices.
	// Only the top-level can have generated columns.
	auto &col = table.GetColumn(column_id.ToLogical());
	auto result = TransformStorageIndex(column_id);
	result.SetIndex(col.StorageOid());
	return result;
}

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
	    : TableScanGlobalState(context, bind_data_p), next_batch_index(0), finished(false) {
	}

	//! The batch index of the next Sink.
	//! Also determines the offset of the next chunk. I.e., offset = next_batch_index * STANDARD_VECTOR_SIZE.
	atomic<idx_t> next_batch_index;
	//! The total scanned row IDs.
	unsafe_vector<row_t> row_ids;
	//! The column IDs of the to-be-scanned columns.
	vector<StorageIndex> column_ids;
	//! True, if no more row IDs must be scanned.
	bool finished;
	//! Synchronize changes to the global index scan state.
	mutex index_scan_lock;

	TableScanState table_scan_state;

public:
	unique_ptr<LocalTableFunctionState> InitLocalState(ExecutionContext &context,
	                                                   TableFunctionInitInput &input) override {
		auto l_state = make_uniq<IndexScanLocalState>();
		if (input.CanRemoveFilterColumns()) {
			l_state->all_columns.Initialize(context.client, scanned_types);
		}
		return std::move(l_state);
	}

	void TableScanFunc(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) override {
		auto &bind_data = data_p.bind_data->Cast<TableScanBindData>();
		auto &duck_table = bind_data.table.Cast<DuckTableEntry>();
		auto &tx = DuckTransaction::Get(context, duck_table.catalog);
		auto &storage = duck_table.GetStorage();
		auto &l_state = data_p.local_state->Cast<IndexScanLocalState>();

		auto row_id_count = row_ids.size();
		idx_t scan_count = 0;
		idx_t offset = 0;

		{
			// Synchronize changes to the shared global state.
			lock_guard<mutex> l(index_scan_lock);
			if (!finished) {
				l_state.batch_index = next_batch_index;
				next_batch_index++;

				offset = l_state.batch_index * STANDARD_VECTOR_SIZE;
				auto remaining = row_id_count - offset;
				scan_count = remaining < STANDARD_VECTOR_SIZE ? remaining : STANDARD_VECTOR_SIZE;
				finished = remaining < STANDARD_VECTOR_SIZE ? true : false;
			}
		}

		if (scan_count != 0) {
			auto row_id_data = (data_ptr_t)&row_ids[0 + offset]; // NOLINT - this is not pretty
			Vector local_vector(LogicalType::ROW_TYPE, row_id_data);

			if (CanRemoveFilterColumns()) {
				l_state.all_columns.Reset();
				storage.Fetch(tx, l_state.all_columns, column_ids, local_vector, scan_count, l_state.fetch_state);
				output.ReferenceColumns(l_state.all_columns, projection_ids);
			} else {
				storage.Fetch(tx, output, column_ids, local_vector, scan_count, l_state.fetch_state);
			}
		}

		if (output.size() == 0) {
			auto &local_storage = LocalStorage::Get(tx);
			if (CanRemoveFilterColumns()) {
				l_state.all_columns.Reset();
				local_storage.Scan(table_scan_state.local_state, column_ids, l_state.all_columns);
				output.ReferenceColumns(l_state.all_columns, projection_ids);
			} else {
				local_storage.Scan(table_scan_state.local_state, column_ids, output);
			}
		}
	}

	double TableScanProgress(ClientContext &context, const FunctionData *bind_data_p) const override {
		auto total_rows = row_ids.size();
		if (total_rows == 0) {
			return 100;
		}

		auto scanned_rows = next_batch_index * STANDARD_VECTOR_SIZE;
		auto percentage = 100 * (static_cast<double>(scanned_rows) / static_cast<double>(total_rows));
		return percentage > 100 ? 100 : percentage;
	}

	OperatorPartitionData TableScanGetPartitionData(ClientContext &context,
	                                                TableFunctionGetPartitionInput &input) override {
		auto &l_state = input.local_state->Cast<IndexScanLocalState>();
		return OperatorPartitionData(l_state.batch_index);
	}
};

class DuckTableScanState : public TableScanGlobalState {
public:
	DuckTableScanState(ClientContext &context, const FunctionData *bind_data_p)
	    : TableScanGlobalState(context, bind_data_p) {
	}

	ParallelTableScanState state;

public:
	unique_ptr<LocalTableFunctionState> InitLocalState(ExecutionContext &context,
	                                                   TableFunctionInitInput &input) override {
		auto &bind_data = input.bind_data->Cast<TableScanBindData>();
		auto l_state = make_uniq<TableScanLocalState>();

		vector<StorageIndex> storage_ids;
		for (auto &col : input.column_indexes) {
			storage_ids.push_back(GetStorageIndex(bind_data.table, col));
		}

		l_state->scan_state.Initialize(std::move(storage_ids), input.filters.get(), input.sample_options.get());

		auto &duck_table = bind_data.table.Cast<DuckTableEntry>();
		auto &storage = duck_table.GetStorage();
		storage.NextParallelScan(context.client, state, l_state->scan_state);
		if (input.CanRemoveFilterColumns()) {
			l_state->all_columns.Initialize(context.client, scanned_types);
		}

		l_state->scan_state.options.force_fetch_row = ClientConfig::GetConfig(context.client).force_fetch_row;
		return std::move(l_state);
	}

	void TableScanFunc(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) override {
		auto &bind_data = data_p.bind_data->Cast<TableScanBindData>();
		auto &duck_table = bind_data.table.Cast<DuckTableEntry>();
		auto &tx = DuckTransaction::Get(context, duck_table.catalog);
		auto &storage = duck_table.GetStorage();

		auto &l_state = data_p.local_state->Cast<TableScanLocalState>();
		l_state.scan_state.options.force_fetch_row = ClientConfig::GetConfig(context).force_fetch_row;

		do {
			if (bind_data.is_create_index) {
				storage.CreateIndexScan(l_state.scan_state, output,
				                        TableScanType::TABLE_SCAN_COMMITTED_ROWS_OMIT_PERMANENTLY_DELETED);
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

			auto next = storage.NextParallelScan(context, state, l_state.scan_state);
			if (!next) {
				return;
			}
		} while (true);
	}

	double TableScanProgress(ClientContext &context, const FunctionData *bind_data_p) const override {
		auto &bind_data = bind_data_p->Cast<TableScanBindData>();
		auto &duck_table = bind_data.table.Cast<DuckTableEntry>();
		auto &storage = duck_table.GetStorage();
		auto total_rows = storage.GetTotalRows();

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
};

static unique_ptr<LocalTableFunctionState> TableScanInitLocal(ExecutionContext &context, TableFunctionInitInput &input,
                                                              GlobalTableFunctionState *g_state) {
	auto &cast_g_state = g_state->Cast<TableScanGlobalState>();
	return cast_g_state.InitLocalState(context, input);
}

unique_ptr<GlobalTableFunctionState> DuckTableScanInitGlobal(ClientContext &context, TableFunctionInitInput &input,
                                                             DataTable &storage, const TableScanBindData &bind_data) {
	auto g_state = make_uniq<DuckTableScanState>(context, input.bind_data.get());
	storage.InitializeParallelScan(context, g_state->state);
	if (!input.CanRemoveFilterColumns()) {
		return std::move(g_state);
	}

	g_state->projection_ids = input.projection_ids;
	auto &duck_table = bind_data.table.Cast<DuckTableEntry>();
	const auto &columns = duck_table.GetColumns();
	for (const auto &col_idx : input.column_indexes) {
		if (col_idx.IsRowIdColumn()) {
			g_state->scanned_types.emplace_back(LogicalType::ROW_TYPE);
		} else {
			g_state->scanned_types.push_back(columns.GetColumn(col_idx.ToLogical()).Type());
		}
	}
	return std::move(g_state);
}

unique_ptr<GlobalTableFunctionState> DuckIndexScanInitGlobal(ClientContext &context, TableFunctionInitInput &input,
                                                             DataTable &storage, const TableScanBindData &bind_data,
                                                             unsafe_vector<row_t> &row_ids) {
	auto g_state = make_uniq<DuckIndexScanState>(context, input.bind_data.get());
	if (!row_ids.empty()) {
		std::sort(row_ids.begin(), row_ids.end());
		g_state->row_ids = std::move(row_ids);
	}
	g_state->finished = g_state->row_ids.empty() ? true : false;

	auto &duck_table = bind_data.table.Cast<DuckTableEntry>();
	auto &local_storage = LocalStorage::Get(context, duck_table.catalog);
	g_state->table_scan_state.options.force_fetch_row = ClientConfig::GetConfig(context).force_fetch_row;

	if (input.CanRemoveFilterColumns()) {
		g_state->projection_ids = input.projection_ids;
	}

	const auto &columns = duck_table.GetColumns();
	for (const auto &col_idx : input.column_indexes) {
		g_state->column_ids.push_back(GetStorageIndex(bind_data.table, col_idx));
		if (col_idx.IsRowIdColumn()) {
			g_state->scanned_types.emplace_back(LogicalType::ROW_TYPE);
			continue;
		}
		g_state->scanned_types.push_back(columns.GetColumn(col_idx.ToLogical()).Type());
	}

	g_state->table_scan_state.Initialize(g_state->column_ids, input.filters.get());
	local_storage.InitializeScan(storage, g_state->table_scan_state.local_state, input.filters);

	// Const-cast to indicate an index scan.
	// We need this information in the bind data so that we can access it during ANALYZE.
	auto &no_const_bind_data = bind_data.CastNoConst<TableScanBindData>();
	no_const_bind_data.is_index_scan = true;

	return std::move(g_state);
}

void ExtractExpressionsFromValues(value_set_t &unique_values, BoundColumnRefExpression &bound_ref,
                                  vector<unique_ptr<Expression>> &expressions) {
	for (const auto &value : unique_values) {
		auto bound_constant = make_uniq<BoundConstantExpression>(value);
		auto filter_expr = make_uniq<BoundComparisonExpression>(ExpressionType::COMPARE_EQUAL, bound_ref.Copy(),
		                                                        std::move(bound_constant));
		expressions.push_back(std::move(filter_expr));
	}
}

void ExtractIn(InFilter &filter, BoundColumnRefExpression &bound_ref, vector<unique_ptr<Expression>> &expressions) {
	// Eliminate any duplicates.
	value_set_t unique_values;
	for (const auto &value : filter.values) {
		if (unique_values.find(value) == unique_values.end()) {
			unique_values.insert(value);
		}
	}
	ExtractExpressionsFromValues(unique_values, bound_ref, expressions);
}

void ExtractConjunctionAnd(ConjunctionAndFilter &filter, BoundColumnRefExpression &bound_ref,
                           vector<unique_ptr<Expression>> &expressions) {
	if (filter.child_filters.empty()) {
		return;
	}

	// Extract the CONSTANT_COMPARISON and IN_FILTER children.
	vector<reference<ConstantFilter>> comparisons;
	vector<reference<InFilter>> in_filters;

	for (idx_t i = 0; i < filter.child_filters.size(); i++) {
		if (filter.child_filters[i]->filter_type == TableFilterType::CONSTANT_COMPARISON) {
			auto &comparison = filter.child_filters[i]->Cast<ConstantFilter>();
			comparisons.push_back(comparison);
			continue;
		}

		if (filter.child_filters[i]->filter_type == TableFilterType::OPTIONAL_FILTER) {
			auto &optional_filter = filter.child_filters[i]->Cast<OptionalFilter>();
			if (!optional_filter.child_filter) {
				return;
			}
			if (optional_filter.child_filter->filter_type != TableFilterType::IN_FILTER) {
				// No support for other optional filter types yet.
				return;
			}
			auto &in_filter = optional_filter.child_filter->Cast<InFilter>();
			in_filters.push_back(in_filter);
			continue;
		}

		// No support for other filter types than CONSTANT_COMPARISON and IN_FILTER in CONJUNCTION_AND yet.
		return;
	}

	// No support for other CONJUNCTION_AND cases yet.
	if (in_filters.empty()) {
		return;
	}

	// Get the combined unique values of the IN filters.
	value_set_t unique_values;
	for (idx_t filter_idx = 0; filter_idx < in_filters.size(); filter_idx++) {
		auto &in_filter = in_filters[filter_idx].get();
		for (idx_t value_idx = 0; value_idx < in_filter.values.size(); value_idx++) {
			auto &value = in_filter.values[value_idx];
			if (unique_values.find(value) != unique_values.end()) {
				continue;
			}
			unique_values.insert(value);
		}
	}

	// Extract all qualifying values.
	for (auto value_it = unique_values.begin(); value_it != unique_values.end();) {
		bool qualifies = true;
		for (idx_t comp_idx = 0; comp_idx < comparisons.size(); comp_idx++) {
			if (!comparisons[comp_idx].get().Compare(*value_it)) {
				qualifies = false;
				value_it = unique_values.erase(value_it);
				break;
			}
		}
		if (qualifies) {
			value_it++;
		}
	}

	ExtractExpressionsFromValues(unique_values, bound_ref, expressions);
}

void ExtractFilter(TableFilter &filter, BoundColumnRefExpression &bound_ref,
                   vector<unique_ptr<Expression>> &expressions) {
	switch (filter.filter_type) {
	case TableFilterType::OPTIONAL_FILTER: {
		auto &optional_filter = filter.Cast<OptionalFilter>();
		if (!optional_filter.child_filter) {
			return;
		}
		return ExtractFilter(*optional_filter.child_filter, bound_ref, expressions);
	}
	case TableFilterType::IN_FILTER: {
		auto &in_filter = filter.Cast<InFilter>();
		ExtractIn(in_filter, bound_ref, expressions);
		return;
	}
	case TableFilterType::CONJUNCTION_AND: {
		auto &conjunction_and = filter.Cast<ConjunctionAndFilter>();
		ExtractConjunctionAnd(conjunction_and, bound_ref, expressions);
		return;
	}
	default:
		return;
	}
}

vector<unique_ptr<Expression>> ExtractFilterExpressions(const ColumnDefinition &col, unique_ptr<TableFilter> &filter,
                                                        idx_t storage_idx) {
	ColumnBinding binding(0, storage_idx);
	auto bound_ref = make_uniq<BoundColumnRefExpression>(col.Name(), col.Type(), binding);

	vector<unique_ptr<Expression>> expressions;
	ExtractFilter(*filter, *bound_ref, expressions);

	// Attempt matching the top-level filter to the index expression.
	if (expressions.empty()) {
		auto filter_expr = filter->ToExpression(*bound_ref);
		expressions.push_back(std::move(filter_expr));
	}
	return expressions;
}

bool TryScanIndex(ART &art, const ColumnList &column_list, TableFunctionInitInput &input, TableFilterSet &filter_set,
                  idx_t max_count, unsafe_vector<row_t> &row_ids) {
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

	// Get ART column.
	auto &col = column_list.GetColumn(LogicalIndex(indexed_columns[0]));

	// The indexes of the filters match input.column_indexes, which are: i -> column_index.
	// Try to find a filter on the ART column.
	optional_idx storage_index;
	for (idx_t i = 0; i < input.column_indexes.size(); i++) {
		if (input.column_indexes[i].ToLogical() == col.Logical()) {
			storage_index = i;
			break;
		}
	}

	// No filter matches the ART column.
	if (!storage_index.IsValid()) {
		return false;
	}

	// Try to find a matching filter for the column.
	auto filter = filter_set.filters.find(storage_index.GetIndex());
	if (filter == filter_set.filters.end()) {
		return false;
	}

	auto expressions = ExtractFilterExpressions(col, filter->second, storage_index.GetIndex());
	for (const auto &filter_expr : expressions) {
		auto scan_state = art.TryInitializeScan(*index_expr, *filter_expr);
		if (!scan_state) {
			return false;
		}

		// Check if we can use an index scan, and already retrieve the matching row ids.
		if (!art.Scan(*scan_state, max_count, row_ids)) {
			row_ids.clear();
			return false;
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
	if (filter_set.filters.size() != 1) {
		return DuckTableScanInitGlobal(context, input, storage, bind_data);
	}

	// The checkpoint lock ensures that we do not checkpoint while scanning this table.
	auto &transaction = DuckTransaction::Get(context, storage.db);
	auto checkpoint_lock = transaction.SharedLockTable(*storage.GetDataTableInfo());
	auto &info = storage.GetDataTableInfo();
	auto &indexes = info->GetIndexes();
	if (indexes.Empty()) {
		return DuckTableScanInitGlobal(context, input, storage, bind_data);
	}

	auto &db_config = DBConfig::GetConfig(context);
	auto scan_percentage = db_config.GetSetting<IndexScanPercentageSetting>(context);
	auto scan_max_count = db_config.GetSetting<IndexScanMaxCountSetting>(context);

	auto total_rows = storage.GetTotalRows();
	auto total_rows_from_percentage = LossyNumericCast<idx_t>(double(total_rows) * scan_percentage);
	auto max_count = MaxValue(scan_max_count, total_rows_from_percentage);

	auto &column_list = duck_table.GetColumns();
	bool index_scan = false;
	unsafe_vector<row_t> row_ids;

	info->GetIndexes().BindAndScan<ART>(context, *info, [&](ART &art) {
		index_scan = TryScanIndex(art, column_list, input, filter_set, max_count, row_ids);
		return index_scan;
	});

	if (!index_scan) {
		return DuckTableScanInitGlobal(context, input, storage, bind_data);
	}
	return DuckIndexScanInitGlobal(context, input, storage, bind_data, row_ids);
}

static unique_ptr<BaseStatistics> TableScanStatistics(ClientContext &context, const FunctionData *bind_data_p,
                                                      column_t column_id) {
	auto &bind_data = bind_data_p->Cast<TableScanBindData>();
	auto &duck_table = bind_data.table.Cast<DuckTableEntry>();
	auto &local_storage = LocalStorage::Get(context, duck_table.catalog);

	// Don't emit statistics for tables with outstanding transaction-local data.
	if (local_storage.Find(duck_table.GetStorage())) {
		return nullptr;
	}
	return duck_table.GetStatistics(context, column_id);
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
	vector<PartitionStatistics> result;
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
	return make_uniq<NodeStatistics>(table_rows, estimated_cardinality);
}

InsertionOrderPreservingMap<string> TableScanToString(TableFunctionToStringInput &input) {
	InsertionOrderPreservingMap<string> result;
	auto &bind_data = input.bind_data->Cast<TableScanBindData>();
	result["Table"] = bind_data.table.name;
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

TableFunction TableScanFunction::GetFunction() {
	TableFunction scan_function("seq_scan", {}, TableScanFunc);
	scan_function.init_local = TableScanInitLocal;
	scan_function.init_global = TableScanInitGlobal;
	scan_function.statistics = TableScanStatistics;
	scan_function.dependency = TableScanDependency;
	scan_function.cardinality = TableScanCardinality;
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
	scan_function.serialize = TableScanSerialize;
	scan_function.deserialize = TableScanDeserialize;
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
