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
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/storage/data_table.hpp"
#include "duckdb/storage/table/scan_state.hpp"
#include "duckdb/transaction/duck_transaction.hpp"
#include "duckdb/transaction/local_storage.hpp"
#include "duckdb/storage/storage_index.hpp"
#include "duckdb/main/client_data.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// Table Scan
//===--------------------------------------------------------------------===//
bool TableScanParallelStateNext(ClientContext &context, const FunctionData *bind_data_p,
                                LocalTableFunctionState *local_state, GlobalTableFunctionState *gstate);

struct TableScanLocalState : public LocalTableFunctionState {
	//! The current position in the scan
	TableScanState scan_state;
	//! The DataChunk containing all read columns (even filter columns that are immediately removed)
	DataChunk all_columns;
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
	// the index of the base ColumnIndex is equal to the physical column index in the table
	// for any child indices - the indices are already the physical indices
	// (since only the top-level can have generated columns)
	auto &col = table.GetColumn(column_id.ToLogical());
	auto result = TransformStorageIndex(column_id);
	result.SetIndex(col.StorageOid());
	return result;
}

struct TableScanGlobalState : public GlobalTableFunctionState {
	TableScanGlobalState(ClientContext &context, const FunctionData *bind_data_p)
	    : row_ids_offset(0), index_scan(false) {
		D_ASSERT(bind_data_p);
		auto &bind_data = bind_data_p->Cast<TableScanBindData>();
		max_threads = bind_data.table.GetStorage().MaxThreads(context);
	}

	// Scan the table.
	ParallelTableScanState state;
	idx_t max_threads;
	vector<idx_t> projection_ids;
	vector<LogicalType> scanned_types;

	// Probe the table (index scan).
	vector<row_t> row_ids;
	unique_ptr<Vector> row_id_vector;
	idx_t row_ids_offset;
	ColumnFetchState fetch_state;
	TableScanState local_storage_state;
	vector<StorageIndex> column_ids;
	bool finished;
	bool index_scan;

	idx_t MaxThreads() const override {
		return max_threads;
	}

	bool CanRemoveFilterColumns() const {
		return !projection_ids.empty();
	}
};

static unique_ptr<LocalTableFunctionState> TableScanInitLocal(ExecutionContext &context, TableFunctionInitInput &input,
                                                              GlobalTableFunctionState *gstate) {
	auto result = make_uniq<TableScanLocalState>();
	auto &bind_data = input.bind_data->Cast<TableScanBindData>();
	vector<StorageIndex> storage_ids;
	for (auto &col : input.column_indexes) {
		storage_ids.push_back(GetStorageIndex(bind_data.table, col));
	}
	result->scan_state.Initialize(std::move(storage_ids), input.filters.get(), input.sample_options.get());
	TableScanParallelStateNext(context.client, input.bind_data.get(), result.get(), gstate);
	if (input.CanRemoveFilterColumns()) {
		auto &tsgs = gstate->Cast<TableScanGlobalState>();
		result->all_columns.Initialize(context.client, tsgs.scanned_types);
	}

	result->scan_state.options.force_fetch_row = ClientConfig::GetConfig(context.client).force_fetch_row;

	return std::move(result);
}

void ScanIndex(ClientContext &context, const TableScanBindData &bind_data, TableScanGlobalState &g_state,
               optional_ptr<TableFilterSet> filter_set) {
	auto &table = bind_data.table;
	auto &storage = table.GetStorage();

	if (!filter_set) {
		return;
	}

	// TODO: do we still need this lock?
	// TODO: we probably need a way to allow multiple reads (and no writes) to an index.
	auto checkpoint_lock = storage.GetSharedCheckpointLock();
	auto &info = storage.GetDataTableInfo();
	auto &indexes = info->GetIndexes();
	if (indexes.Empty()) {
		return;
	}

	auto &db_config = DBConfig::GetConfig(context);
	auto scan_percentage = db_config.GetSetting<IndexScanPercentageSetting>(context);
	auto scan_max_count = db_config.GetSetting<IndexScanMaxCountSetting>(context);

	auto total_rows = storage.GetTotalRows();
	auto total_rows_from_percentage = LossyNumericCast<idx_t>(double(total_rows) * scan_percentage);
	auto max_count = MaxValue(scan_max_count, total_rows_from_percentage);

	auto &column_list = table.GetColumns();
	info->GetIndexes().BindAndScan<ART>(context, *info, [&](ART &art) {
		// NOTE: No support for index scans on compound indexes.
		if (art.unbound_expressions.size() > 1) {
			return false;
		}

		auto index_expr = art.unbound_expressions[0]->Copy();
		auto &indexed_columns = art.GetColumnIds();

		// NOTE: We do not push down multi-column filters, e.g., 42 = a + b.
		// FIXME: Push down optional multi-column filters for a potential index scan.
		if (indexed_columns.size() != 1) {
			return false;
		}

		auto &col = column_list.GetColumn(LogicalIndex(indexed_columns[0]));
		auto storage_index = col.Physical().index;

		// Try to find a matching filter for the column.
		auto filter = filter_set->filters.find(storage_index);
		if (filter == filter_set->filters.end()) {
			return false;
		}

		auto bound_ref = make_uniq<BoundReferenceExpression>(col.GetType(), storage_index);
		auto filter_expr = filter->second->ToExpression(*bound_ref);

		auto scan_state = art.TryInitializeScan(*index_expr, *filter_expr);
		if (!scan_state) {
			return false;
		}

		// Check if we can use an index scan, and already retrieve the matching row ids.
		unsafe_vector<row_t> row_ids;
		if (art.Scan(*scan_state, max_count, row_ids)) {
			g_state.index_scan = true;
			if (!g_state.row_ids.empty()) {
				auto row_id_data = (data_ptr_t)&g_state.row_ids[0]; // NOLINT - this is not pretty
				g_state.row_id_vector = make_uniq<Vector>(LogicalType::ROW_TYPE, row_id_data);
			}
			return true;
		}
		g_state.row_ids.clear();
		return false;
	});
}

unique_ptr<GlobalTableFunctionState> TableScanInitGlobal(ClientContext &context, TableFunctionInitInput &input) {
	D_ASSERT(input.bind_data);

	auto &bind_data = input.bind_data->Cast<TableScanBindData>();
	auto &storage = bind_data.table.GetStorage();
	auto g_state = make_uniq<TableScanGlobalState>(context, input.bind_data.get());

	ScanIndex(context, bind_data, *g_state, input.filters);
	if (g_state->index_scan) {
		auto &local_storage = LocalStorage::Get(context, bind_data.table.catalog);
		g_state->local_storage_state.options.force_fetch_row = ClientConfig::GetConfig(context).force_fetch_row;

		g_state->column_ids.reserve(input.column_ids.size());
		for (auto &col_id : input.column_indexes) {
			g_state->column_ids.push_back(GetStorageIndex(bind_data.table, col_id));
		}

		g_state->local_storage_state.Initialize(g_state->column_ids, input.filters.get());
		local_storage.InitializeScan(storage, g_state->local_storage_state.local_state, input.filters);

		g_state->finished = false;
		return std::move(g_state);
	}

	storage.InitializeParallelScan(context, g_state->state);
	if (!input.CanRemoveFilterColumns()) {
		return std::move(g_state);
	}

	g_state->projection_ids = input.projection_ids;
	const auto &columns = bind_data.table.GetColumns();
	for (const auto &col_idx : input.column_indexes) {
		if (col_idx.IsRowIdColumn()) {
			g_state->scanned_types.emplace_back(LogicalType::ROW_TYPE);
		} else {
			g_state->scanned_types.push_back(columns.GetColumn(col_idx.ToLogical()).Type());
		}
	}
	return std::move(g_state);
}

static unique_ptr<BaseStatistics> TableScanStatistics(ClientContext &context, const FunctionData *bind_data_p,
                                                      column_t column_id) {
	auto &bind_data = bind_data_p->Cast<TableScanBindData>();
	auto &local_storage = LocalStorage::Get(context, bind_data.table.catalog);
	if (local_storage.Find(bind_data.table.GetStorage())) {
		// we don't emit any statistics for tables that have outstanding transaction-local data
		return nullptr;
	}
	return bind_data.table.GetStatistics(context, column_id);
}

static void TableScanFunc(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &bind_data = data_p.bind_data->Cast<TableScanBindData>();
	auto &g_state = data_p.global_state->Cast<TableScanGlobalState>();
	auto &transaction = DuckTransaction::Get(context, bind_data.table.catalog);
	auto &storage = bind_data.table.GetStorage();

	if (g_state.index_scan) {
		auto &local_storage = LocalStorage::Get(transaction);

		if (g_state.row_ids.empty()) {
			g_state.finished = true;
		}

		if (!g_state.finished) {
			// TODO: lock here?
			auto row_id_count = g_state.row_ids.size();
			auto remaining = row_id_count - g_state.row_ids_offset;
			auto scan_count = remaining < STANDARD_VECTOR_SIZE ? remaining : STANDARD_VECTOR_SIZE;

			D_ASSERT(g_state.row_id_vector);
			Vector row_ids(*g_state.row_id_vector, g_state.row_ids_offset, g_state.row_ids_offset + scan_count);
			storage.Fetch(transaction, output, g_state.column_ids, row_ids, scan_count, g_state.fetch_state);

			g_state.row_ids_offset += scan_count;
			if (g_state.row_ids_offset == row_id_count) {
				g_state.finished = true;
			}
		}
		if (output.size() == 0) {
			local_storage.Scan(g_state.local_storage_state.local_state, g_state.column_ids, output);
		}
		return;
	}

	auto &state = data_p.local_state->Cast<TableScanLocalState>();
	state.scan_state.options.force_fetch_row = ClientConfig::GetConfig(context).force_fetch_row;
	do {
		if (bind_data.is_create_index) {
			storage.CreateIndexScan(state.scan_state, output,
			                        TableScanType::TABLE_SCAN_COMMITTED_ROWS_OMIT_PERMANENTLY_DELETED);
		} else if (g_state.CanRemoveFilterColumns()) {
			state.all_columns.Reset();
			storage.Scan(transaction, state.all_columns, state.scan_state);
			output.ReferenceColumns(state.all_columns, g_state.projection_ids);
		} else {
			storage.Scan(transaction, output, state.scan_state);
		}
		if (output.size() > 0) {
			return;
		}
		if (!TableScanParallelStateNext(context, data_p.bind_data.get(), data_p.local_state.get(),
		                                data_p.global_state.get())) {
			return;
		}
	} while (true);
}

bool TableScanParallelStateNext(ClientContext &context, const FunctionData *bind_data_p,
                                LocalTableFunctionState *local_state, GlobalTableFunctionState *global_state) {
	auto &bind_data = bind_data_p->Cast<TableScanBindData>();
	auto &parallel_state = global_state->Cast<TableScanGlobalState>();
	auto &state = local_state->Cast<TableScanLocalState>();
	auto &storage = bind_data.table.GetStorage();

	return storage.NextParallelScan(context, parallel_state.state, state.scan_state);
}

double TableScanProgress(ClientContext &context, const FunctionData *bind_data_p,
                         const GlobalTableFunctionState *gstate_p) {
	auto &bind_data = bind_data_p->Cast<TableScanBindData>();
	auto &gstate = gstate_p->Cast<TableScanGlobalState>();
	auto &storage = bind_data.table.GetStorage();
	idx_t total_rows = storage.GetTotalRows();
	if (total_rows == 0) {
		//! Table is either empty or smaller than a vector size, so it is finished
		return 100;
	}
	idx_t scanned_rows = gstate.state.scan_state.processed_rows;
	scanned_rows += gstate.state.local_state.processed_rows;
	auto percentage = 100 * (static_cast<double>(scanned_rows) / static_cast<double>(total_rows));
	if (percentage > 100) {
		//! In case the last chunk has less elements than STANDARD_VECTOR_SIZE, if our percentage is over 100
		//! It means we finished this table.
		return 100;
	}
	return percentage;
}

OperatorPartitionData TableScanGetPartitionData(ClientContext &context, TableFunctionGetPartitionInput &input) {
	if (input.partition_info.RequiresPartitionColumns()) {
		throw InternalException("TableScan::GetPartitionData: partition columns not supported");
	}
	auto &state = input.local_state->Cast<TableScanLocalState>();
	if (state.scan_state.table_state.row_group) {
		return OperatorPartitionData(state.scan_state.table_state.batch_index);
	}
	if (state.scan_state.local_state.row_group) {
		return OperatorPartitionData(state.scan_state.table_state.batch_index +
		                             state.scan_state.local_state.batch_index);
	}
	return OperatorPartitionData(0);
}

vector<PartitionStatistics> TableScanGetPartitionStats(ClientContext &context, GetPartitionStatsInput &input) {
	auto &bind_data = input.bind_data->Cast<TableScanBindData>();
	vector<PartitionStatistics> result;
	auto &storage = bind_data.table.GetStorage();
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
	auto &local_storage = LocalStorage::Get(context, bind_data.table.catalog);
	auto &storage = bind_data.table.GetStorage();
	idx_t table_rows = storage.GetTotalRows();
	idx_t estimated_cardinality = table_rows + local_storage.AddedRows(bind_data.table.GetStorage());
	return make_uniq<NodeStatistics>(table_rows, estimated_cardinality);
}

InsertionOrderPreservingMap<string> TableScanToString(TableFunctionToStringInput &input) {
	InsertionOrderPreservingMap<string> result;
	auto &bind_data = input.bind_data->Cast<TableScanBindData>();
	result["Table"] = bind_data.table.name;
	return result;
}

static void TableScanSerialize(Serializer &serializer, const optional_ptr<FunctionData> bind_data_p,
                               const TableFunction &function) {
	auto &bind_data = bind_data_p->Cast<TableScanBindData>();
	serializer.WriteProperty(100, "catalog", bind_data.table.schema.catalog.GetName());
	serializer.WriteProperty(101, "schema", bind_data.table.schema.name);
	serializer.WriteProperty(102, "table", bind_data.table.name);
	serializer.WritePropertyWithDefault(103, "is_index_scan", false);
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
	deserializer.ReadDeletedProperty<bool>(103, "is_index_scan");
	deserializer.ReadProperty(104, "is_create_index", result->is_create_index);
	deserializer.ReadDeletedProperty<unsafe_vector<row_t>>(105, "result_ids");
	return std::move(result);
}

TableFunction TableScanFunction::GetFunction() {
	TableFunction scan_function("seq_scan", {}, TableScanFunc);
	scan_function.init_local = TableScanInitLocal; // TODO: nullptr for index scans
	scan_function.init_global = TableScanInitGlobal;
	scan_function.statistics = TableScanStatistics;
	scan_function.dependency = TableScanDependency;
	scan_function.cardinality = TableScanCardinality;
	scan_function.pushdown_complex_filter = nullptr;
	scan_function.to_string = TableScanToString;
	scan_function.table_scan_progress = TableScanProgress;          // TODO: nullptr for index scans
	scan_function.get_partition_data = TableScanGetPartitionData;   // TODO: nullptr for index scans
	scan_function.get_partition_stats = TableScanGetPartitionStats; // TODO: nullptr for index scans
	scan_function.get_bind_info = TableScanGetBindInfo;
	scan_function.projection_pushdown = true;
	scan_function.filter_pushdown = true; // TODO: false for index scans
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
