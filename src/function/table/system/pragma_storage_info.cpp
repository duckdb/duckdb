#include "duckdb/function/table/system_functions.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/view_catalog_entry.hpp"
#include "duckdb/parser/qualified_name.hpp"
#include "duckdb/planner/constraints/bound_not_null_constraint.hpp"
#include "duckdb/planner/constraints/bound_unique_constraint.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/limits.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/execution/partition_info.hpp"
#include "duckdb/storage/data_table.hpp"
#include "duckdb/storage/table/row_group_collection.hpp"
#include "duckdb/storage/table_storage_info.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/storage/table/column_data.hpp"

#include <algorithm>

namespace duckdb {

struct PragmaStorageFunctionData : public TableFunctionData {
	PragmaStorageFunctionData(TableCatalogEntry &table_entry, ColumnSegmentInfoScanState scan_state_p)
	    : table_entry(table_entry), scan_state(std::move(scan_state_p)) {
	}

	TableCatalogEntry &table_entry;
	//! Pinned row-group snapshot taken at bind time. Mutable because the cursor advances during
	//! execution; concurrent access is serialized via PragmaStorageGlobalState::lock.
	mutable ColumnSegmentInfoScanState scan_state;
};

struct PragmaStorageGlobalState : public GlobalTableFunctionState {
	//! Protects access to the shared scan state in bind data while local threads pull row groups.
	mutex lock;

	idx_t MaxThreads() const override {
		return MAX_THREADS;
	}
};

struct PragmaStorageLocalState : public LocalTableFunctionState {
	//! Buffered column segment info for the row group this thread is currently emitting.
	vector<ColumnSegmentInfo> buffer;
	idx_t buffer_offset = 0;
	//! Index of the row group currently being emitted; used as the batch index for partitioning.
	idx_t batch_index = 0;
};

static unique_ptr<FunctionData> PragmaStorageInfoBind(ClientContext &context, TableFunctionBindInput &input,
                                                      vector<LogicalType> &return_types, vector<string> &names) {
	names.emplace_back("row_group_id");
	return_types.emplace_back(LogicalType::BIGINT);

	names.emplace_back("column_name");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("column_id");
	return_types.emplace_back(LogicalType::BIGINT);

	names.emplace_back("column_path");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("segment_id");
	return_types.emplace_back(LogicalType::BIGINT);

	names.emplace_back("segment_type");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("start");
	return_types.emplace_back(LogicalType::BIGINT);

	names.emplace_back("count");
	return_types.emplace_back(LogicalType::BIGINT);

	names.emplace_back("compression");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("stats");
	return_types.emplace_back(LogicalType::VARIANT());

	names.emplace_back("has_updates");
	return_types.emplace_back(LogicalType::BOOLEAN);

	names.emplace_back("persistent");
	return_types.emplace_back(LogicalType::BOOLEAN);

	names.emplace_back("block_id");
	return_types.emplace_back(LogicalType::BIGINT);

	names.emplace_back("block_offset");
	return_types.emplace_back(LogicalType::BIGINT);

	names.emplace_back("segment_info");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("additional_block_ids");
	return_types.emplace_back(LogicalType::LIST(LogicalTypeId::BIGINT));

	auto qname = QualifiedName::Parse(input.inputs[0].GetValue<string>());

	// look up the table name in the catalog
	Binder::BindSchemaOrCatalog(context, qname.catalog, qname.schema);
	auto &table_entry = Catalog::GetEntry<TableCatalogEntry>(context, qname.catalog, qname.schema, qname.name);
	ColumnSegmentInfoScanState scan_state;
	table_entry.InitializeColumnSegmentInfoScan(scan_state);
	return make_uniq<PragmaStorageFunctionData>(table_entry, std::move(scan_state));
}

unique_ptr<GlobalTableFunctionState> PragmaStorageInfoInitGlobal(ClientContext &context,
                                                                 TableFunctionInitInput &input) {
	return make_uniq<PragmaStorageGlobalState>();
}

unique_ptr<LocalTableFunctionState> PragmaStorageInfoInitLocal(ExecutionContext &context, TableFunctionInitInput &input,
                                                               GlobalTableFunctionState *global_state) {
	return make_uniq<PragmaStorageLocalState>();
}

static Value ValueFromBlockIdList(const vector<block_id_t> &block_ids) {
	vector<Value> blocks;
	for (auto &block_id : block_ids) {
		blocks.push_back(Value::BIGINT(block_id));
	}
	return Value::LIST(LogicalTypeId::BIGINT, blocks);
}

static void PragmaStorageInfoFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &bind_data = data_p.bind_data->Cast<PragmaStorageFunctionData>();
	auto &gstate = data_p.global_state->Cast<PragmaStorageGlobalState>();
	auto &lstate = data_p.local_state->Cast<PragmaStorageLocalState>();
	auto &columns = bind_data.table_entry.GetColumns();
	QueryContext query_context(context);

	idx_t count = 0;

	auto &row_group_id = output.data[0];
	auto &column_name = output.data[1];
	auto &column_id = output.data[2];
	auto &column_path = output.data[3];
	auto &segment_id = output.data[4];
	auto &segment_type = output.data[5];
	auto &start = output.data[6];
	auto &count_col = output.data[7];
	auto &compression = output.data[8];
	auto &stats = output.data[9];
	auto &has_updates = output.data[10];
	auto &persistent = output.data[11];
	auto &block_id = output.data[12];
	auto &block_offset = output.data[13];
	auto &segment_info = output.data[14];
	auto &additional_block_ids = output.data[15];

	while (count < STANDARD_VECTOR_SIZE) {
		if (lstate.buffer_offset >= lstate.buffer.size()) {
			// drained the current row group's buffer; pull the next row group under the global lock
			lstate.buffer.clear();
			lstate.buffer_offset = 0;
			bool has_more;
			{
				lock_guard<mutex> guard(gstate.lock);
				has_more =
				    bind_data.table_entry.ScanColumnSegmentInfo(query_context, bind_data.scan_state, lstate.buffer);
			}
			if (!has_more) {
				break;
			}
			if (lstate.buffer.empty()) {
				continue;
			}
			// every entry in buffer comes from the same row group; use its index as the batch index
			lstate.batch_index = lstate.buffer.front().row_group_index;
		}

		auto &entry = lstate.buffer[lstate.buffer_offset++];

		row_group_id.Append(Value::BIGINT(NumericCast<int64_t>(entry.row_group_index)));
		auto &col = columns.GetColumn(PhysicalIndex(entry.column_id));
		column_name.Append(Value(col.Name()));
		column_id.Append(Value::BIGINT(NumericCast<int64_t>(entry.column_id)));
		column_path.Append(Value(entry.column_path));
		segment_id.Append(Value::BIGINT(NumericCast<int64_t>(entry.segment_idx)));
		segment_type.Append(Value(entry.segment_type));
		start.Append(Value::BIGINT(NumericCast<int64_t>(entry.segment_start)));
		count_col.Append(Value::BIGINT(NumericCast<int64_t>(entry.segment_count)));
		compression.Append(Value(entry.compression_type));
		stats.Append(entry.segment_stats);
		has_updates.Append(Value::BOOLEAN(entry.has_updates));
		persistent.Append(Value::BOOLEAN(entry.persistent));
		if (entry.persistent) {
			block_id.Append(Value::BIGINT(entry.block_id));
			block_offset.Append(Value::BIGINT(NumericCast<int64_t>(entry.block_offset)));
		} else {
			block_id.Append(Value());
			block_offset.Append(Value());
		}
		segment_info.Append(Value(entry.segment_info));
		if (entry.persistent) {
			additional_block_ids.Append(ValueFromBlockIdList(entry.additional_blocks));
		} else {
			additional_block_ids.Append(Value());
		}

		count++;
	}
	output.SetCardinality(count);
}

static OperatorPartitionData PragmaStorageInfoGetPartitionData(ClientContext &context,
                                                               TableFunctionGetPartitionInput &input) {
	auto &lstate = input.local_state->Cast<PragmaStorageLocalState>();
	return OperatorPartitionData(lstate.batch_index);
}

void PragmaStorageInfo::RegisterFunction(BuiltinFunctions &set) {
	TableFunction storage_info("pragma_storage_info", {LogicalType::VARCHAR}, PragmaStorageInfoFunction,
	                           PragmaStorageInfoBind, PragmaStorageInfoInitGlobal, PragmaStorageInfoInitLocal);
	storage_info.get_partition_data = PragmaStorageInfoGetPartitionData;
	set.AddFunction(std::move(storage_info));
}

} // namespace duckdb
