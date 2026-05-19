#include "duckdb/function/table/system_functions.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/view_catalog_entry.hpp"
#include "duckdb/parser/qualified_name.hpp"
#include "duckdb/planner/constraints/bound_not_null_constraint.hpp"
#include "duckdb/planner/constraints/bound_unique_constraint.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/limits.hpp"
#include "duckdb/storage/data_table.hpp"
#include "duckdb/storage/table_storage_info.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/storage/table/column_data.hpp"
#include "duckdb/common/enums/column_segment_info_scan_type.hpp"

#include <algorithm>

namespace duckdb {

struct PragmaStorageFunctionData : public TableFunctionData {
	explicit PragmaStorageFunctionData(TableCatalogEntry &table_entry) : table_entry(table_entry) {
	}

	TableCatalogEntry &table_entry;
	ColumnSegmentInfoScanType scan_type = ColumnSegmentInfoScanType::ALL;
	vector<ColumnSegmentInfo> column_segments_info;
};

struct PragmaStorageOperatorData : public GlobalTableFunctionState {
	PragmaStorageOperatorData() : offset(0) {
	}

	idx_t offset;
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
	auto result = make_uniq<PragmaStorageFunctionData>(table_entry);

	auto scan_type_entry = input.named_parameters.find("scan_type");
	if (scan_type_entry != input.named_parameters.end()) {
		result->scan_type = ColumnSegmentInfoScanTypeFromString(scan_type_entry->second.GetValue<string>());
	}
	result->column_segments_info = table_entry.GetColumnSegmentInfo(context, result->scan_type);
	return std::move(result);
}

unique_ptr<GlobalTableFunctionState> PragmaStorageInfoInit(ClientContext &context, TableFunctionInitInput &input) {
	return make_uniq<PragmaStorageOperatorData>();
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
	auto &data = data_p.global_state->Cast<PragmaStorageOperatorData>();
	idx_t count = 0;
	auto &columns = bind_data.table_entry.GetColumns();

	// row_group_id
	auto &row_group_id = output.data[0];
	// column_name
	auto &column_name = output.data[1];
	// column_id
	auto &column_id = output.data[2];
	// column_path
	auto &column_path = output.data[3];
	// segment_id
	auto &segment_id = output.data[4];
	// segment_type
	auto &segment_type = output.data[5];
	// start
	auto &start = output.data[6];
	// count
	auto &count_col = output.data[7];
	// compression
	auto &compression = output.data[8];
	// stats
	auto &stats = output.data[9];
	// has_updates
	auto &has_updates = output.data[10];
	// persistent
	auto &persistent = output.data[11];
	// block_id
	auto &block_id = output.data[12];
	// block_offset
	auto &block_offset = output.data[13];
	// segment_info
	auto &segment_info = output.data[14];
	// additional_block_ids
	auto &additional_block_ids = output.data[15];

	while (data.offset < bind_data.column_segments_info.size() && count < STANDARD_VECTOR_SIZE) {
		auto &entry = bind_data.column_segments_info[data.offset++];

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

void PragmaStorageInfo::RegisterFunction(BuiltinFunctions &set) {
	TableFunction func("pragma_storage_info", {LogicalType::VARCHAR}, PragmaStorageInfoFunction, PragmaStorageInfoBind,
	                   PragmaStorageInfoInit);
	func.named_parameters["scan_type"] = LogicalType::VARCHAR;
	set.AddFunction(func);
}

} // namespace duckdb
