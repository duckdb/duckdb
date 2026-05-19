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
	return_types.emplace_back(LogicalType::VARCHAR);

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
	while (data.offset < bind_data.column_segments_info.size() && count < STANDARD_VECTOR_SIZE) {
		auto &entry = bind_data.column_segments_info[data.offset++];

		idx_t col_idx = 0;
		// row_group_id
		output.SetValue(col_idx++, count, Value::BIGINT(NumericCast<int64_t>(entry.row_group_index)));
		// column_name
		auto &col = columns.GetColumn(PhysicalIndex(entry.column_id));
		output.SetValue(col_idx++, count, Value(col.Name()));
		// column_id
		output.SetValue(col_idx++, count, Value::BIGINT(NumericCast<int64_t>(entry.column_id)));
		// column_path
		output.SetValue(col_idx++, count, Value(entry.column_path));
		// segment_id
		output.SetValue(col_idx++, count, Value::BIGINT(NumericCast<int64_t>(entry.segment_idx)));
		// segment_type
		output.SetValue(col_idx++, count, Value(entry.segment_type));
		// start
		output.SetValue(col_idx++, count, Value::BIGINT(NumericCast<int64_t>(entry.segment_start)));
		// count
		output.SetValue(col_idx++, count, Value::BIGINT(NumericCast<int64_t>(entry.segment_count)));
		// compression
		output.SetValue(col_idx++, count, Value(entry.compression_type));
		// stats
		output.SetValue(col_idx++, count, Value(entry.segment_stats));
		// has_updates
		output.SetValue(col_idx++, count, Value::BOOLEAN(entry.has_updates));
		// persistent
		output.SetValue(col_idx++, count, Value::BOOLEAN(entry.persistent));
		// block_id
		// block_offset
		if (entry.persistent) {
			output.SetValue(col_idx++, count, Value::BIGINT(entry.block_id));
			output.SetValue(col_idx++, count, Value::BIGINT(NumericCast<int64_t>(entry.block_offset)));
		} else {
			output.SetValue(col_idx++, count, Value());
			output.SetValue(col_idx++, count, Value());
		}
		// segment_info
		output.SetValue(col_idx++, count, Value(entry.segment_info));
		// additional_block_ids
		if (entry.persistent) {
			output.SetValue(col_idx++, count, ValueFromBlockIdList(entry.additional_blocks));
		} else {
			output.SetValue(col_idx++, count, Value());
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
