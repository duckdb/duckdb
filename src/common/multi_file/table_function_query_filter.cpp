#include "duckdb/common/multi_file/table_function_query_filter.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/bit.hpp"
#include "duckdb/common/types/selection_vector.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/common/vector/vector_iterator.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/query_result_stream.hpp"

namespace duckdb {

static const char *FILTER_QUERY_OPTION = "filter_query";

bool TableFunctionQueryFilter::HasFilterQuery(const OpenFileInfo &file) {
	if (!file.extended_info) {
		return false;
	}
	auto entry = file.extended_info->options.find(FILTER_QUERY_OPTION);
	return entry != file.extended_info->options.end() && !entry->second.IsNull();
}

unique_ptr<DeleteFilter> TableFunctionQueryFilter::TryCreate(ClientContext &context, BaseFileReader &reader) {
	if (!HasFilterQuery(reader.file)) {
		return nullptr;
	}
	auto result = make_uniq<TableFunctionQueryFilter>();
	auto query = StringValue::Get(reader.file.extended_info->options.at(FILTER_QUERY_OPTION));
	result->ReadFilterQuery(context, query, reader.file.path, reader.TryGetRowCount(context));
	return std::move(result);
}

void TableFunctionQueryFilter::ReadFilterQuery(ClientContext &context, const string &query, const string &path,
                                               optional_idx row_count) {
	// run the query on a connection of its own - this runs while the scan holds the lock of its context
	Connection connection(DatabaseInstance::GetDatabase(context));
	string sql;
	if (row_count.IsValid()) {
		// the rows of the file are visible to the query as the CTE file_row_ids
		sql = StringUtil::Format("WITH file_row_ids AS (SELECT range::BIGINT AS row_id FROM range(%d)) ",
		                         row_count.GetIndex());
	}
	sql += StringUtil::Format("SELECT * FROM (%s)", query);
	auto submitted = connection.Submit(sql);
	if (submitted->HasError()) {
		submitted->GetErrorObject().Throw(StringUtil::Format("Failed to read the filter query of file %s: ", path));
	}
	QueryResultStream<> stream(std::move(submitted));
	auto &types = stream.GetTypes();
	if (types.size() != 1) {
		throw InvalidInputException("The filter query of file %s must return a single column", path);
	}
	auto type = types[0].InternalType();
	bool is_bitmap = types[0].id() == LogicalTypeId::BIT;
	if (!is_bitmap && !row_count.IsValid()) {
		throw NotImplementedException("The filter query of file %s returns row ids, which needs the number of rows of "
		                              "the file",
		                              path);
	}
	if (!is_bitmap) {
		// the rows to scan, none of them yet
		bit_count = row_count.GetIndex();
		scan_bits = string(Bit::ComputeBitstringLen(bit_count), '\0');
		scan_bits[0] = static_cast<char>((8 - (bit_count % 8)) % 8);
	}
	while (auto chunk = stream.Fetch()) {
		if (chunk->size() == 0) {
			break;
		}
		auto &column = chunk->data[0];
		if (is_bitmap) {
			for (auto entry : column.ValidValues<string_t>()) {
				auto bits = entry.GetValue();
				AddBitmap(bits.GetData(), bits.GetSize(), path);
			}
			continue;
		}
		switch (type) {
		case PhysicalType::INT8:
			AddRowIds<int8_t>(column, path);
			break;
		case PhysicalType::INT16:
			AddRowIds<int16_t>(column, path);
			break;
		case PhysicalType::INT32:
			AddRowIds<int32_t>(column, path);
			break;
		case PhysicalType::INT64:
			AddRowIds<int64_t>(column, path);
			break;
		case PhysicalType::UINT8:
			AddRowIds<uint8_t>(column, path);
			break;
		case PhysicalType::UINT16:
			AddRowIds<uint16_t>(column, path);
			break;
		case PhysicalType::UINT32:
			AddRowIds<uint32_t>(column, path);
			break;
		case PhysicalType::UINT64:
			AddRowIds<uint64_t>(column, path);
			break;
		default:
			throw InvalidInputException("The filter query of file %s must return integers or a BIT, not %s", path,
			                            types[0].ToString());
		}
	}
	if (stream.HasError()) {
		stream.GetErrorObject().Throw(StringUtil::Format("Failed to read the filter query of file %s: ", path));
	}
}

template <class T>
void TableFunctionQueryFilter::AddRowIds(Vector &row_ids, const string &path) {
	auto data = reinterpret_cast<uint8_t *>(&scan_bits[0]);
	auto padding = idx_t(data[0]);
	for (auto entry : row_ids.ValidValues<T>()) {
		auto row_id = entry.GetValue();
		if (row_id < 0 || static_cast<uint64_t>(row_id) >= bit_count) {
			continue;
		}
		auto pos = static_cast<idx_t>(row_id) + padding;
		data[1 + pos / 8] |= static_cast<uint8_t>(1 << (7 - pos % 8));
	}
}

void TableFunctionQueryFilter::AddBitmap(const char *data, idx_t size, const string &path) {
	if (scan_bits.empty()) {
		scan_bits.assign(data, size);
		bit_count = (size - 1) * 8 - static_cast<uint8_t>(data[0]);
		return;
	}
	if (size != scan_bits.size() || data[0] != scan_bits[0]) {
		throw InvalidInputException("The filter query of file %s returns bitmaps of different sizes", path);
	}
	for (idx_t i = 1; i < size; i++) {
		scan_bits[i] = static_cast<char>(scan_bits[i] | data[i]);
	}
}

bool TableFunctionQueryFilter::RowIsScanned(idx_t row_idx) const {
	auto data = reinterpret_cast<const uint8_t *>(scan_bits.data());
	auto pos = row_idx + data[0];
	return (data[1 + pos / 8] >> (7 - pos % 8)) & 1;
}

idx_t TableFunctionQueryFilter::Filter(row_t start_row_index, idx_t count, SelectionVector &result_sel) {
	auto start = NumericCast<idx_t>(start_row_index);
	result_sel.Initialize(STANDARD_VECTOR_SIZE);
	idx_t result_count = 0;
	for (idx_t i = 0; i < count && start + i < bit_count; i++) {
		if (RowIsScanned(start + i)) {
			result_sel.set_index(result_count++, i);
		}
	}
	return result_count;
}

} // namespace duckdb
