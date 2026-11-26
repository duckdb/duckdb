#include "duckdb/common/multi_file/table_function_query_filter.hpp"

#include "duckdb/common/algorithm.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/bit.hpp"
#include "duckdb/common/types/data_chunk.hpp"
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

unique_ptr<DeleteFilter> TableFunctionQueryFilter::TryCreate(ClientContext &context, const OpenFileInfo &file) {
	if (!HasFilterQuery(file)) {
		return nullptr;
	}
	auto result = make_uniq<TableFunctionQueryFilter>();
	result->ReadFilterQuery(context, StringValue::Get(file.extended_info->options.at(FILTER_QUERY_OPTION)), file.path);
	return std::move(result);
}

void TableFunctionQueryFilter::ReadFilterQuery(ClientContext &context, const string &query, const string &path) {
	// run the query on a connection of its own - this runs while the scan holds the lock of its context
	Connection connection(DatabaseInstance::GetDatabase(context));
	auto submitted = connection.Submit(StringUtil::Format("SELECT * FROM (%s)", query));
	if (submitted->HasError()) {
		submitted->GetErrorObject().Throw(StringUtil::Format("Failed to read the filter query of file %s: ", path));
	}
	QueryResultStream<> stream(std::move(submitted));
	auto &types = stream.GetTypes();
	bool is_bitmap = types.size() == 1 && types[0].id() == LogicalTypeId::BIT;
	has_ranges = types.size() == 2 && types[0].id() == LogicalTypeId::BIGINT && types[1].id() == LogicalTypeId::BIGINT;
	if (!is_bitmap && !has_ranges) {
		throw InvalidInputException("The filter query of file %s must return a BIT, or two BIGINT columns "
		                            "(row_start, row_end)",
		                            path);
	}
	while (auto chunk = stream.Fetch()) {
		if (chunk->size() == 0) {
			break;
		}
		if (has_ranges) {
			AddRanges(*chunk);
			continue;
		}
		for (auto entry : chunk->data[0].ValidValues<string_t>()) {
			auto bits = entry.GetValue();
			AddBitmap(bits.GetData(), bits.GetSize(), path);
		}
	}
	if (stream.HasError()) {
		stream.GetErrorObject().Throw(StringUtil::Format("Failed to read the filter query of file %s: ", path));
	}
	if (has_ranges) {
		FinalizeRanges();
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

void TableFunctionQueryFilter::AddRanges(DataChunk &chunk) {
	auto starts = chunk.data[0].Values<int64_t>();
	auto ends = chunk.data[1].Values<int64_t>();
	for (idx_t i = 0; i < chunk.size(); i++) {
		auto row_start = starts[i];
		auto row_end = ends[i];
		idx_t range_start = 0;
		idx_t range_end = NumericLimits<idx_t>::Maximum();
		if (row_start.IsValid() && row_start.GetValue() > 0) {
			range_start = NumericCast<idx_t>(row_start.GetValue());
		}
		if (row_end.IsValid()) {
			if (row_end.GetValue() <= 0) {
				continue;
			}
			range_end = NumericCast<idx_t>(row_end.GetValue());
		}
		if (range_start < range_end) {
			scan_ranges.emplace_back(range_start, range_end);
		}
	}
}

void TableFunctionQueryFilter::FinalizeRanges() {
	sort(scan_ranges.begin(), scan_ranges.end());
	vector<pair<idx_t, idx_t>> merged;
	for (auto &range : scan_ranges) {
		if (!merged.empty() && range.first <= merged.back().second) {
			merged.back().second = MaxValue<idx_t>(merged.back().second, range.second);
			continue;
		}
		merged.push_back(range);
	}
	scan_ranges = std::move(merged);
}

bool TableFunctionQueryFilter::RowIsScanned(idx_t row_idx) const {
	auto data = reinterpret_cast<const uint8_t *>(scan_bits.data());
	auto pos = row_idx + data[0];
	return (data[1 + pos / 8] >> (7 - pos % 8)) & 1;
}

bool TableFunctionQueryFilter::RowsAreScanned(idx_t row_start, idx_t count) const {
	if (row_start + count > bit_count) {
		return false;
	}
	auto data = reinterpret_cast<const uint8_t *>(scan_bits.data());
	auto pos = row_start + data[0];
	auto end = pos + count;
	auto bit_is_set = [&](idx_t bit_pos) {
		return (data[1 + bit_pos / 8] >> (7 - bit_pos % 8)) & 1;
	};
	for (; pos < end && pos % 8 != 0; pos++) {
		if (!bit_is_set(pos)) {
			return false;
		}
	}
	for (; pos + 8 <= end; pos += 8) {
		if (data[1 + pos / 8] != 0xFF) {
			return false;
		}
	}
	for (; pos < end; pos++) {
		if (!bit_is_set(pos)) {
			return false;
		}
	}
	return true;
}

idx_t TableFunctionQueryFilter::Filter(row_t start_row_index, idx_t count, SelectionVector &result_sel) {
	auto start = NumericCast<idx_t>(start_row_index);
	auto end = start + count;
	if (has_ranges) {
		// the first range that ends after the start of these rows
		auto range = std::upper_bound(scan_ranges.begin(), scan_ranges.end(), start,
		                              [](idx_t row, const pair<idx_t, idx_t> &r) { return row < r.second; });
		if (range == scan_ranges.end() || range->first >= end) {
			return 0;
		}
		if (range->first <= start && range->second >= end) {
			return count;
		}
		result_sel.Initialize(STANDARD_VECTOR_SIZE);
		idx_t result_count = 0;
		for (; range != scan_ranges.end() && range->first < end; ++range) {
			auto range_start = MaxValue<idx_t>(range->first, start);
			auto range_end = MinValue<idx_t>(range->second, end);
			for (idx_t row = range_start; row < range_end; row++) {
				result_sel.set_index(result_count++, row - start);
			}
		}
		return result_count;
	}
	if (RowsAreScanned(start, count)) {
		return count;
	}
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
