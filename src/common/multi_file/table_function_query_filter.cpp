#include "duckdb/common/multi_file/table_function_query_filter.hpp"

#include "duckdb/catalog/catalog_search_path.hpp"
#include "duckdb/common/algorithm.hpp"
#include "duckdb/common/multi_file/base_file_reader.hpp"
#include "duckdb/common/bit_utils.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/types/selection_vector.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/common/vector/vector_iterator.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/client_data.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/prepared_statement.hpp"
#include "duckdb/main/query_result_stream.hpp"
#include "duckdb/main/valid_checker.hpp"
#include "duckdb/transaction/meta_transaction.hpp"

#include <bitset>

namespace duckdb {

constexpr const char *TableFunctionQueryFilter::FILTER_QUERY_OPTION;

//! A filter query can scan files with filter queries of their own - bound the nesting before it exhausts the stack
static constexpr idx_t MAX_FILTER_QUERY_DEPTH = 8;

unique_ptr<DeleteFilter> TableFunctionQueryFilter::TryCreate(ClientContext &context, BaseFileReader &reader) {
	auto &file = reader.file;
	if (!HasFilterQuery(file)) {
		return nullptr;
	}
	auto &query = file.extended_info->options.at(FILTER_QUERY_OPTION);
	if (query.type().id() != LogicalTypeId::VARCHAR) {
		throw InvalidInputException("The %s option of file %s must be a VARCHAR, not %s", FILTER_QUERY_OPTION,
		                            file.path, query.type().ToString());
	}
	if (!reader.SupportsDeletionFilter()) {
		throw NotImplementedException("The %s option of file %s is not supported by the %s reader", FILTER_QUERY_OPTION,
		                              file.path, reader.GetReaderType());
	}
	auto result = make_uniq<TableFunctionQueryFilter>();
	result->ReadFilterQuery(context, StringValue::Get(query), file.path);
	return std::move(result);
}

//! Leaves the transaction of the caller that a filter query joined - without a vote to roll it back, the filter query
//! only reads
static void LeaveCallerTransaction(ClientContext &inner) {
	inner.Destroy();
	if (!inner.transaction.HasActiveTransaction()) {
		return;
	}
	if (ValidChecker::IsInvalidated(inner.transaction.ActiveTransaction())) {
		inner.transaction.Rollback(nullptr);
	} else {
		inner.transaction.Commit();
	}
}

void TableFunctionQueryFilter::ReadFilterQuery(ClientContext &context, const string &query, const string &path) {
	if (context.internal_depth + 1 > MAX_FILTER_QUERY_DEPTH) {
		throw InvalidInputException("Filter queries are nested too deeply (maximum depth %llu) - does the filter query "
		                            "of file %s scan the file itself?",
		                            MAX_FILTER_QUERY_DEPTH, path);
	}
	if (!context.transaction.HasActiveTransaction()) {
		throw InternalException("The filter query of file %s is read outside of a transaction", path);
	}
	// the scan holds the lock of its context: run the query on an internal connection that is interrupted with the
	// caller, and that joins the transaction of the caller so that it sees what the caller sees
	auto connection = Connection::CreateInternal(context);
	auto &inner = *connection.context;
	// resolve names as the caller does (USE, SET schema, SET search_path), unqualified entries included
	auto &caller_paths = ClientData::Get(context).catalog_search_path->GetSetPaths();
	ClientData::Get(inner).catalog_search_path = make_uniq<CatalogSearchPath>(inner, caller_paths);
	ErrorData error;
	try {
		auto statements = connection.ExtractStatements(query);
		if (statements.size() != 1 || statements[0]->type != StatementType::SELECT_STATEMENT) {
			throw InvalidInputException("The filter query must be a single SELECT statement");
		}
		// a failing filter query affects the transaction of the caller as a failing statement of the caller would
		inner.transaction.InheritTransaction(context);
		auto prepared = connection.Prepare(std::move(statements[0]));
		if (prepared->HasError()) {
			prepared->GetErrorObject().Throw();
		}
		if (!prepared->GetStatementProperties().IsReadOnly()) {
			throw InvalidInputException("The filter query must not modify a database");
		}
		vector<Value> values;
		QueryResultStream<> stream(prepared->Submit(values));
		if (stream.HasError()) {
			stream.GetErrorObject().Throw();
		}
		auto &types = stream.GetTypes();
		if (types.size() != 3 || types[0].id() != LogicalTypeId::BIGINT || types[1].id() != LogicalTypeId::BIGINT ||
		    types[2].id() != LogicalTypeId::BIT) {
			throw InvalidInputException("The filter query must return (row_start BIGINT, row_end BIGINT, bits BIT)");
		}
		while (auto chunk = stream.Fetch()) {
			if (chunk->size() == 0) {
				break;
			}
			AddRows(*chunk);
		}
		if (stream.HasError()) {
			stream.GetErrorObject().Throw();
		}
	} catch (std::exception &ex) {
		error = ErrorData(ex);
	}
	LeaveCallerTransaction(inner);
	if (error.HasError()) {
		error.Throw(StringUtil::Format("Failed to read the filter query of file %s: ", path));
	}
	Finalize();
}

static uint8_t ReverseBits(uint8_t value) {
	value = static_cast<uint8_t>(((value & 0xF0) >> 4) | ((value & 0x0F) << 4));
	value = static_cast<uint8_t>(((value & 0xCC) >> 2) | ((value & 0x33) << 2));
	value = static_cast<uint8_t>(((value & 0xAA) >> 1) | ((value & 0x55) << 1));
	return value;
}

void TableFunctionQueryFilter::AddRows(DataChunk &chunk) {
	auto starts = chunk.data[0].Values<int64_t>();
	auto ends = chunk.data[1].Values<int64_t>();
	auto bitmaps = chunk.data[2].Values<string_t>();
	for (idx_t i = 0; i < chunk.size(); i++) {
		auto row_start = starts[i];
		auto row_end = ends[i];
		auto bitmap = bitmaps[i];
		if (bitmap.IsValid()) {
			if (row_start.IsValid() && row_start.GetValue() < 0) {
				throw InvalidInputException("The filter query returns a bitmap that starts before the file");
			}
			auto bits = bitmap.GetValue();
			auto window_start = row_start.IsValid() ? NumericCast<idx_t>(row_start.GetValue()) : 0;
			AddWindow(window_start, bits.GetData(), bits.GetSize());
			continue;
		}
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

void TableFunctionQueryFilter::AddWindowBits(idx_t row_start, uint64_t bits, idx_t bit_count) {
	auto word_index = row_start / 64;
	auto shift = row_start % 64;
	auto add_to_word = [&](idx_t index, uint64_t word_bits) {
		if (!scan_words.empty() && scan_words.back().index == index) {
			scan_words.back().bits |= word_bits;
		} else {
			scan_words.push_back(ScanWord {index, word_bits});
		}
	};
	add_to_word(word_index, bits << shift);
	if (shift + bit_count > 64) {
		add_to_word(word_index + 1, bits >> (64 - shift));
	}
}

void TableFunctionQueryFilter::AddWindow(idx_t window_start, const char *data, idx_t size) {
	if (size < 2) {
		return;
	}
	auto bytes = reinterpret_cast<const uint8_t *>(data);
	// the first byte is the number of padding bits - the most significant bits of the first byte that follows
	auto padding = idx_t(bytes[0]);
	idx_t byte_idx = 0;
	if (padding == 0 && window_start % 64 == 0) {
		// the bytes are 8 words of rows when they start at a word: reverse the bits of every byte
		for (; byte_idx + 8 < size; byte_idx += 8) {
			uint64_t word;
			memcpy(&word, bytes + 1 + byte_idx, sizeof(word));
			word = ((word & 0xF0F0F0F0F0F0F0F0ULL) >> 4) | ((word & 0x0F0F0F0F0F0F0F0FULL) << 4);
			word = ((word & 0xCCCCCCCCCCCCCCCCULL) >> 2) | ((word & 0x3333333333333333ULL) << 2);
			word = ((word & 0xAAAAAAAAAAAAAAAAULL) >> 1) | ((word & 0x5555555555555555ULL) << 1);
			if (word != 0) {
				scan_words.push_back(ScanWord {window_start / 64 + byte_idx / 8, word});
			}
		}
	}
	for (; byte_idx + 1 < size; byte_idx++) {
		auto value = bytes[1 + byte_idx];
		if (byte_idx == 0) {
			value = static_cast<uint8_t>(value & (0xFF >> padding));
		}
		if (value == 0) {
			continue;
		}
		// the bit of the highest significance is the first row of the byte
		uint64_t bits = ReverseBits(value);
		if (byte_idx == 0) {
			AddWindowBits(window_start, bits >> padding, 8 - padding);
		} else {
			AddWindowBits(window_start + byte_idx * 8 - padding, bits, 8);
		}
	}
}

void TableFunctionQueryFilter::Finalize() {
	sort(scan_ranges.begin(), scan_ranges.end());
	vector<pair<idx_t, idx_t>> merged_ranges;
	for (auto &range : scan_ranges) {
		if (!merged_ranges.empty() && range.first <= merged_ranges.back().second) {
			merged_ranges.back().second = MaxValue<idx_t>(merged_ranges.back().second, range.second);
			continue;
		}
		merged_ranges.push_back(range);
	}
	scan_ranges = std::move(merged_ranges);

	sort(scan_words.begin(), scan_words.end(),
	     [](const ScanWord &lhs, const ScanWord &rhs) { return lhs.index < rhs.index; });
	vector<ScanWord> merged_words;
	for (auto &word : scan_words) {
		if (!merged_words.empty() && merged_words.back().index == word.index) {
			merged_words.back().bits |= word.bits;
		} else if (word.bits != 0) {
			merged_words.push_back(word);
		}
	}
	scan_words = std::move(merged_words);
}

//! The bits [begin, end) of a word, where begin < end <= 64
static uint64_t BitRange(idx_t begin, idx_t end) {
	auto upper = end == 64 ? ~uint64_t(0) : ((uint64_t(1) << end) - 1);
	return upper & ~((uint64_t(1) << begin) - 1);
}

idx_t TableFunctionQueryFilter::Filter(row_t start_row_index, idx_t count, SelectionVector &result_sel) {
	auto start = NumericCast<idx_t>(start_row_index);
	auto end = start + count;
	auto range = std::upper_bound(scan_ranges.begin(), scan_ranges.end(), start,
	                              [](idx_t row, const pair<idx_t, idx_t> &r) { return row < r.second; });
	if (range != scan_ranges.end() && range->first <= start && range->second >= end) {
		// one range scans all of the rows
		return count;
	}
	// the rows that are scanned, as a bitmap of the rows of this vector
	static constexpr idx_t MAX_WORDS = STANDARD_VECTOR_SIZE / 64 + 2;
	uint64_t scanned[MAX_WORDS] = {};
	auto first_word = start / 64;
	auto last_word = (end - 1) / 64;
	D_ASSERT(last_word - first_word < MAX_WORDS);
	auto base = first_word * 64;
	for (; range != scan_ranges.end() && range->first < end; ++range) {
		auto range_start = MaxValue<idx_t>(range->first, start) - base;
		auto range_end = MinValue<idx_t>(range->second, end) - base;
		for (auto word = range_start / 64; word <= (range_end - 1) / 64; word++) {
			scanned[word] |= BitRange(MaxValue<idx_t>(range_start, word * 64) - word * 64,
			                          MinValue<idx_t>(range_end, (word + 1) * 64) - word * 64);
		}
	}
	auto scan_word = std::lower_bound(scan_words.begin(), scan_words.end(), first_word,
	                                  [](const ScanWord &word, idx_t index) { return word.index < index; });
	for (; scan_word != scan_words.end() && scan_word->index <= last_word; ++scan_word) {
		scanned[scan_word->index - first_word] |= scan_word->bits;
	}
	// only the rows of this vector
	idx_t scan_count = 0;
	for (auto word = first_word; word <= last_word; word++) {
		auto &bits = scanned[word - first_word];
		bits &=
		    BitRange(MaxValue<idx_t>(start, word * 64) - word * 64, MinValue<idx_t>(end, (word + 1) * 64) - word * 64);
		scan_count += std::bitset<64>(bits).count();
	}
	if (scan_count == count || scan_count == 0) {
		return scan_count;
	}
	result_sel.Initialize(STANDARD_VECTOR_SIZE);
	idx_t result_count = 0;
	for (auto word = first_word; word <= last_word; word++) {
		auto bits = scanned[word - first_word];
		while (bits) {
			auto bit = CountZeros<uint64_t>::Trailing(bits);
			result_sel.set_index(result_count++, word * 64 + bit - start);
			bits &= bits - 1;
		}
	}
	return result_count;
}

} // namespace duckdb
