#include "column_writer.hpp"

#include "duckdb.hpp"
#include "geo_parquet.hpp"
#include "parquet_dbp_encoder.hpp"
#include "parquet_dlba_encoder.hpp"
#include "parquet_rle_bp_decoder.hpp"
#include "parquet_rle_bp_encoder.hpp"
#include "parquet_bss_encoder.hpp"
#include "parquet_statistics.hpp"
#include "parquet_writer.hpp"
#include "writer/array_column_writer.hpp"
#include "writer/list_column_writer.hpp"
#include "writer/primitive_column_writer.hpp"
#include "writer/struct_column_writer.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/operator/comparison_operators.hpp"
#include "duckdb/common/serializer/buffered_file_writer.hpp"
#include "duckdb/common/serializer/memory_stream.hpp"
#include "duckdb/common/serializer/write_stream.hpp"
#include "duckdb/common/string_map_set.hpp"
#include "duckdb/common/types/hugeint.hpp"
#include "duckdb/common/types/time.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/common/types/uhugeint.hpp"
#include "duckdb/execution/expression_executor.hpp"

#include "brotli/encode.h"
#include "lz4.hpp"
#include "miniz_wrapper.hpp"
#include "snappy.h"
#include "zstd.h"
#include "zstd/common/xxhash.hpp"

#include <cmath>

namespace duckdb {

using namespace duckdb_parquet; // NOLINT
using namespace duckdb_miniz;   // NOLINT

using duckdb_parquet::CompressionCodec;
using duckdb_parquet::ConvertedType;
using duckdb_parquet::Encoding;
using duckdb_parquet::FieldRepetitionType;
using duckdb_parquet::FileMetaData;
using duckdb_parquet::PageHeader;
using duckdb_parquet::PageType;
using ParquetRowGroup = duckdb_parquet::RowGroup;
using duckdb_parquet::Type;

constexpr uint16_t ColumnWriter::PARQUET_DEFINE_VALID;

//===--------------------------------------------------------------------===//
// ColumnWriterStatistics
//===--------------------------------------------------------------------===//
ColumnWriterStatistics::~ColumnWriterStatistics() {
}

bool ColumnWriterStatistics::HasStats() {
	return false;
}

string ColumnWriterStatistics::GetMin() {
	return string();
}

string ColumnWriterStatistics::GetMax() {
	return string();
}

string ColumnWriterStatistics::GetMinValue() {
	return string();
}

string ColumnWriterStatistics::GetMaxValue() {
	return string();
}

//===--------------------------------------------------------------------===//
// RleBpEncoder
//===--------------------------------------------------------------------===//
RleBpEncoder::RleBpEncoder(uint32_t bit_width)
    : byte_width((bit_width + 7) / 8), byte_count(idx_t(-1)), run_count(idx_t(-1)) {
}

// we always RLE everything (for now)
void RleBpEncoder::BeginPrepare(uint32_t first_value) {
	byte_count = 0;
	run_count = 1;
	current_run_count = 1;
	last_value = first_value;
}

void RleBpEncoder::FinishRun() {
	// last value, or value has changed
	// write out the current run
	byte_count += ParquetDecodeUtils::GetVarintSize(current_run_count << 1) + byte_width;
	current_run_count = 1;
	run_count++;
}

void RleBpEncoder::PrepareValue(uint32_t value) {
	if (value != last_value) {
		FinishRun();
		last_value = value;
	} else {
		current_run_count++;
	}
}

void RleBpEncoder::FinishPrepare() {
	FinishRun();
}

idx_t RleBpEncoder::GetByteCount() {
	D_ASSERT(byte_count != idx_t(-1));
	return byte_count;
}

void RleBpEncoder::BeginWrite(WriteStream &writer, uint32_t first_value) {
	// start the RLE runs
	last_value = first_value;
	current_run_count = 1;
}

void RleBpEncoder::WriteRun(WriteStream &writer) {
	// write the header of the run
	ParquetDecodeUtils::VarintEncode(current_run_count << 1, writer);
	// now write the value
	D_ASSERT(last_value >> (byte_width * 8) == 0);
	switch (byte_width) {
	case 1:
		writer.Write<uint8_t>(last_value);
		break;
	case 2:
		writer.Write<uint16_t>(last_value);
		break;
	case 3:
		writer.Write<uint8_t>(last_value & 0xFF);
		writer.Write<uint8_t>((last_value >> 8) & 0xFF);
		writer.Write<uint8_t>((last_value >> 16) & 0xFF);
		break;
	case 4:
		writer.Write<uint32_t>(last_value);
		break;
	default:
		throw InternalException("unsupported byte width for RLE encoding");
	}
	current_run_count = 1;
}

void RleBpEncoder::WriteValue(WriteStream &writer, uint32_t value) {
	if (value != last_value) {
		WriteRun(writer);
		last_value = value;
	} else {
		current_run_count++;
	}
}

void RleBpEncoder::FinishWrite(WriteStream &writer) {
	WriteRun(writer);
}

//===--------------------------------------------------------------------===//
// ColumnWriter
//===--------------------------------------------------------------------===//
ColumnWriter::ColumnWriter(ParquetWriter &writer, idx_t schema_idx, vector<string> schema_path_p, idx_t max_repeat,
                           idx_t max_define, bool can_have_nulls)
    : writer(writer), schema_idx(schema_idx), schema_path(std::move(schema_path_p)), max_repeat(max_repeat),
      max_define(max_define), can_have_nulls(can_have_nulls) {
}
ColumnWriter::~ColumnWriter() {
}

ColumnWriterState::~ColumnWriterState() {
}

void ColumnWriter::CompressPage(MemoryStream &temp_writer, size_t &compressed_size, data_ptr_t &compressed_data,
                                unique_ptr<data_t[]> &compressed_buf) {
	switch (writer.GetCodec()) {
	case CompressionCodec::UNCOMPRESSED:
		compressed_size = temp_writer.GetPosition();
		compressed_data = temp_writer.GetData();
		break;

	case CompressionCodec::SNAPPY: {
		compressed_size = duckdb_snappy::MaxCompressedLength(temp_writer.GetPosition());
		compressed_buf = unique_ptr<data_t[]>(new data_t[compressed_size]);
		duckdb_snappy::RawCompress(const_char_ptr_cast(temp_writer.GetData()), temp_writer.GetPosition(),
		                           char_ptr_cast(compressed_buf.get()), &compressed_size);
		compressed_data = compressed_buf.get();
		D_ASSERT(compressed_size <= duckdb_snappy::MaxCompressedLength(temp_writer.GetPosition()));
		break;
	}
	case CompressionCodec::LZ4_RAW: {
		compressed_size = duckdb_lz4::LZ4_compressBound(UnsafeNumericCast<int32_t>(temp_writer.GetPosition()));
		compressed_buf = unique_ptr<data_t[]>(new data_t[compressed_size]);
		compressed_size = duckdb_lz4::LZ4_compress_default(
		    const_char_ptr_cast(temp_writer.GetData()), char_ptr_cast(compressed_buf.get()),
		    UnsafeNumericCast<int32_t>(temp_writer.GetPosition()), UnsafeNumericCast<int32_t>(compressed_size));
		compressed_data = compressed_buf.get();
		break;
	}
	case CompressionCodec::GZIP: {
		MiniZStream s;
		compressed_size = s.MaxCompressedLength(temp_writer.GetPosition());
		compressed_buf = unique_ptr<data_t[]>(new data_t[compressed_size]);
		s.Compress(const_char_ptr_cast(temp_writer.GetData()), temp_writer.GetPosition(),
		           char_ptr_cast(compressed_buf.get()), &compressed_size);
		compressed_data = compressed_buf.get();
		break;
	}
	case CompressionCodec::ZSTD: {
		compressed_size = duckdb_zstd::ZSTD_compressBound(temp_writer.GetPosition());
		compressed_buf = unique_ptr<data_t[]>(new data_t[compressed_size]);
		compressed_size = duckdb_zstd::ZSTD_compress((void *)compressed_buf.get(), compressed_size,
		                                             (const void *)temp_writer.GetData(), temp_writer.GetPosition(),
		                                             UnsafeNumericCast<int32_t>(writer.CompressionLevel()));
		compressed_data = compressed_buf.get();
		break;
	}
	case CompressionCodec::BROTLI: {

		compressed_size = duckdb_brotli::BrotliEncoderMaxCompressedSize(temp_writer.GetPosition());
		compressed_buf = unique_ptr<data_t[]>(new data_t[compressed_size]);

		duckdb_brotli::BrotliEncoderCompress(BROTLI_DEFAULT_QUALITY, BROTLI_DEFAULT_WINDOW, BROTLI_DEFAULT_MODE,
		                                     temp_writer.GetPosition(), temp_writer.GetData(), &compressed_size,
		                                     compressed_buf.get());
		compressed_data = compressed_buf.get();

		break;
	}
	default:
		throw InternalException("Unsupported codec for Parquet Writer");
	}

	if (compressed_size > idx_t(NumericLimits<int32_t>::Maximum())) {
		throw InternalException("Parquet writer: %d compressed page size out of range for type integer",
		                        temp_writer.GetPosition());
	}
}

void ColumnWriter::HandleRepeatLevels(ColumnWriterState &state, ColumnWriterState *parent, idx_t count,
                                      idx_t max_repeat) const {
	if (!parent) {
		// no repeat levels without a parent node
		return;
	}
	while (state.repetition_levels.size() < parent->repetition_levels.size()) {
		state.repetition_levels.push_back(parent->repetition_levels[state.repetition_levels.size()]);
	}
}

void ColumnWriter::HandleDefineLevels(ColumnWriterState &state, ColumnWriterState *parent, const ValidityMask &validity,
                                      const idx_t count, const uint16_t define_value, const uint16_t null_value) const {
	if (parent) {
		// parent node: inherit definition level from the parent
		idx_t vector_index = 0;
		while (state.definition_levels.size() < parent->definition_levels.size()) {
			idx_t current_index = state.definition_levels.size();
			if (parent->definition_levels[current_index] != PARQUET_DEFINE_VALID) {
				state.definition_levels.push_back(parent->definition_levels[current_index]);
			} else if (validity.RowIsValid(vector_index)) {
				state.definition_levels.push_back(define_value);
			} else {
				if (!can_have_nulls) {
					throw IOException("Parquet writer: map key column is not allowed to contain NULL values");
				}
				state.null_count++;
				state.definition_levels.push_back(null_value);
			}
			if (parent->is_empty.empty() || !parent->is_empty[current_index]) {
				vector_index++;
			}
		}
	} else {
		// no parent: set definition levels only from this validity mask
		for (idx_t i = 0; i < count; i++) {
			const auto is_null = !validity.RowIsValid(i);
			state.definition_levels.emplace_back(is_null ? null_value : define_value);
			state.null_count += is_null;
		}
		if (!can_have_nulls && state.null_count != 0) {
			throw IOException("Parquet writer: map key column is not allowed to contain NULL values");
		}
	}
}

//===--------------------------------------------------------------------===//
// Standard Column Writer
//===--------------------------------------------------------------------===//
template <class SRC, class T, class OP>
class NumericStatisticsState : public ColumnWriterStatistics {
public:
	NumericStatisticsState() : min(NumericLimits<T>::Maximum()), max(NumericLimits<T>::Minimum()) {
	}

	T min;
	T max;

public:
	bool HasStats() override {
		return min <= max;
	}

	string GetMin() override {
		return NumericLimits<SRC>::IsSigned() ? GetMinValue() : string();
	}
	string GetMax() override {
		return NumericLimits<SRC>::IsSigned() ? GetMaxValue() : string();
	}
	string GetMinValue() override {
		return HasStats() ? string(char_ptr_cast(&min), sizeof(T)) : string();
	}
	string GetMaxValue() override {
		return HasStats() ? string(char_ptr_cast(&max), sizeof(T)) : string();
	}
};

struct BaseParquetOperator {
	template <class SRC, class TGT>
	static void WriteToStream(const TGT &input, WriteStream &ser) {
		ser.WriteData(const_data_ptr_cast(&input), sizeof(TGT));
	}

	template <class SRC, class TGT>
	static uint64_t XXHash64(const TGT &target_value) {
		return duckdb_zstd::XXH64(&target_value, sizeof(target_value), 0);
	}

	template <class SRC, class TGT>
	static unique_ptr<ColumnWriterStatistics> InitializeStats() {
		return nullptr;
	}

	template <class SRC, class TGT>
	static void HandleStats(ColumnWriterStatistics *stats, TGT target_value) {
	}

	template <class SRC, class TGT>
	static idx_t GetRowSize(const Vector &, idx_t) {
		return sizeof(TGT);
	}
};

struct ParquetCastOperator : public BaseParquetOperator {
	template <class SRC, class TGT>
	static TGT Operation(SRC input) {
		return TGT(input);
	}
	template <class SRC, class TGT>
	static unique_ptr<ColumnWriterStatistics> InitializeStats() {
		return make_uniq<NumericStatisticsState<SRC, TGT, BaseParquetOperator>>();
	}

	template <class SRC, class TGT>
	static void HandleStats(ColumnWriterStatistics *stats, TGT target_value) {
		auto &numeric_stats = (NumericStatisticsState<SRC, TGT, BaseParquetOperator> &)*stats;
		if (LessThan::Operation(target_value, numeric_stats.min)) {
			numeric_stats.min = target_value;
		}
		if (GreaterThan::Operation(target_value, numeric_stats.max)) {
			numeric_stats.max = target_value;
		}
	}
};

struct ParquetTimestampNSOperator : public ParquetCastOperator {
	template <class SRC, class TGT>
	static TGT Operation(SRC input) {
		return TGT(input);
	}
};

struct ParquetTimestampSOperator : public ParquetCastOperator {
	template <class SRC, class TGT>
	static TGT Operation(SRC input) {
		return Timestamp::FromEpochSecondsPossiblyInfinite(input).value;
	}
};

class StringStatisticsState : public ColumnWriterStatistics {
	static constexpr const idx_t MAX_STRING_STATISTICS_SIZE = 10000;

public:
	StringStatisticsState() : has_stats(false), values_too_big(false), min(), max() {
	}

	bool has_stats;
	bool values_too_big;
	string min;
	string max;

public:
	bool HasStats() override {
		return has_stats;
	}

	void Update(const string_t &val) {
		if (values_too_big) {
			return;
		}
		auto str_len = val.GetSize();
		if (str_len > MAX_STRING_STATISTICS_SIZE) {
			// we avoid gathering stats when individual string values are too large
			// this is because the statistics are copied into the Parquet file meta data in uncompressed format
			// ideally we avoid placing several mega or giga-byte long strings there
			// we put a threshold of 10KB, if we see strings that exceed this threshold we avoid gathering stats
			values_too_big = true;
			has_stats = false;
			min = string();
			max = string();
			return;
		}
		if (!has_stats || LessThan::Operation(val, string_t(min))) {
			min = val.GetString();
		}
		if (!has_stats || GreaterThan::Operation(val, string_t(max))) {
			max = val.GetString();
		}
		has_stats = true;
	}

	string GetMin() override {
		return GetMinValue();
	}
	string GetMax() override {
		return GetMaxValue();
	}
	string GetMinValue() override {
		return HasStats() ? min : string();
	}
	string GetMaxValue() override {
		return HasStats() ? max : string();
	}
};

struct ParquetStringOperator : public BaseParquetOperator {
	template <class SRC, class TGT>
	static TGT Operation(SRC input) {
		return input;
	}

	template <class SRC, class TGT>
	static unique_ptr<ColumnWriterStatistics> InitializeStats() {
		return make_uniq<StringStatisticsState>();
	}

	template <class SRC, class TGT>
	static void HandleStats(ColumnWriterStatistics *stats, TGT target_value) {
		auto &string_stats = stats->Cast<StringStatisticsState>();
		string_stats.Update(target_value);
	}

	template <class SRC, class TGT>
	static void WriteToStream(const TGT &target_value, WriteStream &ser) {
		ser.Write<uint32_t>(target_value.GetSize());
		ser.WriteData(const_data_ptr_cast(target_value.GetData()), target_value.GetSize());
	}

	template <class SRC, class TGT>
	static uint64_t XXHash64(const TGT &target_value) {
		return duckdb_zstd::XXH64(target_value.GetData(), target_value.GetSize(), 0);
	}

	template <class SRC, class TGT>
	static idx_t GetRowSize(const Vector &vector, idx_t index) {
		return FlatVector::GetData<string_t>(vector)[index].GetSize();
	}
};

struct ParquetIntervalTargetType {
	static constexpr const idx_t PARQUET_INTERVAL_SIZE = 12;
	data_t bytes[PARQUET_INTERVAL_SIZE];
};

struct ParquetIntervalOperator : public BaseParquetOperator {
	template <class SRC, class TGT>
	static TGT Operation(SRC input) {

		if (input.days < 0 || input.months < 0 || input.micros < 0) {
			throw IOException("Parquet files do not support negative intervals");
		}
		TGT result;
		Store<uint32_t>(input.months, result.bytes);
		Store<uint32_t>(input.days, result.bytes + sizeof(uint32_t));
		Store<uint32_t>(input.micros / 1000, result.bytes + sizeof(uint32_t) * 2);
		return result;
	}

	template <class SRC, class TGT>
	static void WriteToStream(const TGT &target_value, WriteStream &ser) {
		ser.WriteData(target_value.bytes, ParquetIntervalTargetType::PARQUET_INTERVAL_SIZE);
	}

	template <class SRC, class TGT>
	static uint64_t XXHash64(const TGT &target_value) {
		return duckdb_zstd::XXH64(target_value.bytes, ParquetIntervalTargetType::PARQUET_INTERVAL_SIZE, 0);
	}
};

struct ParquetUUIDTargetType {
	static constexpr const idx_t PARQUET_UUID_SIZE = 16;
	data_t bytes[PARQUET_UUID_SIZE];
};

struct ParquetUUIDOperator : public BaseParquetOperator {
	template <class SRC, class TGT>
	static TGT Operation(SRC input) {
		TGT result;
		uint64_t high_bytes = input.upper ^ (int64_t(1) << 63);
		uint64_t low_bytes = input.lower;
		for (idx_t i = 0; i < sizeof(uint64_t); i++) {
			auto shift_count = (sizeof(uint64_t) - i - 1) * 8;
			result.bytes[i] = (high_bytes >> shift_count) & 0xFF;
		}
		for (idx_t i = 0; i < sizeof(uint64_t); i++) {
			auto shift_count = (sizeof(uint64_t) - i - 1) * 8;
			result.bytes[sizeof(uint64_t) + i] = (low_bytes >> shift_count) & 0xFF;
		}
		return result;
	}

	template <class SRC, class TGT>
	static void WriteToStream(const TGT &target_value, WriteStream &ser) {
		ser.WriteData(target_value.bytes, ParquetUUIDTargetType::PARQUET_UUID_SIZE);
	}

	template <class SRC, class TGT>
	static uint64_t XXHash64(const TGT &target_value) {
		return duckdb_zstd::XXH64(target_value.bytes, ParquetUUIDTargetType::PARQUET_UUID_SIZE, 0);
	}
};

struct ParquetTimeTZOperator : public BaseParquetOperator {
	template <class SRC, class TGT>
	static TGT Operation(SRC input) {
		return input.time().micros;
	}
};

struct ParquetHugeintOperator : public BaseParquetOperator {
	template <class SRC, class TGT>
	static TGT Operation(SRC input) {
		return Hugeint::Cast<double>(input);
	}

	template <class SRC, class TGT>
	static unique_ptr<ColumnWriterStatistics> InitializeStats() {
		return make_uniq<ColumnWriterStatistics>();
	}

	template <class SRC, class TGT>
	static void HandleStats(ColumnWriterStatistics *stats, TGT target_value) {
	}
};

struct ParquetUhugeintOperator : public BaseParquetOperator {
	template <class SRC, class TGT>
	static TGT Operation(SRC input) {
		return Uhugeint::Cast<double>(input);
	}

	template <class SRC, class TGT>
	static unique_ptr<ColumnWriterStatistics> InitializeStats() {
		return make_uniq<ColumnWriterStatistics>();
	}

	template <class SRC, class TGT>
	static void HandleStats(ColumnWriterStatistics *stats, TGT target_value) {
	}
};

template <class SRC, class TGT, class OP = ParquetCastOperator>
static void TemplatedWritePlain(Vector &col, ColumnWriterStatistics *stats, const idx_t chunk_start,
                                const idx_t chunk_end, const ValidityMask &mask, WriteStream &ser) {

	const auto *ptr = FlatVector::GetData<SRC>(col);
	for (idx_t r = chunk_start; r < chunk_end; r++) {
		if (!mask.RowIsValid(r)) {
			continue;
		}
		TGT target_value = OP::template Operation<SRC, TGT>(ptr[r]);
		OP::template HandleStats<SRC, TGT>(stats, target_value);
		OP::template WriteToStream<SRC, TGT>(target_value, ser);
	}
}

template <class T>
class StandardColumnWriterState : public PrimitiveColumnWriterState {
public:
	StandardColumnWriterState(duckdb_parquet::RowGroup &row_group, idx_t col_idx)
	    : PrimitiveColumnWriterState(row_group, col_idx) {
	}
	~StandardColumnWriterState() override = default;

	// analysis state for integer values for DELTA_BINARY_PACKED/DELTA_LENGTH_BYTE_ARRAY
	idx_t total_value_count = 0;
	idx_t total_string_size = 0;
	uint32_t key_bit_width = 0;

	unordered_map<T, uint32_t> dictionary;
	duckdb_parquet::Encoding::type encoding;
};

template <class SRC, class TGT>
class StandardWriterPageState : public ColumnWriterPageState {
public:
	explicit StandardWriterPageState(const idx_t total_value_count, const idx_t total_string_size,
	                                 Encoding::type encoding_p, const unordered_map<SRC, uint32_t> &dictionary_p)
	    : encoding(encoding_p), dbp_initialized(false), dbp_encoder(total_value_count), dlba_initialized(false),
	      dlba_encoder(total_value_count, total_string_size), bss_encoder(total_value_count, sizeof(TGT)),
	      dictionary(dictionary_p), dict_written_value(false),
	      dict_bit_width(RleBpDecoder::ComputeBitWidth(dictionary.size())), dict_encoder(dict_bit_width) {
	}
	duckdb_parquet::Encoding::type encoding;

	bool dbp_initialized;
	DbpEncoder dbp_encoder;

	bool dlba_initialized;
	DlbaEncoder dlba_encoder;

	BssEncoder bss_encoder;

	const unordered_map<SRC, uint32_t> &dictionary;
	bool dict_written_value;
	uint32_t dict_bit_width;
	RleBpEncoder dict_encoder;
};

namespace dbp_encoder {

template <class T>
void BeginWrite(DbpEncoder &encoder, WriteStream &writer, const T &first_value) {
	throw InternalException("Can't write type to DELTA_BINARY_PACKED column");
}

template <>
void BeginWrite(DbpEncoder &encoder, WriteStream &writer, const int64_t &first_value) {
	encoder.BeginWrite(writer, first_value);
}

template <>
void BeginWrite(DbpEncoder &encoder, WriteStream &writer, const int32_t &first_value) {
	BeginWrite(encoder, writer, UnsafeNumericCast<int64_t>(first_value));
}

template <>
void BeginWrite(DbpEncoder &encoder, WriteStream &writer, const uint64_t &first_value) {
	encoder.BeginWrite(writer, UnsafeNumericCast<int64_t>(first_value));
}

template <>
void BeginWrite(DbpEncoder &encoder, WriteStream &writer, const uint32_t &first_value) {
	BeginWrite(encoder, writer, UnsafeNumericCast<int64_t>(first_value));
}

template <class T>
void WriteValue(DbpEncoder &encoder, WriteStream &writer, const T &value) {
	throw InternalException("Can't write type to DELTA_BINARY_PACKED column");
}

template <>
void WriteValue(DbpEncoder &encoder, WriteStream &writer, const int64_t &value) {
	encoder.WriteValue(writer, value);
}

template <>
void WriteValue(DbpEncoder &encoder, WriteStream &writer, const int32_t &value) {
	WriteValue(encoder, writer, UnsafeNumericCast<int64_t>(value));
}

template <>
void WriteValue(DbpEncoder &encoder, WriteStream &writer, const uint64_t &value) {
	encoder.WriteValue(writer, UnsafeNumericCast<int64_t>(value));
}

template <>
void WriteValue(DbpEncoder &encoder, WriteStream &writer, const uint32_t &value) {
	WriteValue(encoder, writer, UnsafeNumericCast<int64_t>(value));
}

} // namespace dbp_encoder

namespace dlba_encoder {

template <class T>
void BeginWrite(DlbaEncoder &encoder, WriteStream &writer, const T &first_value) {
	throw InternalException("Can't write type to DELTA_LENGTH_BYTE_ARRAY column");
}

template <>
void BeginWrite(DlbaEncoder &encoder, WriteStream &writer, const string_t &first_value) {
	encoder.BeginWrite(writer, first_value);
}

template <class T>
void WriteValue(DlbaEncoder &encoder, WriteStream &writer, const T &value) {
	throw InternalException("Can't write type to DELTA_LENGTH_BYTE_ARRAY column");
}

template <>
void WriteValue(DlbaEncoder &encoder, WriteStream &writer, const string_t &value) {
	encoder.WriteValue(writer, value);
}

// helpers to get size from strings
template <class SRC>
static idx_t GetDlbaStringSize(const SRC &src_value) {
	return 0;
}

template <>
idx_t GetDlbaStringSize(const string_t &src_value) {
	return src_value.GetSize();
}

} // namespace dlba_encoder

namespace bss_encoder {

template <class T>
void WriteValue(BssEncoder &encoder, const T &value) {
	throw InternalException("Can't write type to BYTE_STREAM_SPLIT column");
}

template <>
void WriteValue(BssEncoder &encoder, const float &value) {
	encoder.WriteValue(value);
}

template <>
void WriteValue(BssEncoder &encoder, const double &value) {
	encoder.WriteValue(value);
}

} // namespace bss_encoder

template <class SRC, class TGT, class OP = ParquetCastOperator>
class StandardColumnWriter : public PrimitiveColumnWriter {
public:
	StandardColumnWriter(ParquetWriter &writer, idx_t schema_idx, vector<string> schema_path_p, // NOLINT
	                     idx_t max_repeat, idx_t max_define, bool can_have_nulls)
	    : PrimitiveColumnWriter(writer, schema_idx, std::move(schema_path_p), max_repeat, max_define, can_have_nulls) {
	}
	~StandardColumnWriter() override = default;

public:
	unique_ptr<ColumnWriterState> InitializeWriteState(duckdb_parquet::RowGroup &row_group) override {
		auto result = make_uniq<StandardColumnWriterState<SRC>>(row_group, row_group.columns.size());
		result->encoding = Encoding::RLE_DICTIONARY;
		RegisterToRowGroup(row_group);
		return std::move(result);
	}

	unique_ptr<ColumnWriterPageState> InitializePageState(PrimitiveColumnWriterState &state_p) override {
		auto &state = state_p.Cast<StandardColumnWriterState<SRC>>();

		auto result = make_uniq<StandardWriterPageState<SRC, TGT>>(state.total_value_count, state.total_string_size,
		                                                           state.encoding, state.dictionary);
		return std::move(result);
	}

	void FlushPageState(WriteStream &temp_writer, ColumnWriterPageState *state_p) override {
		auto &page_state = state_p->Cast<StandardWriterPageState<SRC, TGT>>();
		switch (page_state.encoding) {
		case Encoding::DELTA_BINARY_PACKED:
			if (!page_state.dbp_initialized) {
				dbp_encoder::BeginWrite<int64_t>(page_state.dbp_encoder, temp_writer, 0);
			}
			page_state.dbp_encoder.FinishWrite(temp_writer);
			break;
		case Encoding::RLE_DICTIONARY:
			D_ASSERT(page_state.dict_bit_width != 0);
			if (!page_state.dict_written_value) {
				// all values are null
				// just write the bit width
				temp_writer.Write<uint8_t>(page_state.dict_bit_width);
				return;
			}
			page_state.dict_encoder.FinishWrite(temp_writer);
			break;
		case Encoding::DELTA_LENGTH_BYTE_ARRAY:
			if (!page_state.dlba_initialized) {
				dlba_encoder::BeginWrite<string_t>(page_state.dlba_encoder, temp_writer, string_t(""));
			}
			page_state.dlba_encoder.FinishWrite(temp_writer);
			break;
		case Encoding::BYTE_STREAM_SPLIT:
			page_state.bss_encoder.FinishWrite(temp_writer);
			break;
		case Encoding::PLAIN:
			break;
		default:
			throw InternalException("Unknown encoding");
		}
	}

	Encoding::type GetEncoding(PrimitiveColumnWriterState &state_p) override {
		auto &state = state_p.Cast<StandardColumnWriterState<SRC>>();
		return state.encoding;
	}

	bool HasAnalyze() override {
		return true;
	}

	void Analyze(ColumnWriterState &state_p, ColumnWriterState *parent, Vector &vector, idx_t count) override {
		auto &state = state_p.Cast<StandardColumnWriterState<SRC>>();

		auto data_ptr = FlatVector::GetData<SRC>(vector);
		idx_t vector_index = 0;
		uint32_t new_value_index = state.dictionary.size();

		const bool check_parent_empty = parent && !parent->is_empty.empty();
		const idx_t parent_index = state.definition_levels.size();

		const idx_t vcount =
		    check_parent_empty ? parent->definition_levels.size() - state.definition_levels.size() : count;

		const auto &validity = FlatVector::Validity(vector);

		for (idx_t i = 0; i < vcount; i++) {
			if (check_parent_empty && parent->is_empty[parent_index + i]) {
				continue;
			}
			if (validity.RowIsValid(vector_index)) {
				const auto &src_value = data_ptr[vector_index];
				if (state.dictionary.size() <= writer.DictionarySizeLimit()) {
					if (state.dictionary.find(src_value) == state.dictionary.end()) {
						state.dictionary[src_value] = new_value_index;
						new_value_index++;
					}
				}
				state.total_value_count++;
				state.total_string_size += dlba_encoder::GetDlbaStringSize(src_value);
			}
			vector_index++;
		}
	}

	void FinalizeAnalyze(ColumnWriterState &state_p) override {
		const auto type = writer.GetType(schema_idx);

		auto &state = state_p.Cast<StandardColumnWriterState<SRC>>();
		if (state.dictionary.size() == 0 || state.dictionary.size() > writer.DictionarySizeLimit()) {
			if (writer.GetParquetVersion() == ParquetVersion::V1) {
				// Can't do the cool stuff for V1
				state.encoding = Encoding::PLAIN;
			} else {
				// If we aren't doing dictionary encoding, these encodings are virtually always better than PLAIN
				switch (type) {
				case Type::type::INT32:
				case Type::type::INT64:
					state.encoding = Encoding::DELTA_BINARY_PACKED;
					break;
				case Type::type::BYTE_ARRAY:
					state.encoding = Encoding::DELTA_LENGTH_BYTE_ARRAY;
					break;
				case Type::type::FLOAT:
				case Type::type::DOUBLE:
					state.encoding = Encoding::BYTE_STREAM_SPLIT;
					break;
				default:
					state.encoding = Encoding::PLAIN;
				}
			}
			state.dictionary.clear();
		} else {
			state.key_bit_width = RleBpDecoder::ComputeBitWidth(state.dictionary.size());
		}
	}

	unique_ptr<ColumnWriterStatistics> InitializeStatsState() override {
		return OP::template InitializeStats<SRC, TGT>();
	}

	bool HasDictionary(PrimitiveColumnWriterState &state_p) override {
		auto &state = state_p.Cast<StandardColumnWriterState<SRC>>();
		return state.encoding == Encoding::RLE_DICTIONARY;
	}

	idx_t DictionarySize(PrimitiveColumnWriterState &state_p) override {
		auto &state = state_p.Cast<StandardColumnWriterState<SRC>>();
		return state.dictionary.size();
	}

	void WriteVector(WriteStream &temp_writer, ColumnWriterStatistics *stats, ColumnWriterPageState *page_state_p,
	                 Vector &input_column, idx_t chunk_start, idx_t chunk_end) override {
		auto &page_state = page_state_p->Cast<StandardWriterPageState<SRC, TGT>>();

		const auto &mask = FlatVector::Validity(input_column);
		const auto *data_ptr = FlatVector::GetData<SRC>(input_column);

		switch (page_state.encoding) {
		case Encoding::RLE_DICTIONARY: {
			for (idx_t r = chunk_start; r < chunk_end; r++) {
				if (!mask.RowIsValid(r)) {
					continue;
				}
				auto &src_val = data_ptr[r];
				auto value_index = page_state.dictionary.at(src_val);
				if (!page_state.dict_written_value) {
					// first value
					// write the bit-width as a one-byte entry
					temp_writer.Write<uint8_t>(page_state.dict_bit_width);
					// now begin writing the actual value
					page_state.dict_encoder.BeginWrite(temp_writer, value_index);
					page_state.dict_written_value = true;
				} else {
					page_state.dict_encoder.WriteValue(temp_writer, value_index);
				}
			}
			break;
		}
		case Encoding::DELTA_BINARY_PACKED: {
			idx_t r = chunk_start;
			if (!page_state.dbp_initialized) {
				// find first non-null value
				for (; r < chunk_end; r++) {
					if (!mask.RowIsValid(r)) {
						continue;
					}
					const TGT target_value = OP::template Operation<SRC, TGT>(data_ptr[r]);
					OP::template HandleStats<SRC, TGT>(stats, target_value);
					dbp_encoder::BeginWrite(page_state.dbp_encoder, temp_writer, target_value);
					page_state.dbp_initialized = true;
					r++; // skip over
					break;
				}
			}

			for (; r < chunk_end; r++) {
				if (!mask.RowIsValid(r)) {
					continue;
				}
				const TGT target_value = OP::template Operation<SRC, TGT>(data_ptr[r]);
				OP::template HandleStats<SRC, TGT>(stats, target_value);
				dbp_encoder::WriteValue(page_state.dbp_encoder, temp_writer, target_value);
			}
			break;
		}
		case Encoding::DELTA_LENGTH_BYTE_ARRAY: {
			idx_t r = chunk_start;
			if (!page_state.dlba_initialized) {
				// find first non-null value
				for (; r < chunk_end; r++) {
					if (!mask.RowIsValid(r)) {
						continue;
					}
					const TGT target_value = OP::template Operation<SRC, TGT>(data_ptr[r]);
					OP::template HandleStats<SRC, TGT>(stats, target_value);
					dlba_encoder::BeginWrite(page_state.dlba_encoder, temp_writer, target_value);
					page_state.dlba_initialized = true;
					r++; // skip over
					break;
				}
			}

			for (; r < chunk_end; r++) {
				if (!mask.RowIsValid(r)) {
					continue;
				}
				const TGT target_value = OP::template Operation<SRC, TGT>(data_ptr[r]);
				OP::template HandleStats<SRC, TGT>(stats, target_value);
				dlba_encoder::WriteValue(page_state.dlba_encoder, temp_writer, target_value);
			}
			break;
		}
		case Encoding::BYTE_STREAM_SPLIT: {
			for (idx_t r = chunk_start; r < chunk_end; r++) {
				if (!mask.RowIsValid(r)) {
					continue;
				}
				const TGT target_value = OP::template Operation<SRC, TGT>(data_ptr[r]);
				OP::template HandleStats<SRC, TGT>(stats, target_value);
				bss_encoder::WriteValue(page_state.bss_encoder, target_value);
			}
			break;
		}
		case Encoding::PLAIN: {
			D_ASSERT(page_state.encoding == Encoding::PLAIN);
			TemplatedWritePlain<SRC, TGT, OP>(input_column, stats, chunk_start, chunk_end, mask, temp_writer);
			break;
		}
		default:
			throw InternalException("Unknown encoding");
		}
	}

	void FlushDictionary(PrimitiveColumnWriterState &state_p, ColumnWriterStatistics *stats) override {
		auto &state = state_p.Cast<StandardColumnWriterState<SRC>>();

		D_ASSERT(state.encoding == Encoding::RLE_DICTIONARY);

		// first we need to sort the values in index order
		auto values = vector<SRC>(state.dictionary.size());
		for (const auto &entry : state.dictionary) {
			values[entry.second] = entry.first;
		}

		state.bloom_filter =
		    make_uniq<ParquetBloomFilter>(state.dictionary.size(), writer.BloomFilterFalsePositiveRatio());

		// first write the contents of the dictionary page to a temporary buffer
		auto temp_writer = make_uniq<MemoryStream>(
		    Allocator::Get(writer.GetContext()), MaxValue<idx_t>(NextPowerOfTwo(state.dictionary.size() * sizeof(TGT)),
		                                                         MemoryStream::DEFAULT_INITIAL_CAPACITY));
		for (idx_t r = 0; r < values.size(); r++) {
			const TGT target_value = OP::template Operation<SRC, TGT>(values[r]);
			// update the statistics
			OP::template HandleStats<SRC, TGT>(stats, target_value);
			// update the bloom filter
			auto hash = OP::template XXHash64<SRC, TGT>(target_value);
			state.bloom_filter->FilterInsert(hash);
			// actually write the dictionary value
			OP::template WriteToStream<SRC, TGT>(target_value, *temp_writer);
		}
		// flush the dictionary page and add it to the to-be-written pages
		WriteDictionary(state, std::move(temp_writer), values.size());
		// bloom filter will be queued for writing in ParquetWriter::BufferBloomFilter one level up
	}

	idx_t GetRowSize(const Vector &vector, const idx_t index, const PrimitiveColumnWriterState &state_p) const override {
		auto &state = state_p.Cast<StandardColumnWriterState<SRC>>();
		if (state.encoding == Encoding::RLE_DICTIONARY) {
			return (state.key_bit_width + 7) / 8;
		} else {
			return OP::template GetRowSize<SRC, TGT>(vector, index);
		}
	}
};

//===--------------------------------------------------------------------===//
// Boolean Column Writer
//===--------------------------------------------------------------------===//
class BooleanStatisticsState : public ColumnWriterStatistics {
public:
	BooleanStatisticsState() : min(true), max(false) {
	}

	bool min;
	bool max;

public:
	bool HasStats() override {
		return !(min && !max);
	}

	string GetMin() override {
		return GetMinValue();
	}
	string GetMax() override {
		return GetMaxValue();
	}
	string GetMinValue() override {
		return HasStats() ? string(const_char_ptr_cast(&min), sizeof(bool)) : string();
	}
	string GetMaxValue() override {
		return HasStats() ? string(const_char_ptr_cast(&max), sizeof(bool)) : string();
	}
};

class BooleanWriterPageState : public ColumnWriterPageState {
public:
	uint8_t byte = 0;
	uint8_t byte_pos = 0;
};

class BooleanColumnWriter : public PrimitiveColumnWriter {
public:
	BooleanColumnWriter(ParquetWriter &writer, idx_t schema_idx, vector<string> schema_path_p, idx_t max_repeat,
	                    idx_t max_define, bool can_have_nulls)
	    : PrimitiveColumnWriter(writer, schema_idx, std::move(schema_path_p), max_repeat, max_define, can_have_nulls) {
	}
	~BooleanColumnWriter() override = default;

public:
	unique_ptr<ColumnWriterStatistics> InitializeStatsState() override {
		return make_uniq<BooleanStatisticsState>();
	}

	void WriteVector(WriteStream &temp_writer, ColumnWriterStatistics *stats_p, ColumnWriterPageState *state_p,
	                 Vector &input_column, idx_t chunk_start, idx_t chunk_end) override {
		auto &stats = stats_p->Cast<BooleanStatisticsState>();
		auto &state = state_p->Cast<BooleanWriterPageState>();
		auto &mask = FlatVector::Validity(input_column);

		auto *ptr = FlatVector::GetData<bool>(input_column);
		for (idx_t r = chunk_start; r < chunk_end; r++) {
			if (mask.RowIsValid(r)) {
				// only encode if non-null
				if (ptr[r]) {
					stats.max = true;
					state.byte |= 1 << state.byte_pos;
				} else {
					stats.min = false;
				}
				state.byte_pos++;

				if (state.byte_pos == 8) {
					temp_writer.Write<uint8_t>(state.byte);
					state.byte = 0;
					state.byte_pos = 0;
				}
			}
		}
	}

	unique_ptr<ColumnWriterPageState> InitializePageState(PrimitiveColumnWriterState &state) override {
		return make_uniq<BooleanWriterPageState>();
	}

	void FlushPageState(WriteStream &temp_writer, ColumnWriterPageState *state_p) override {
		auto &state = state_p->Cast<BooleanWriterPageState>();
		if (state.byte_pos > 0) {
			temp_writer.Write<uint8_t>(state.byte);
			state.byte = 0;
			state.byte_pos = 0;
		}
	}

	idx_t GetRowSize(const Vector &vector, const idx_t index, const PrimitiveColumnWriterState &state) const override {
		return sizeof(bool);
	}
};

//===--------------------------------------------------------------------===//
// Decimal Column Writer
//===--------------------------------------------------------------------===//
static void WriteParquetDecimal(hugeint_t input, data_ptr_t result) {
	bool positive = input >= 0;
	// numbers are stored as two's complement so some muckery is required
	if (!positive) {
		input = NumericLimits<hugeint_t>::Maximum() + input + 1;
	}
	uint64_t high_bytes = uint64_t(input.upper);
	uint64_t low_bytes = input.lower;

	for (idx_t i = 0; i < sizeof(uint64_t); i++) {
		auto shift_count = (sizeof(uint64_t) - i - 1) * 8;
		result[i] = (high_bytes >> shift_count) & 0xFF;
	}
	for (idx_t i = 0; i < sizeof(uint64_t); i++) {
		auto shift_count = (sizeof(uint64_t) - i - 1) * 8;
		result[sizeof(uint64_t) + i] = (low_bytes >> shift_count) & 0xFF;
	}
	if (!positive) {
		result[0] |= 0x80;
	}
}

class FixedDecimalStatistics : public ColumnWriterStatistics {
public:
	FixedDecimalStatistics() : min(NumericLimits<hugeint_t>::Maximum()), max(NumericLimits<hugeint_t>::Minimum()) {
	}

	hugeint_t min;
	hugeint_t max;

public:
	string GetStats(hugeint_t &input) {
		data_t buffer[16];
		WriteParquetDecimal(input, buffer);
		return string(const_char_ptr_cast(buffer), 16);
	}

	bool HasStats() override {
		return min <= max;
	}

	void Update(hugeint_t &val) {
		if (LessThan::Operation(val, min)) {
			min = val;
		}
		if (GreaterThan::Operation(val, max)) {
			max = val;
		}
	}

	string GetMin() override {
		return GetMinValue();
	}
	string GetMax() override {
		return GetMaxValue();
	}
	string GetMinValue() override {
		return HasStats() ? GetStats(min) : string();
	}
	string GetMaxValue() override {
		return HasStats() ? GetStats(max) : string();
	}
};

class FixedDecimalColumnWriter : public PrimitiveColumnWriter {
public:
	FixedDecimalColumnWriter(ParquetWriter &writer, idx_t schema_idx, vector<string> schema_path_p, idx_t max_repeat,
	                         idx_t max_define, bool can_have_nulls)
	    : PrimitiveColumnWriter(writer, schema_idx, std::move(schema_path_p), max_repeat, max_define, can_have_nulls) {
	}
	~FixedDecimalColumnWriter() override = default;

public:
	unique_ptr<ColumnWriterStatistics> InitializeStatsState() override {
		return make_uniq<FixedDecimalStatistics>();
	}

	void WriteVector(WriteStream &temp_writer, ColumnWriterStatistics *stats_p, ColumnWriterPageState *page_state,
	                 Vector &input_column, idx_t chunk_start, idx_t chunk_end) override {
		auto &mask = FlatVector::Validity(input_column);
		auto *ptr = FlatVector::GetData<hugeint_t>(input_column);
		auto &stats = stats_p->Cast<FixedDecimalStatistics>();

		data_t temp_buffer[16];
		for (idx_t r = chunk_start; r < chunk_end; r++) {
			if (mask.RowIsValid(r)) {
				stats.Update(ptr[r]);
				WriteParquetDecimal(ptr[r], temp_buffer);
				temp_writer.WriteData(temp_buffer, 16);
			}
		}
	}

	idx_t GetRowSize(const Vector &vector, const idx_t index, const PrimitiveColumnWriterState &state) const override {
		return sizeof(hugeint_t);
	}
};

//===--------------------------------------------------------------------===//
// WKB Column Writer
//===--------------------------------------------------------------------===//
// Used to store the metadata for a WKB-encoded geometry column when writing
// GeoParquet files.
class WKBColumnWriterState final : public StandardColumnWriterState<string_t> {
public:
	WKBColumnWriterState(ClientContext &context, duckdb_parquet::RowGroup &row_group, idx_t col_idx)
	    : StandardColumnWriterState(row_group, col_idx), geo_data(), geo_data_writer(context) {
	}

	GeoParquetColumnMetadata geo_data;
	GeoParquetColumnMetadataWriter geo_data_writer;
};

class WKBColumnWriter final : public StandardColumnWriter<string_t, string_t, ParquetStringOperator> {
public:
	WKBColumnWriter(ClientContext &context_p, ParquetWriter &writer, idx_t schema_idx, vector<string> schema_path_p,
	                idx_t max_repeat, idx_t max_define, bool can_have_nulls, string name)
	    : StandardColumnWriter(writer, schema_idx, std::move(schema_path_p), max_repeat, max_define, can_have_nulls),
	      column_name(std::move(name)), context(context_p) {

		this->writer.GetGeoParquetData().RegisterGeometryColumn(column_name);
	}

	unique_ptr<ColumnWriterState> InitializeWriteState(duckdb_parquet::RowGroup &row_group) override {
		auto result = make_uniq<WKBColumnWriterState>(context, row_group, row_group.columns.size());
		result->encoding = Encoding::RLE_DICTIONARY;
		RegisterToRowGroup(row_group);
		return std::move(result);
	}

	void Write(ColumnWriterState &state, Vector &vector, idx_t count) override {
		StandardColumnWriter::Write(state, vector, count);

		auto &geo_state = state.Cast<WKBColumnWriterState>();
		geo_state.geo_data_writer.Update(geo_state.geo_data, vector, count);
	}

	void FinalizeWrite(ColumnWriterState &state) override {
		StandardColumnWriter::FinalizeWrite(state);

		// Add the geodata object to the writer
		const auto &geo_state = state.Cast<WKBColumnWriterState>();

		// Merge this state's geo column data with the writer's geo column data
		writer.GetGeoParquetData().FlushColumnMeta(column_name, geo_state.geo_data);
	}

private:
	string column_name;
	ClientContext &context;
};

//===--------------------------------------------------------------------===//
// Enum Column Writer
//===--------------------------------------------------------------------===//
class EnumWriterPageState : public ColumnWriterPageState {
public:
	explicit EnumWriterPageState(uint32_t bit_width) : encoder(bit_width), written_value(false) {
	}

	RleBpEncoder encoder;
	bool written_value;
};

class EnumColumnWriter : public PrimitiveColumnWriter {
public:
	EnumColumnWriter(ParquetWriter &writer, LogicalType enum_type_p, idx_t schema_idx, vector<string> schema_path_p,
	                 idx_t max_repeat, idx_t max_define, bool can_have_nulls)
	    : PrimitiveColumnWriter(writer, schema_idx, std::move(schema_path_p), max_repeat, max_define, can_have_nulls),
	      enum_type(std::move(enum_type_p)) {
		bit_width = RleBpDecoder::ComputeBitWidth(EnumType::GetSize(enum_type));
	}
	~EnumColumnWriter() override = default;

	LogicalType enum_type;
	uint32_t bit_width;

public:
	unique_ptr<ColumnWriterStatistics> InitializeStatsState() override {
		return make_uniq<StringStatisticsState>();
	}

	template <class T>
	void WriteEnumInternal(WriteStream &temp_writer, Vector &input_column, idx_t chunk_start, idx_t chunk_end,
	                       EnumWriterPageState &page_state) {
		auto &mask = FlatVector::Validity(input_column);
		auto *ptr = FlatVector::GetData<T>(input_column);
		for (idx_t r = chunk_start; r < chunk_end; r++) {
			if (mask.RowIsValid(r)) {
				if (!page_state.written_value) {
					// first value
					// write the bit-width as a one-byte entry
					temp_writer.Write<uint8_t>(bit_width);
					// now begin writing the actual value
					page_state.encoder.BeginWrite(temp_writer, ptr[r]);
					page_state.written_value = true;
				} else {
					page_state.encoder.WriteValue(temp_writer, ptr[r]);
				}
			}
		}
	}

	void WriteVector(WriteStream &temp_writer, ColumnWriterStatistics *stats_p, ColumnWriterPageState *page_state_p,
	                 Vector &input_column, idx_t chunk_start, idx_t chunk_end) override {
		auto &page_state = page_state_p->Cast<EnumWriterPageState>();
		switch (enum_type.InternalType()) {
		case PhysicalType::UINT8:
			WriteEnumInternal<uint8_t>(temp_writer, input_column, chunk_start, chunk_end, page_state);
			break;
		case PhysicalType::UINT16:
			WriteEnumInternal<uint16_t>(temp_writer, input_column, chunk_start, chunk_end, page_state);
			break;
		case PhysicalType::UINT32:
			WriteEnumInternal<uint32_t>(temp_writer, input_column, chunk_start, chunk_end, page_state);
			break;
		default:
			throw InternalException("Unsupported internal enum type");
		}
	}

	unique_ptr<ColumnWriterPageState> InitializePageState(PrimitiveColumnWriterState &state) override {
		return make_uniq<EnumWriterPageState>(bit_width);
	}

	void FlushPageState(WriteStream &temp_writer, ColumnWriterPageState *state_p) override {
		auto &page_state = state_p->Cast<EnumWriterPageState>();
		if (!page_state.written_value) {
			// all values are null
			// just write the bit width
			temp_writer.Write<uint8_t>(bit_width);
			return;
		}
		page_state.encoder.FinishWrite(temp_writer);
	}

	duckdb_parquet::Encoding::type GetEncoding(PrimitiveColumnWriterState &state) override {
		return Encoding::RLE_DICTIONARY;
	}

	bool HasDictionary(PrimitiveColumnWriterState &state) override {
		return true;
	}

	idx_t DictionarySize(PrimitiveColumnWriterState &state_p) override {
		return EnumType::GetSize(enum_type);
	}

	void FlushDictionary(PrimitiveColumnWriterState &state, ColumnWriterStatistics *stats_p) override {
		auto &stats = stats_p->Cast<StringStatisticsState>();
		// write the enum values to a dictionary page
		auto &enum_values = EnumType::GetValuesInsertOrder(enum_type);
		auto enum_count = EnumType::GetSize(enum_type);
		auto string_values = FlatVector::GetData<string_t>(enum_values);
		// first write the contents of the dictionary page to a temporary buffer
		auto temp_writer = make_uniq<MemoryStream>(Allocator::Get(writer.GetContext()));
		for (idx_t r = 0; r < enum_count; r++) {
			D_ASSERT(!FlatVector::IsNull(enum_values, r));
			// update the statistics
			stats.Update(string_values[r]);
			// write this string value to the dictionary
			temp_writer->Write<uint32_t>(string_values[r].GetSize());
			temp_writer->WriteData(const_data_ptr_cast(string_values[r].GetData()), string_values[r].GetSize());
		}
		// flush the dictionary page and add it to the to-be-written pages
		WriteDictionary(state, std::move(temp_writer), enum_count);
	}

	idx_t GetRowSize(const Vector &vector, const idx_t index, const PrimitiveColumnWriterState &state) const override {
		return (bit_width + 7) / 8;
	}
};

// special double/float class to deal with dictionary encoding and NaN equality
struct double_na_equal {
	double_na_equal() : val(0) {
	}
	explicit double_na_equal(const double val_p) : val(val_p) {
	}
	// NOLINTNEXTLINE: allow implicit conversion to double
	operator double() const {
		return val;
	}

	bool operator==(const double &right) const {
		if (std::isnan(val) && std::isnan(right)) {
			return true;
		}
		return val == right;
	}
	double val;
};

struct float_na_equal {
	float_na_equal() : val(0) {
	}
	explicit float_na_equal(const float val_p) : val(val_p) {
	}
	// NOLINTNEXTLINE: allow implicit conversion to float
	operator float() const {
		return val;
	}
	bool operator==(const float &right) const {
		if (std::isnan(val) && std::isnan(right)) {
			return true;
		}
		return val == right;
	}
	float val;
};

//===--------------------------------------------------------------------===//
// Create Column Writer
//===--------------------------------------------------------------------===//

unique_ptr<ColumnWriter> ColumnWriter::CreateWriterRecursive(ClientContext &context,
                                                             vector<duckdb_parquet::SchemaElement> &schemas,
                                                             ParquetWriter &writer, const LogicalType &type,
                                                             const string &name, vector<string> schema_path,
                                                             optional_ptr<const ChildFieldIDs> field_ids,
                                                             idx_t max_repeat, idx_t max_define, bool can_have_nulls) {
	auto null_type = can_have_nulls ? FieldRepetitionType::OPTIONAL : FieldRepetitionType::REQUIRED;
	if (!can_have_nulls) {
		max_define--;
	}
	idx_t schema_idx = schemas.size();

	optional_ptr<const FieldID> field_id;
	optional_ptr<const ChildFieldIDs> child_field_ids;
	if (field_ids) {
		auto field_id_it = field_ids->ids->find(name);
		if (field_id_it != field_ids->ids->end()) {
			field_id = &field_id_it->second;
			child_field_ids = &field_id->child_field_ids;
		}
	}

	if (type.id() == LogicalTypeId::STRUCT || type.id() == LogicalTypeId::UNION) {
		auto &child_types = StructType::GetChildTypes(type);
		// set up the schema element for this struct
		duckdb_parquet::SchemaElement schema_element;
		schema_element.repetition_type = null_type;
		schema_element.num_children = UnsafeNumericCast<int32_t>(child_types.size());
		schema_element.__isset.num_children = true;
		schema_element.__isset.type = false;
		schema_element.__isset.repetition_type = true;
		schema_element.name = name;
		if (field_id && field_id->set) {
			schema_element.__isset.field_id = true;
			schema_element.field_id = field_id->field_id;
		}
		schemas.push_back(std::move(schema_element));
		schema_path.push_back(name);

		// construct the child types recursively
		vector<unique_ptr<ColumnWriter>> child_writers;
		child_writers.reserve(child_types.size());
		for (auto &child_type : child_types) {
			child_writers.push_back(CreateWriterRecursive(context, schemas, writer, child_type.second, child_type.first,
			                                              schema_path, child_field_ids, max_repeat, max_define + 1));
		}
		return make_uniq<StructColumnWriter>(writer, schema_idx, std::move(schema_path), max_repeat, max_define,
		                                     std::move(child_writers), can_have_nulls);
	}
	if (type.id() == LogicalTypeId::LIST || type.id() == LogicalTypeId::ARRAY) {
		auto is_list = type.id() == LogicalTypeId::LIST;
		auto &child_type = is_list ? ListType::GetChildType(type) : ArrayType::GetChildType(type);
		// set up the two schema elements for the list
		// for some reason we only set the converted type in the OPTIONAL element
		// first an OPTIONAL element
		duckdb_parquet::SchemaElement optional_element;
		optional_element.repetition_type = null_type;
		optional_element.num_children = 1;
		optional_element.converted_type = ConvertedType::LIST;
		optional_element.__isset.num_children = true;
		optional_element.__isset.type = false;
		optional_element.__isset.repetition_type = true;
		optional_element.__isset.converted_type = true;
		optional_element.name = name;
		if (field_id && field_id->set) {
			optional_element.__isset.field_id = true;
			optional_element.field_id = field_id->field_id;
		}
		schemas.push_back(std::move(optional_element));
		schema_path.push_back(name);

		// then a REPEATED element
		duckdb_parquet::SchemaElement repeated_element;
		repeated_element.repetition_type = FieldRepetitionType::REPEATED;
		repeated_element.num_children = 1;
		repeated_element.__isset.num_children = true;
		repeated_element.__isset.type = false;
		repeated_element.__isset.repetition_type = true;
		repeated_element.name = is_list ? "list" : "array";
		schemas.push_back(std::move(repeated_element));
		schema_path.emplace_back(is_list ? "list" : "array");

		auto child_writer = CreateWriterRecursive(context, schemas, writer, child_type, "element", schema_path,
		                                          child_field_ids, max_repeat + 1, max_define + 2);
		if (is_list) {
			return make_uniq<ListColumnWriter>(writer, schema_idx, std::move(schema_path), max_repeat, max_define,
			                                   std::move(child_writer), can_have_nulls);
		} else {
			return make_uniq<ArrayColumnWriter>(writer, schema_idx, std::move(schema_path), max_repeat, max_define,
			                                    std::move(child_writer), can_have_nulls);
		}
	}
	if (type.id() == LogicalTypeId::MAP) {
		// map type
		// maps are stored as follows:
		// <map-repetition> group <name> (MAP) {
		// 	repeated group key_value {
		// 		required <key-type> key;
		// 		<value-repetition> <value-type> value;
		// 	}
		// }
		// top map element
		duckdb_parquet::SchemaElement top_element;
		top_element.repetition_type = null_type;
		top_element.num_children = 1;
		top_element.converted_type = ConvertedType::MAP;
		top_element.__isset.repetition_type = true;
		top_element.__isset.num_children = true;
		top_element.__isset.converted_type = true;
		top_element.__isset.type = false;
		top_element.name = name;
		if (field_id && field_id->set) {
			top_element.__isset.field_id = true;
			top_element.field_id = field_id->field_id;
		}
		schemas.push_back(std::move(top_element));
		schema_path.push_back(name);

		// key_value element
		duckdb_parquet::SchemaElement kv_element;
		kv_element.repetition_type = FieldRepetitionType::REPEATED;
		kv_element.num_children = 2;
		kv_element.__isset.repetition_type = true;
		kv_element.__isset.num_children = true;
		kv_element.__isset.type = false;
		kv_element.name = "key_value";
		schemas.push_back(std::move(kv_element));
		schema_path.emplace_back("key_value");

		// construct the child types recursively
		vector<LogicalType> kv_types {MapType::KeyType(type), MapType::ValueType(type)};
		vector<string> kv_names {"key", "value"};
		vector<unique_ptr<ColumnWriter>> child_writers;
		child_writers.reserve(2);
		for (idx_t i = 0; i < 2; i++) {
			// key needs to be marked as REQUIRED
			bool is_key = i == 0;
			auto child_writer = CreateWriterRecursive(context, schemas, writer, kv_types[i], kv_names[i], schema_path,
			                                          child_field_ids, max_repeat + 1, max_define + 2, !is_key);

			child_writers.push_back(std::move(child_writer));
		}
		auto struct_writer = make_uniq<StructColumnWriter>(writer, schema_idx, schema_path, max_repeat, max_define,
		                                                   std::move(child_writers), can_have_nulls);
		return make_uniq<ListColumnWriter>(writer, schema_idx, schema_path, max_repeat, max_define,
		                                   std::move(struct_writer), can_have_nulls);
	}
	duckdb_parquet::SchemaElement schema_element;
	schema_element.type = ParquetWriter::DuckDBTypeToParquetType(type);
	schema_element.repetition_type = null_type;
	schema_element.__isset.num_children = false;
	schema_element.__isset.type = true;
	schema_element.__isset.repetition_type = true;
	schema_element.name = name;
	if (field_id && field_id->set) {
		schema_element.__isset.field_id = true;
		schema_element.field_id = field_id->field_id;
	}
	ParquetWriter::SetSchemaProperties(type, schema_element);
	schemas.push_back(std::move(schema_element));
	schema_path.push_back(name);
	if (type.id() == LogicalTypeId::BLOB && type.GetAlias() == "WKB_BLOB" &&
	    GeoParquetFileMetadata::IsGeoParquetConversionEnabled(context)) {
		return make_uniq<WKBColumnWriter>(context, writer, schema_idx, std::move(schema_path), max_repeat, max_define,
		                                  can_have_nulls, name);
	}

	switch (type.id()) {
	case LogicalTypeId::BOOLEAN:
		return make_uniq<BooleanColumnWriter>(writer, schema_idx, std::move(schema_path), max_repeat, max_define,
		                                      can_have_nulls);
	case LogicalTypeId::TINYINT:
		return make_uniq<StandardColumnWriter<int8_t, int32_t>>(writer, schema_idx, std::move(schema_path), max_repeat,
		                                                        max_define, can_have_nulls);
	case LogicalTypeId::SMALLINT:
		return make_uniq<StandardColumnWriter<int16_t, int32_t>>(writer, schema_idx, std::move(schema_path), max_repeat,
		                                                         max_define, can_have_nulls);
	case LogicalTypeId::INTEGER:
	case LogicalTypeId::DATE:
		return make_uniq<StandardColumnWriter<int32_t, int32_t>>(writer, schema_idx, std::move(schema_path), max_repeat,
		                                                         max_define, can_have_nulls);
	case LogicalTypeId::BIGINT:
	case LogicalTypeId::TIME:
	case LogicalTypeId::TIMESTAMP:
	case LogicalTypeId::TIMESTAMP_TZ:
	case LogicalTypeId::TIMESTAMP_MS:
		return make_uniq<StandardColumnWriter<int64_t, int64_t>>(writer, schema_idx, std::move(schema_path), max_repeat,
		                                                         max_define, can_have_nulls);
	case LogicalTypeId::TIME_TZ:
		return make_uniq<StandardColumnWriter<dtime_tz_t, int64_t, ParquetTimeTZOperator>>(
		    writer, schema_idx, std::move(schema_path), max_repeat, max_define, can_have_nulls);
	case LogicalTypeId::HUGEINT:
		return make_uniq<StandardColumnWriter<hugeint_t, double, ParquetHugeintOperator>>(
		    writer, schema_idx, std::move(schema_path), max_repeat, max_define, can_have_nulls);
	case LogicalTypeId::UHUGEINT:
		return make_uniq<StandardColumnWriter<uhugeint_t, double, ParquetUhugeintOperator>>(
		    writer, schema_idx, std::move(schema_path), max_repeat, max_define, can_have_nulls);
	case LogicalTypeId::TIMESTAMP_NS:
		return make_uniq<StandardColumnWriter<int64_t, int64_t, ParquetTimestampNSOperator>>(
		    writer, schema_idx, std::move(schema_path), max_repeat, max_define, can_have_nulls);
	case LogicalTypeId::TIMESTAMP_SEC:
		return make_uniq<StandardColumnWriter<int64_t, int64_t, ParquetTimestampSOperator>>(
		    writer, schema_idx, std::move(schema_path), max_repeat, max_define, can_have_nulls);
	case LogicalTypeId::UTINYINT:
		return make_uniq<StandardColumnWriter<uint8_t, int32_t>>(writer, schema_idx, std::move(schema_path), max_repeat,
		                                                         max_define, can_have_nulls);
	case LogicalTypeId::USMALLINT:
		return make_uniq<StandardColumnWriter<uint16_t, int32_t>>(writer, schema_idx, std::move(schema_path),
		                                                          max_repeat, max_define, can_have_nulls);
	case LogicalTypeId::UINTEGER:
		return make_uniq<StandardColumnWriter<uint32_t, uint32_t>>(writer, schema_idx, std::move(schema_path),
		                                                           max_repeat, max_define, can_have_nulls);
	case LogicalTypeId::UBIGINT:
		return make_uniq<StandardColumnWriter<uint64_t, uint64_t>>(writer, schema_idx, std::move(schema_path),
		                                                           max_repeat, max_define, can_have_nulls);
	case LogicalTypeId::FLOAT:
		return make_uniq<StandardColumnWriter<float_na_equal, float>>(writer, schema_idx, std::move(schema_path),
		                                                              max_repeat, max_define, can_have_nulls);
	case LogicalTypeId::DOUBLE:
		return make_uniq<StandardColumnWriter<double_na_equal, double>>(writer, schema_idx, std::move(schema_path),
		                                                                max_repeat, max_define, can_have_nulls);
	case LogicalTypeId::DECIMAL:
		switch (type.InternalType()) {
		case PhysicalType::INT16:
			return make_uniq<StandardColumnWriter<int16_t, int32_t>>(writer, schema_idx, std::move(schema_path),
			                                                         max_repeat, max_define, can_have_nulls);
		case PhysicalType::INT32:
			return make_uniq<StandardColumnWriter<int32_t, int32_t>>(writer, schema_idx, std::move(schema_path),
			                                                         max_repeat, max_define, can_have_nulls);
		case PhysicalType::INT64:
			return make_uniq<StandardColumnWriter<int64_t, int64_t>>(writer, schema_idx, std::move(schema_path),
			                                                         max_repeat, max_define, can_have_nulls);
		default:
			return make_uniq<FixedDecimalColumnWriter>(writer, schema_idx, std::move(schema_path), max_repeat,
			                                           max_define, can_have_nulls);
		}
	case LogicalTypeId::BLOB:
	case LogicalTypeId::VARCHAR:
		return make_uniq<StandardColumnWriter<string_t, string_t, ParquetStringOperator>>(
		    writer, schema_idx, std::move(schema_path), max_repeat, max_define, can_have_nulls);
	case LogicalTypeId::UUID:
		return make_uniq<StandardColumnWriter<hugeint_t, ParquetUUIDTargetType, ParquetUUIDOperator>>(
		    writer, schema_idx, std::move(schema_path), max_repeat, max_define, can_have_nulls);
	case LogicalTypeId::INTERVAL:
		return make_uniq<StandardColumnWriter<interval_t, ParquetIntervalTargetType, ParquetIntervalOperator>>(
		    writer, schema_idx, std::move(schema_path), max_repeat, max_define, can_have_nulls);
	case LogicalTypeId::ENUM:
		return make_uniq<EnumColumnWriter>(writer, type, schema_idx, std::move(schema_path), max_repeat, max_define,
		                                   can_have_nulls);
	default:
		throw InternalException("Unsupported type \"%s\" in Parquet writer", type.ToString());
	}
}

template <>
struct NumericLimits<float_na_equal> {
	static constexpr float Minimum() {
		return std::numeric_limits<float>::lowest();
	};
	static constexpr float Maximum() {
		return std::numeric_limits<float>::max();
	};
	static constexpr bool IsSigned() {
		return std::is_signed<float>::value;
	}
	static constexpr bool IsIntegral() {
		return std::is_integral<float>::value;
	}
};

template <>
struct NumericLimits<double_na_equal> {
	static constexpr double Minimum() {
		return std::numeric_limits<double>::lowest();
	};
	static constexpr double Maximum() {
		return std::numeric_limits<double>::max();
	};
	static constexpr bool IsSigned() {
		return std::is_signed<double>::value;
	}
	static constexpr bool IsIntegral() {
		return std::is_integral<double>::value;
	}
};

} // namespace duckdb

namespace std {
template <>
struct hash<duckdb::ParquetIntervalTargetType> {
	size_t operator()(const duckdb::ParquetIntervalTargetType &val) const {
		return duckdb::Hash(duckdb::const_char_ptr_cast(val.bytes),
		                    duckdb::ParquetIntervalTargetType::PARQUET_INTERVAL_SIZE);
	}
};

template <>
struct hash<duckdb::ParquetUUIDTargetType> {
	size_t operator()(const duckdb::ParquetUUIDTargetType &val) const {
		return duckdb::Hash(duckdb::const_char_ptr_cast(val.bytes), duckdb::ParquetUUIDTargetType::PARQUET_UUID_SIZE);
	}
};

template <>
struct hash<duckdb::float_na_equal> {
	size_t operator()(const duckdb::float_na_equal &val) const {
		if (std::isnan(val.val)) {
			return duckdb::Hash<float>(std::numeric_limits<float>::quiet_NaN());
		}
		return duckdb::Hash<float>(val.val);
	}
};

template <>
struct hash<duckdb::double_na_equal> {
	inline size_t operator()(const duckdb::double_na_equal &val) const {
		if (std::isnan(val.val)) {
			return duckdb::Hash<double>(std::numeric_limits<double>::quiet_NaN());
		}
		return duckdb::Hash<double>(val.val);
	}
};
} // namespace std
