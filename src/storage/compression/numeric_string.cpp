#include "duckdb/common/bitpacking.hpp"
#include "duckdb/common/types/cast_helpers.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "duckdb/common/vector/flat_vector.hpp"
#include "duckdb/common/vector/string_vector.hpp"
#include "duckdb/function/compression/compression.hpp"
#include "duckdb/storage/compression/compression_segment_reader.hpp"
#include "duckdb/storage/compression/standard_compression_state.hpp"
#include "duckdb/storage/segment/uncompressed.hpp"
#include "duckdb/storage/storage_manager.hpp"
#include "duckdb/storage/table/column_data.hpp"
#include "duckdb/storage/table/column_data_checkpointer.hpp"
#include "duckdb/storage/table/column_segment.hpp"
#include "duckdb/storage/table/scan_state.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// NumericString compression
//
// Stores VARCHAR values that are decimal numbers ("42", "-17", "123.45", "00123") as a bitpacked integer mantissa plus
// a (usually constant) decimal scale instead of as strings. Values are packed in groups of GROUP_SIZE rows using
// frame-of-reference bitpacking. Two modes exist:
// - CANONICAL: every value is the shortest decimal rendering of (mantissa, scale): optional '-', no leading zeros.
//   The scale can vary per value ("1.5" and "2.25" in the same segment are fine).
// - FIXED_WIDTH: every value has the same length and the same scale and no sign. Leading zeros are allowed ("00123",
//   "01.50"). This covers zero-padded identifiers such as postal codes.
// Values are formatted back into strings on scan.
//
// Segment layout:
//   SegmentHeader
//   MantissaGroupMeta[group_count]
//   ScaleGroupMeta[group_count]
//   packed group data (8-byte aligned start), mantissa bits followed by scale bits per group
//===--------------------------------------------------------------------===//
namespace numeric_string {

static constexpr idx_t GROUP_SIZE = 1024;
//! 18 digits always fit in an int64_t mantissa
static constexpr idx_t MAX_DIGITS = 18;
//! Longest accepted string (sign, digits, decimal point and zero padding); bounds the formatting buffer
static constexpr idx_t MAX_LENGTH = 48;
//! Rough size of an FSST-compressed dictionary entry relative to the raw digits; used to model what dictionary
//! compression would need for the same data
static constexpr double DICTIONARY_ENTRY_RATIO = 0.5;

enum class NumericStringMode : uint8_t { CANONICAL = 0, FIXED_WIDTH = 1 };

struct SegmentHeader {
	uint8_t mode;
	uint8_t fixed_width;
	uint8_t fixed_scale;
	uint8_t reserved;
	uint32_t group_count;
	uint32_t mantissa_meta_offset;
	uint32_t scale_meta_offset;
	uint32_t data_offset;
	uint32_t reserved2;
};
static_assert(sizeof(SegmentHeader) == 24, "unexpected NumericString segment header size");

struct MantissaGroupMeta {
	int64_t base;
	//! Offset of the packed bits relative to SegmentHeader::data_offset
	uint32_t data_offset;
	uint8_t width;
	uint8_t reserved[3];
};
static_assert(sizeof(MantissaGroupMeta) == 16, "unexpected NumericString mantissa meta size");

struct ScaleGroupMeta {
	//! Offset of the packed bits relative to SegmentHeader::data_offset
	uint32_t data_offset;
	uint8_t base;
	uint8_t width;
	uint8_t reserved[2];
};
static_assert(sizeof(ScaleGroupMeta) == 8, "unexpected NumericString scale meta size");

struct ParsedNumber {
	int64_t mantissa;
	uint8_t scale;
	uint8_t length;
	bool negative;
	bool leading_zero;
};

//! Parses "-?[0-9]+(\.[0-9]+)?" with at most MAX_DIGITS digits. "-0" (and "-0.0", ...) is rejected because it does
//! not round-trip through the mantissa.
static bool ParseNumericString(const char *data, idx_t len, ParsedNumber &result) {
	if (len == 0 || len > MAX_LENGTH) {
		return false;
	}
	idx_t pos = 0;
	bool negative = false;
	if (data[0] == '-') {
		negative = true;
		pos = 1;
	}
	if (pos >= len || data[pos] < '0' || data[pos] > '9') {
		return false;
	}
	bool leading_zero = data[pos] == '0' && pos + 1 < len && data[pos + 1] >= '0' && data[pos + 1] <= '9';
	uint64_t mantissa = 0;
	idx_t digits = 0;
	idx_t scale = 0;
	bool seen_dot = false;
	for (; pos < len; pos++) {
		auto c = data[pos];
		if (c >= '0' && c <= '9') {
			// only significant digits count towards the mantissa limit: "0.000000000000000000001" is fine
			if ((mantissa != 0 || c != '0') && ++digits > MAX_DIGITS) {
				return false;
			}
			mantissa = mantissa * 10 + UnsafeNumericCast<uint64_t>(c - '0');
			if (seen_dot) {
				scale++;
			}
		} else if (c == '.') {
			if (seen_dot || pos + 1 >= len) {
				return false;
			}
			seen_dot = true;
		} else {
			return false;
		}
	}
	if (negative && mantissa == 0) {
		return false;
	}
	result.mantissa = negative ? -UnsafeNumericCast<int64_t>(mantissa) : UnsafeNumericCast<int64_t>(mantissa);
	result.scale = UnsafeNumericCast<uint8_t>(scale);
	result.length = UnsafeNumericCast<uint8_t>(len);
	result.negative = negative;
	result.leading_zero = leading_zero;
	return true;
}

//! Largest buffer any formatted value needs: sign + 18 digits + '.' + padding zeros
static constexpr idx_t FORMAT_BUFFER_SIZE = 64;
static_assert(FORMAT_BUFFER_SIZE > MAX_LENGTH, "formatted values must fit the format buffer");

static inline uint64_t AbsoluteMantissa(int64_t mantissa) {
	return mantissa < 0 ? UnsafeNumericCast<uint64_t>(-(mantissa + 1)) + 1 : UnsafeNumericCast<uint64_t>(mantissa);
}

//! Formats a canonical value into out, returns the length
static idx_t FormatCanonical(int64_t mantissa, uint8_t scale, char *out) {
	char digits[24];
	auto end = digits + sizeof(digits);
	auto ptr = NumericHelper::FormatUnsigned<uint64_t>(AbsoluteMantissa(mantissa), end);
	auto digit_count = UnsafeNumericCast<idx_t>(end - ptr);
	auto o = out;
	if (mantissa < 0) {
		*o++ = '-';
	}
	if (scale == 0) {
		memcpy(o, ptr, digit_count);
		return UnsafeNumericCast<idx_t>(o + digit_count - out);
	}
	idx_t int_digits = digit_count > scale ? digit_count - scale : 0;
	if (int_digits == 0) {
		*o++ = '0';
	} else {
		memcpy(o, ptr, int_digits);
		o += int_digits;
	}
	*o++ = '.';
	idx_t frac_digits = digit_count - int_digits;
	for (idx_t i = frac_digits; i < scale; i++) {
		*o++ = '0';
	}
	memcpy(o, ptr + int_digits, frac_digits);
	o += frac_digits;
	return UnsafeNumericCast<idx_t>(o - out);
}

[[noreturn]] static void ThrowCorrupt(const char *message) {
	throw DataCorruptionException("Corrupted NumericString segment: %s", message);
}

//! Formats a fixed-width value into out, returns the length (always == width)
static idx_t FormatFixedWidth(int64_t mantissa, uint8_t width, uint8_t scale, char *out) {
	char digits[24];
	auto end = digits + sizeof(digits);
	auto ptr = NumericHelper::FormatUnsigned<uint64_t>(AbsoluteMantissa(mantissa), end);
	auto digit_count = UnsafeNumericCast<idx_t>(end - ptr);
	idx_t total_digits = scale > 0 ? idx_t(width) - 1 : idx_t(width);
	if (mantissa < 0 || digit_count > total_digits || total_digits < scale || (scale > 0 && total_digits == scale)) {
		ThrowCorrupt("fixed-width value does not fit its width");
	}
	auto o = out;
	idx_t int_digits = total_digits - scale;
	// zero-padded digit string
	char padded[24];
	idx_t pad = total_digits - digit_count;
	memset(padded, '0', pad);
	memcpy(padded + pad, ptr, digit_count);
	memcpy(o, padded, int_digits);
	o += int_digits;
	if (scale > 0) {
		*o++ = '.';
		memcpy(o, padded + int_digits, scale);
		o += scale;
	}
	return UnsafeNumericCast<idx_t>(o - out);
}

static inline idx_t PackedBytes(idx_t count, bitpacking_width_t width) {
	return BitpackingPrimitives::GetRequiredSize(count, width);
}

//===--------------------------------------------------------------------===//
// Analyze
//===--------------------------------------------------------------------===//
struct NumericStringAnalyzeState : public AnalyzeState {
	explicit NumericStringAnalyzeState(BlockManager &block_manager) : AnalyzeState(block_manager) {
	}

	bool canonical_ok = true;
	bool fixed_ok = true;
	bool have_fixed = false;
	uint8_t fixed_width = 0;
	uint8_t fixed_scale = 0;

	idx_t total_count = 0;
	idx_t null_count = 0;
	idx_t estimated_bytes = 0;
	//! distinct (mantissa, scale) pairs and total string bytes: used to model the dictionary alternative
	unordered_set<uint64_t> distinct_values;
	idx_t total_string_bytes = 0;

	// the group currently being analyzed
	idx_t group_rows = 0;
	bool group_has_value = false;
	int64_t group_min = 0;
	int64_t group_max = 0;
	uint8_t scale_min = 0;
	uint8_t scale_max = 0;

	void CloseGroup() {
		if (group_rows == 0) {
			return;
		}
		bitpacking_width_t mantissa_width = 0;
		bitpacking_width_t scale_width = 0;
		if (group_has_value) {
			mantissa_width = BitpackingPrimitives::MinimumBitWidth<uint64_t>(UnsafeNumericCast<uint64_t>(group_max) -
			                                                                 UnsafeNumericCast<uint64_t>(group_min));
			scale_width = BitpackingPrimitives::MinimumBitWidth<uint32_t>(uint32_t(scale_max) - uint32_t(scale_min));
		}
		estimated_bytes += PackedBytes(group_rows, mantissa_width) + PackedBytes(group_rows, scale_width) +
		                   sizeof(MantissaGroupMeta) + sizeof(ScaleGroupMeta);
		group_rows = 0;
		group_has_value = false;
	}

	void AddRow() {
		total_count++;
		if (++group_rows == GROUP_SIZE) {
			CloseGroup();
		}
	}

	void AddNull() {
		null_count++;
		AddRow();
	}

	void AddValue(const ParsedNumber &number) {
		distinct_values.insert(UnsafeNumericCast<uint64_t>(number.mantissa) ^ (uint64_t(number.scale) << 56));
		total_string_bytes += number.length;
		if (!group_has_value) {
			group_min = group_max = number.mantissa;
			scale_min = scale_max = number.scale;
			group_has_value = true;
		} else {
			group_min = MinValue(group_min, number.mantissa);
			group_max = MaxValue(group_max, number.mantissa);
			scale_min = MinValue(scale_min, number.scale);
			scale_max = MaxValue(scale_max, number.scale);
		}
		AddRow();
	}

	//! Returns false if the value is not representable in any mode
	bool Accept(const ParsedNumber &number) {
		if (number.leading_zero) {
			canonical_ok = false;
		}
		if (number.negative) {
			fixed_ok = false;
		} else if (!have_fixed) {
			have_fixed = true;
			fixed_width = number.length;
			fixed_scale = number.scale;
		} else if (number.length != fixed_width || number.scale != fixed_scale) {
			fixed_ok = false;
		}
		return canonical_ok || fixed_ok;
	}
};

static unique_ptr<AnalyzeState> InitAnalyze(ColumnData &col_data, PhysicalType type) {
	auto &storage_manager = col_data.GetStorageManager();
	if (!StorageManager::TargetAtLeastVersion(StorageVersion::V2_1_0, storage_manager.GetStorageVersion())) {
		// not supported by the target storage version
		return nullptr;
	}
	return make_uniq<NumericStringAnalyzeState>(col_data.GetBlockManager());
}

static bool Analyze(AnalyzeState &state_p, const Vector &input) {
	auto &state = state_p.Cast<NumericStringAnalyzeState>();
	UnifiedVectorFormat vdata;
	input.ToUnifiedFormat(vdata);
	auto data = UnifiedVectorFormat::GetData<string_t>(vdata);
	auto count = input.size();
	for (idx_t i = 0; i < count; i++) {
		auto idx = vdata.sel->get_index(i);
		if (!vdata.validity.RowIsValid(idx)) {
			state.AddNull();
			continue;
		}
		ParsedNumber number;
		if (!ParseNumericString(data[idx].GetData(), data[idx].GetSize(), number)) {
			return false;
		}
		if (!state.Accept(number)) {
			return false;
		}
		state.AddValue(number);
	}
	return true;
}

static idx_t FinalAnalyze(AnalyzeState &state_p) {
	auto &state = state_p.Cast<NumericStringAnalyzeState>();
	if (state.total_count == 0) {
		return NumericLimits<idx_t>::Maximum();
	}
	state.CloseGroup();
	// Model what dictionary compression would need for the same data: a bitpacked index per row plus the
	// (FSST-compressed) distinct strings. Columns with few distinct values relative to their numeric range (postal
	// codes, keys repeated in runs) are stored at least as compactly by the dictionary and scan faster from it
	// (dictionary vectors instead of formatting every row), so leave those to the dictionary methods.
	auto distinct_count = state.distinct_values.size();
	auto index_width = BitpackingPrimitives::MinimumBitWidth<uint64_t>(distinct_count);
	auto dictionary_estimate = double(state.total_count) * index_width / 8 +
	                           double(state.total_string_bytes) * DICTIONARY_ENTRY_RATIO * double(distinct_count) /
	                               double(MaxValue<idx_t>(state.total_count - state.null_count, 1));
	if (dictionary_estimate <= double(state.estimated_bytes)) {
		return NumericLimits<idx_t>::Maximum();
	}
	auto segments = state.estimated_bytes / state.info.GetBlockSize() + 1;
	return state.estimated_bytes + segments * sizeof(SegmentHeader);
}

//===--------------------------------------------------------------------===//
// Compress
//===--------------------------------------------------------------------===//
class NumericStringCompressionState : public StandardCompressionState {
public:
	NumericStringCompressionState(ColumnDataCheckpointData &checkpoint_data, NumericStringAnalyzeState &analyze_state)
	    : StandardCompressionState(checkpoint_data, CompressionType::COMPRESSION_NUMERIC_STRING),
	      stats_writer(checkpoint_data.GetType()) {
		if (analyze_state.canonical_ok) {
			mode = NumericStringMode::CANONICAL;
		} else {
			D_ASSERT(analyze_state.fixed_ok);
			mode = NumericStringMode::FIXED_WIDTH;
			fixed_width = analyze_state.fixed_width;
			fixed_scale = analyze_state.fixed_scale;
		}
		CreateAndPinNewSegment();
	}

	void Append(UnifiedVectorFormat &vdata, idx_t count) {
		auto data = UnifiedVectorFormat::GetData<string_t>(vdata);
		for (idx_t i = 0; i < count; i++) {
			auto idx = vdata.sel->get_index(i);
			if (!vdata.validity.RowIsValid(idx)) {
				stats_writer.SetHasNull();
				pending_valid[pending_count] = false;
				pending_mantissa[pending_count] = 0;
				pending_scale[pending_count] = 0;
			} else {
				ParsedNumber number;
				if (!ParseNumericString(data[idx].GetData(), data[idx].GetSize(), number)) {
					throw InternalException("NumericString compression: value rejected during compression that was "
					                        "accepted during analysis");
				}
				stats_writer.Update(data[idx]);
				pending_valid[pending_count] = true;
				pending_mantissa[pending_count] = number.mantissa;
				pending_scale[pending_count] = number.scale;
			}
			if (++pending_count == GROUP_SIZE) {
				FlushGroup();
			}
		}
	}

	void Finalize() {
		FlushGroup();
		FlushSegment(true);
	}

private:
	idx_t SegmentSize(idx_t group_count, idx_t packed_bytes) const {
		auto meta_end = sizeof(SegmentHeader) + group_count * (sizeof(MantissaGroupMeta) + sizeof(ScaleGroupMeta));
		return AlignValue<idx_t, 8>(meta_end) + packed_bytes;
	}

	void FlushGroup() {
		if (pending_count == 0) {
			return;
		}
		// frame of reference for the mantissas and the scales
		int64_t mantissa_min = 0, mantissa_max = 0;
		uint8_t scale_min = 0, scale_max = 0;
		bool has_value = false;
		for (idx_t i = 0; i < pending_count; i++) {
			if (!pending_valid[i]) {
				continue;
			}
			if (!has_value) {
				mantissa_min = mantissa_max = pending_mantissa[i];
				scale_min = scale_max = pending_scale[i];
				has_value = true;
			} else {
				mantissa_min = MinValue(mantissa_min, pending_mantissa[i]);
				mantissa_max = MaxValue(mantissa_max, pending_mantissa[i]);
				scale_min = MinValue(scale_min, pending_scale[i]);
				scale_max = MaxValue(scale_max, pending_scale[i]);
			}
		}
		bitpacking_width_t mantissa_width = 0;
		bitpacking_width_t scale_width = 0;
		if (has_value) {
			mantissa_width = BitpackingPrimitives::MinimumBitWidth<uint64_t>(UnsafeNumericCast<uint64_t>(mantissa_max) -
			                                                                 UnsafeNumericCast<uint64_t>(mantissa_min));
			scale_width = BitpackingPrimitives::MinimumBitWidth<uint32_t>(uint32_t(scale_max) - uint32_t(scale_min));
		}
		auto mantissa_bytes = PackedBytes(pending_count, mantissa_width);
		auto scale_bytes = PackedBytes(pending_count, scale_width);

		// does the group fit in the current segment?
		auto new_group_count = mantissa_meta.size() + 1;
		if (SegmentSize(new_group_count, packed.size() + mantissa_bytes + scale_bytes) > info.GetBlockSize()) {
			FlushSegment(false);
		}

		// NULL rows store the frame of reference so they do not widen the group
		for (idx_t i = 0; i < pending_count; i++) {
			if (pending_valid[i]) {
				delta_buffer[i] =
				    UnsafeNumericCast<uint64_t>(pending_mantissa[i]) - UnsafeNumericCast<uint64_t>(mantissa_min);
				scale_buffer[i] = uint32_t(pending_scale[i]) - uint32_t(scale_min);
			} else {
				delta_buffer[i] = 0;
				scale_buffer[i] = 0;
			}
		}

		MantissaGroupMeta mantissa_entry;
		memset(&mantissa_entry, 0, sizeof(mantissa_entry));
		mantissa_entry.base = mantissa_min;
		mantissa_entry.width = mantissa_width;
		mantissa_entry.data_offset = UnsafeNumericCast<uint32_t>(packed.size());
		AppendPacked(mantissa_bytes, [&](data_ptr_t dst) {
			BitpackingPrimitives::PackBuffer<uint64_t>(dst, delta_buffer, pending_count, mantissa_width);
		});

		ScaleGroupMeta scale_entry;
		memset(&scale_entry, 0, sizeof(scale_entry));
		scale_entry.base = scale_min;
		scale_entry.width = scale_width;
		scale_entry.data_offset = UnsafeNumericCast<uint32_t>(packed.size());
		AppendPacked(scale_bytes, [&](data_ptr_t dst) {
			BitpackingPrimitives::PackBuffer<uint32_t>(dst, scale_buffer, pending_count, scale_width);
		});

		mantissa_meta.push_back(mantissa_entry);
		scale_meta.push_back(scale_entry);
		segment_rows += pending_count;
		pending_count = 0;
	}

	template <class FUN>
	void AppendPacked(idx_t bytes, FUN &&pack) {
		if (bytes == 0) {
			return;
		}
		auto aligned = AlignValue<idx_t, 8>(bytes);
		auto offset = packed.size();
		packed.resize(offset + aligned, 0);
		pack(packed.data() + offset);
	}

	void FlushSegment(bool final) {
		if (segment_rows == 0) {
			if (final) {
				// nothing was ever written to the segment that was created last
				current_segment.reset();
			}
			return;
		}
		auto group_count = mantissa_meta.size();
		SegmentHeader header;
		memset(&header, 0, sizeof(header));
		header.mode = static_cast<uint8_t>(mode);
		header.fixed_width = fixed_width;
		header.fixed_scale = fixed_scale;
		header.group_count = UnsafeNumericCast<uint32_t>(group_count);
		header.mantissa_meta_offset = sizeof(SegmentHeader);
		header.scale_meta_offset =
		    UnsafeNumericCast<uint32_t>(header.mantissa_meta_offset + group_count * sizeof(MantissaGroupMeta));
		header.data_offset = UnsafeNumericCast<uint32_t>(
		    AlignValue<idx_t, 8>(header.scale_meta_offset + group_count * sizeof(ScaleGroupMeta)));
		auto total_size = header.data_offset + packed.size();
		D_ASSERT(total_size == SegmentSize(group_count, packed.size()));
		D_ASSERT(total_size <= info.GetBlockSize());

		auto base_ptr = handle.GetDataMutable();
		memcpy(base_ptr, &header, sizeof(header));
		memcpy(base_ptr + header.mantissa_meta_offset, mantissa_meta.data(), group_count * sizeof(MantissaGroupMeta));
		memcpy(base_ptr + header.scale_meta_offset, scale_meta.data(), group_count * sizeof(ScaleGroupMeta));
		// zero the alignment padding
		memset(base_ptr + header.scale_meta_offset + group_count * sizeof(ScaleGroupMeta), 0,
		       header.data_offset - (header.scale_meta_offset + group_count * sizeof(ScaleGroupMeta)));
		memcpy(base_ptr + header.data_offset, packed.data(), packed.size());

		current_segment->count = segment_rows;
		FlushCurrentSegment(stats_writer, total_size);

		mantissa_meta.clear();
		scale_meta.clear();
		packed.clear();
		segment_rows = 0;
		if (!final) {
			CreateAndPinNewSegment();
		}
	}

public:
	NumericStringMode mode = NumericStringMode::CANONICAL;
	uint8_t fixed_width = 0;
	uint8_t fixed_scale = 0;
	StatsWriter<string_t> stats_writer;

	//! rows of the group that is being collected
	idx_t pending_count = 0;
	int64_t pending_mantissa[GROUP_SIZE];
	uint8_t pending_scale[GROUP_SIZE];
	bool pending_valid[GROUP_SIZE];
	uint64_t delta_buffer[GROUP_SIZE];
	uint32_t scale_buffer[GROUP_SIZE];

	//! groups of the current segment
	vector<MantissaGroupMeta> mantissa_meta;
	vector<ScaleGroupMeta> scale_meta;
	vector<data_t> packed;
	idx_t segment_rows = 0;
};

static unique_ptr<CompressionState> InitCompression(ColumnDataCheckpointData &checkpoint_data,
                                                    unique_ptr<AnalyzeState> state) {
	return make_uniq<NumericStringCompressionState>(checkpoint_data, state->Cast<NumericStringAnalyzeState>());
}

static void Compress(CompressionState &state_p, const Vector &scan_vector) {
	auto &state = state_p.Cast<NumericStringCompressionState>();
	UnifiedVectorFormat vdata;
	scan_vector.ToUnifiedFormat(vdata);
	state.Append(vdata, scan_vector.size());
}

static void FinalizeCompress(CompressionState &state_p) {
	auto &state = state_p.Cast<NumericStringCompressionState>();
	state.Finalize();
}

//===--------------------------------------------------------------------===//
// Scan
//===--------------------------------------------------------------------===//
struct NumericStringScanState : public SegmentScanState {
	NumericStringScanState(BufferHandle handle_p, const ColumnSegment &segment)
	    : handle(std::move(handle_p)), segment_count(segment.count.load()),
	      reader(CompressionSegmentReader::FromSegment(handle, segment, "NumericString segment")) {
		if (reader.Size() < sizeof(SegmentHeader)) {
			ThrowCorrupt("segment is smaller than the header");
		}
		memcpy(&header, reader.GetArray<uint8_t>(0, sizeof(SegmentHeader)).data(), sizeof(SegmentHeader));
		if (header.mode > static_cast<uint8_t>(NumericStringMode::FIXED_WIDTH)) {
			ThrowCorrupt("unknown mode");
		}
		auto expected_groups = (segment_count + GROUP_SIZE - 1) / GROUP_SIZE;
		if (header.group_count != expected_groups) {
			ThrowCorrupt("group count does not match the row count");
		}
		if (header.mantissa_meta_offset != sizeof(SegmentHeader) ||
		    header.scale_meta_offset != header.mantissa_meta_offset + header.group_count * sizeof(MantissaGroupMeta) ||
		    header.data_offset < header.scale_meta_offset + header.group_count * sizeof(ScaleGroupMeta) ||
		    header.data_offset > reader.Size()) {
			ThrowCorrupt("invalid section offsets");
		}
	}

	idx_t GroupRows(idx_t group) const {
		auto start = group * GROUP_SIZE;
		return MinValue<idx_t>(GROUP_SIZE, segment_count - start);
	}

	MantissaGroupMeta GetMantissaMeta(idx_t group) const {
		MantissaGroupMeta meta;
		auto offset = header.mantissa_meta_offset + group * sizeof(MantissaGroupMeta);
		memcpy(&meta, reader.GetArray<uint8_t>(offset, sizeof(MantissaGroupMeta)).data(), sizeof(MantissaGroupMeta));
		if (meta.width > 64) {
			ThrowCorrupt("mantissa width exceeds 64 bits");
		}
		return meta;
	}

	ScaleGroupMeta GetScaleMeta(idx_t group) const {
		ScaleGroupMeta meta;
		auto offset = header.scale_meta_offset + group * sizeof(ScaleGroupMeta);
		memcpy(&meta, reader.GetArray<uint8_t>(offset, sizeof(ScaleGroupMeta)).data(), sizeof(ScaleGroupMeta));
		if (meta.width > 32) {
			ThrowCorrupt("scale width exceeds 32 bits");
		}
		return meta;
	}

	//! Returns the packed bytes of a group section after validating they are inside the segment
	const_data_ptr_t PackedData(idx_t relative_offset, idx_t rows, bitpacking_width_t width) const {
		auto bytes = PackedBytes(rows, width);
		auto offset = idx_t(header.data_offset) + relative_offset;
		return reader.GetArray<uint8_t>(offset, bytes).data();
	}

	//! Decode a full group into the mantissa / scale buffers
	void LoadGroup(idx_t group) {
		if (group == loaded_group) {
			return;
		}
		auto rows = GroupRows(group);
		auto aligned_rows = BitpackingPrimitives::RoundUpToAlgorithmGroupSize(rows);
		D_ASSERT(aligned_rows <= GROUP_SIZE);

		auto mantissa = GetMantissaMeta(group);
		if (mantissa.width == 0) {
			for (idx_t i = 0; i < rows; i++) {
				mantissas[i] = mantissa.base;
			}
		} else {
			auto src = PackedData(mantissa.data_offset, rows, mantissa.width);
			BitpackingPrimitives::UnPackBuffer<uint64_t>(data_ptr_cast(delta_buffer), src, aligned_rows, mantissa.width,
			                                             true);
			auto base = UnsafeNumericCast<uint64_t>(mantissa.base);
			for (idx_t i = 0; i < rows; i++) {
				mantissas[i] = UnsafeNumericCast<int64_t>(base + delta_buffer[i]);
			}
		}

		auto scale = GetScaleMeta(group);
		if (scale.width == 0) {
			for (idx_t i = 0; i < rows; i++) {
				scales[i] = scale.base;
			}
		} else {
			auto src = PackedData(scale.data_offset, rows, scale.width);
			BitpackingPrimitives::UnPackBuffer<uint32_t>(data_ptr_cast(scale_buffer), src, aligned_rows, scale.width,
			                                             true);
			for (idx_t i = 0; i < rows; i++) {
				auto value = uint32_t(scale.base) + scale_buffer[i];
				if (value > NumericLimits<uint8_t>::Maximum()) {
					ThrowCorrupt("scale exceeds 255");
				}
				scales[i] = UnsafeNumericCast<uint8_t>(value);
			}
		}
		loaded_group = group;
	}

	//! Decode a single row without decoding the whole group
	void LoadRow(idx_t row, int64_t &mantissa_result, uint8_t &scale_result) {
		auto group = row / GROUP_SIZE;
		auto index = row % GROUP_SIZE;
		if (group == loaded_group) {
			mantissa_result = mantissas[index];
			scale_result = scales[index];
			return;
		}
		auto rows = GroupRows(group);
		auto block = index / BitpackingPrimitives::BITPACKING_ALGORITHM_GROUP_SIZE;
		auto index_in_block = index % BitpackingPrimitives::BITPACKING_ALGORITHM_GROUP_SIZE;

		auto mantissa = GetMantissaMeta(group);
		if (mantissa.width == 0) {
			mantissa_result = mantissa.base;
		} else {
			auto src = PackedData(mantissa.data_offset, rows, mantissa.width);
			auto block_bytes = (BitpackingPrimitives::BITPACKING_ALGORITHM_GROUP_SIZE * mantissa.width) / 8;
			BitpackingPrimitives::UnPackBlock<uint64_t>(data_ptr_cast(delta_buffer), src + block * block_bytes,
			                                            mantissa.width, true);
			mantissa_result =
			    UnsafeNumericCast<int64_t>(UnsafeNumericCast<uint64_t>(mantissa.base) + delta_buffer[index_in_block]);
		}

		auto scale = GetScaleMeta(group);
		if (scale.width == 0) {
			scale_result = scale.base;
		} else {
			auto src = PackedData(scale.data_offset, rows, scale.width);
			auto block_bytes = (BitpackingPrimitives::BITPACKING_ALGORITHM_GROUP_SIZE * scale.width) / 8;
			BitpackingPrimitives::UnPackBlock<uint32_t>(data_ptr_cast(scale_buffer), src + block * block_bytes,
			                                            scale.width, true);
			auto value = uint32_t(scale.base) + scale_buffer[index_in_block];
			if (value > NumericLimits<uint8_t>::Maximum()) {
				ThrowCorrupt("scale exceeds 255");
			}
			scale_result = UnsafeNumericCast<uint8_t>(value);
		}
	}

	inline idx_t Format(int64_t mantissa, uint8_t scale, char *buffer) const {
		if (header.mode == static_cast<uint8_t>(NumericStringMode::FIXED_WIDTH)) {
			return FormatFixedWidth(mantissa, header.fixed_width, header.fixed_scale, buffer);
		}
		return FormatCanonical(mantissa, scale, buffer);
	}

	inline string_t FormatInto(Vector &result, int64_t mantissa, uint8_t scale) const {
		char buffer[FORMAT_BUFFER_SIZE];
		auto len = Format(mantissa, scale, buffer);
		auto str = StringVector::EmptyString(result, len);
		memcpy(str.GetDataWriteable(), buffer, len);
		str.Finalize();
		return str;
	}

	BufferHandle handle;
	const idx_t segment_count;
	CompressionSegmentReader reader;
	SegmentHeader header;

	idx_t loaded_group = DConstants::INVALID_INDEX;
	int64_t mantissas[GROUP_SIZE];
	uint8_t scales[GROUP_SIZE];
	uint64_t delta_buffer[GROUP_SIZE];
	uint32_t scale_buffer[GROUP_SIZE];
};

static unique_ptr<SegmentScanState> InitScan(const QueryContext &context, ColumnSegment &segment) {
	auto &buffer_manager = BufferManager::GetBufferManager(segment.GetDatabase());
	auto handle = buffer_manager.Pin(context, segment.GetBlockHandle());
	return make_uniq<NumericStringScanState>(std::move(handle), segment);
}

static void ScanPartial(ColumnSegment &segment, ColumnScanState &state, idx_t scan_count, Vector &result,
                        idx_t result_offset) {
	auto &scan_state = state.scan_state->Cast<NumericStringScanState>();
	auto start = state.GetPositionInSegment();
	D_ASSERT(start + scan_count <= segment.count.load());
	D_ASSERT(result.GetVectorType() == VectorType::FLAT_VECTOR);
	auto result_data = FlatVector::GetDataMutable<string_t>(result);

	idx_t scanned = 0;
	while (scanned < scan_count) {
		auto row = start + scanned;
		auto group = row / GROUP_SIZE;
		auto index = row % GROUP_SIZE;
		auto to_scan = MinValue<idx_t>(scan_count - scanned, GROUP_SIZE - index);
		scan_state.LoadGroup(group);
		for (idx_t i = 0; i < to_scan; i++) {
			result_data[result_offset + scanned + i] =
			    scan_state.FormatInto(result, scan_state.mantissas[index + i], scan_state.scales[index + i]);
		}
		scanned += to_scan;
	}
}

static void Scan(ColumnSegment &segment, ColumnScanState &state, idx_t scan_count, Vector &result) {
	ScanPartial(segment, state, scan_count, result, 0);
}

//===--------------------------------------------------------------------===//
// Fetch
//===--------------------------------------------------------------------===//
static void FetchRow(ColumnSegment &segment, ColumnFetchState &state, row_t row_id, Vector &result, idx_t result_idx) {
	D_ASSERT(row_id >= 0);
	auto row = UnsafeNumericCast<idx_t>(row_id);
	D_ASSERT(row < segment.count.load());
	auto &buffer_manager = BufferManager::GetBufferManager(segment.GetDatabase());
	auto handle = buffer_manager.Pin(state.context, segment.GetBlockHandle());
	NumericStringScanState scan_state(std::move(handle), segment);

	int64_t mantissa;
	uint8_t scale;
	scan_state.LoadRow(row, mantissa, scale);
	auto result_data = FlatVector::GetDataMutable<string_t>(result);
	result_data[result_idx] = scan_state.FormatInto(result, mantissa, scale);
}

//===--------------------------------------------------------------------===//
// Segment info
//===--------------------------------------------------------------------===//
static InsertionOrderPreservingMap<string> GetSegmentInfo(QueryContext context, ColumnSegment &segment) {
	auto &buffer_manager = BufferManager::GetBufferManager(segment.GetDatabase());
	auto handle = buffer_manager.Pin(context, segment.GetBlockHandle());
	NumericStringScanState scan_state(std::move(handle), segment);

	InsertionOrderPreservingMap<string> result;
	if (scan_state.header.mode == static_cast<uint8_t>(NumericStringMode::FIXED_WIDTH)) {
		result["mode"] = "fixed_width";
		result["width"] = to_string(scan_state.header.fixed_width);
		result["scale"] = to_string(scan_state.header.fixed_scale);
	} else {
		result["mode"] = "canonical";
	}
	result["group_count"] = to_string(scan_state.header.group_count);
	idx_t mantissa_bits = 0;
	idx_t scale_bits = 0;
	for (idx_t group = 0; group < scan_state.header.group_count; group++) {
		mantissa_bits += scan_state.GetMantissaMeta(group).width;
		scale_bits += scan_state.GetScaleMeta(group).width;
	}
	if (scan_state.header.group_count > 0) {
		result["avg_mantissa_width"] = to_string(mantissa_bits / scan_state.header.group_count);
		result["avg_scale_width"] = to_string(scale_bits / scan_state.header.group_count);
	}
	return result;
}

} // namespace numeric_string

//===--------------------------------------------------------------------===//
// Get Function
//===--------------------------------------------------------------------===//
CompressionFunction NumericStringCompressionFun::GetFunction(PhysicalType data_type) {
	D_ASSERT(data_type == PhysicalType::VARCHAR);
	auto function = CompressionFunction(
	    CompressionType::COMPRESSION_NUMERIC_STRING, data_type, numeric_string::InitAnalyze, numeric_string::Analyze,
	    numeric_string::FinalAnalyze, numeric_string::InitCompression, numeric_string::Compress,
	    numeric_string::FinalizeCompress, numeric_string::InitScan, numeric_string::Scan, numeric_string::ScanPartial,
	    numeric_string::FetchRow, UncompressedFunctions::EmptySkip);
	function.get_segment_info = numeric_string::GetSegmentInfo;
	return function;
}

bool NumericStringCompressionFun::TypeIsSupported(const PhysicalType physical_type) {
	return physical_type == PhysicalType::VARCHAR;
}

} // namespace duckdb
