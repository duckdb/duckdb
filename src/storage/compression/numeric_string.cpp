#include "duckdb/common/deque.hpp"
#include "duckdb/common/serializer/deserializer.hpp"
#include "duckdb/common/serializer/serializer.hpp"
#include "duckdb/common/types/cast_helpers.hpp"
#include "duckdb/common/vector/flat_vector.hpp"
#include "duckdb/common/vector/string_vector.hpp"
#include "duckdb/function/compression/compression.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/storage/statistics/stats_writer.hpp"
#include "duckdb/storage/storage_manager.hpp"
#include "duckdb/storage/table/column_checkpoint_state.hpp"
#include "duckdb/storage/table/column_data.hpp"
#include "duckdb/storage/table/column_data_checkpointer.hpp"
#include "duckdb/storage/table/column_segment.hpp"
#include "duckdb/storage/table/scan_state.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// NumericString compression
//
// Stores VARCHAR values that are decimal numbers ("42", "-17", "123.45", "00123") through the integer compression
// methods. Every value is parsed into an int64_t mantissa; the mantissas are handed to the integer compression method
// (bitpacking or RLE) that analyzed smallest, which writes the segment exactly as it would for a BIGINT column. Each
// segment records the chosen integer method and the layout needed to turn the mantissas back into the original
// strings on scan. All values of a column must share one layout:
// - CANONICAL: every value is the shortest decimal rendering of its mantissa at a fixed scale: optional '-', no
//   leading zeros ("42", "-17" at scale 0; "1.50", "123.45" at scale 2).
// - FIXED_WIDTH: every value has the same length and scale and no sign. Leading zeros are allowed ("00123", "01.50").
//   This covers zero-padded identifiers such as postal codes.
// Anything else (mixed scales, mixed widths with leading zeros, non-numeric text) is left to the string methods.
//===--------------------------------------------------------------------===//
namespace numeric_string {

//! 18 digits always fit in an int64_t mantissa
static constexpr idx_t MAX_DIGITS = 18;
//! Longest accepted string (sign, digits, decimal point and zero padding); bounds the formatting buffer
static constexpr idx_t MAX_LENGTH = 48;
//! Largest buffer any formatted value needs: sign + 18 digits + '.' + padding zeros
static constexpr idx_t FORMAT_BUFFER_SIZE = 64;
static_assert(FORMAT_BUFFER_SIZE > MAX_LENGTH, "formatted values must fit the format buffer");
//! The mantissas are stored as BIGINT
static constexpr PhysicalType MANTISSA_PHYSICAL_TYPE = PhysicalType::INT64;
//! Integer compression methods the mantissas can be stored with. These methods only touch the block handle and the
//! row count of a segment, so a VARCHAR segment can hand them its block directly.
static constexpr CompressionType INNER_COMPRESSION_TYPES[] = {CompressionType::COMPRESSION_BITPACKING,
                                                              CompressionType::COMPRESSION_RLE};

enum class NumericStringMode : uint8_t { CANONICAL = 0, FIXED_WIDTH = 1 };

struct NumericStringLayout {
	NumericStringMode mode = NumericStringMode::CANONICAL;
	//! Number of fractional digits
	uint8_t scale = 0;
	//! Total string length (FIXED_WIDTH only)
	uint8_t width = 0;
};

static bool IsInnerCompressionType(CompressionType type) {
	for (auto inner_type : INNER_COMPRESSION_TYPES) {
		if (inner_type == type) {
			return true;
		}
	}
	return false;
}

//===--------------------------------------------------------------------===//
// Parsing and formatting
//===--------------------------------------------------------------------===//
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

//! Formats a fixed-width value into out, returns the length (== width for every value that was stored). The integer
//! methods store arbitrary values in NULL slots, so a mantissa that does not fit the width is rendered canonically
//! instead of being treated as corruption; the row's validity decides whether anyone sees it.
static idx_t FormatFixedWidth(int64_t mantissa, uint8_t width, uint8_t scale, char *out) {
	char digits[24];
	auto end = digits + sizeof(digits);
	auto ptr = NumericHelper::FormatUnsigned<uint64_t>(AbsoluteMantissa(mantissa), end);
	auto digit_count = UnsafeNumericCast<idx_t>(end - ptr);
	idx_t total_digits = scale > 0 ? idx_t(width) - 1 : idx_t(width);
	if (mantissa < 0 || digit_count > total_digits || total_digits < scale || (scale > 0 && total_digits == scale)) {
		return FormatCanonical(mantissa, scale, out);
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

static inline string_t FormatInto(Vector &result, const NumericStringLayout &layout, int64_t mantissa) {
	char buffer[FORMAT_BUFFER_SIZE];
	idx_t len;
	if (layout.mode == NumericStringMode::FIXED_WIDTH) {
		len = FormatFixedWidth(mantissa, layout.width, layout.scale, buffer);
	} else {
		len = FormatCanonical(mantissa, layout.scale, buffer);
	}
	auto str = StringVector::EmptyString(result, len);
	memcpy(str.GetDataWriteable(), buffer, len);
	str.Finalize();
	return str;
}

//===--------------------------------------------------------------------===//
// Segment state
//===--------------------------------------------------------------------===//
//! The persisted per-segment state: which integer method wrote the block and how to format the mantissas
struct NumericStringSegmentState : public ColumnSegmentState {
	NumericStringSegmentState() = default;
	NumericStringSegmentState(CompressionType inner_type, NumericStringLayout layout)
	    : inner_type(inner_type), layout(layout) {
	}

	CompressionType inner_type = CompressionType::COMPRESSION_AUTO;
	NumericStringLayout layout;

	void Serialize(Serializer &serializer) const override {
		serializer.WriteProperty(1, "inner_compression", inner_type);
		serializer.WriteProperty(2, "mode", static_cast<uint8_t>(layout.mode));
		serializer.WriteProperty(3, "scale", layout.scale);
		serializer.WriteProperty(4, "width", layout.width);
	}

	static unique_ptr<ColumnSegmentState> Deserialize(Deserializer &deserializer) {
		auto result = make_uniq<NumericStringSegmentState>();
		deserializer.ReadProperty(1, "inner_compression", result->inner_type);
		auto mode = deserializer.ReadProperty<uint8_t>(2, "mode");
		if (mode > static_cast<uint8_t>(NumericStringMode::FIXED_WIDTH)) {
			ThrowCorrupt("unknown mode");
		}
		result->layout.mode = static_cast<NumericStringMode>(mode);
		deserializer.ReadProperty(3, "scale", result->layout.scale);
		deserializer.ReadProperty(4, "width", result->layout.width);
		return std::move(result);
	}
};

//! The in-memory segment state: the persisted state plus the resolved integer compression function
struct NumericStringCompressedState : public CompressedSegmentState {
	NumericStringCompressedState(const CompressionFunction &inner, NumericStringLayout layout)
	    : inner(inner), layout(layout) {
	}

	const CompressionFunction &inner;
	NumericStringLayout layout;

	string GetSegmentInfo() const override {
		return StringUtil::Format(
		    "%s, %s", CompressionTypeToString(inner.type),
		    layout.mode == NumericStringMode::FIXED_WIDTH
		        ? StringUtil::Format("fixed width %d, scale %d", int(layout.width), int(layout.scale))
		        : StringUtil::Format("canonical, scale %d", int(layout.scale)));
	}
};

static unique_ptr<CompressedSegmentState> InitSegment(ColumnSegment &segment, block_id_t block_id,
                                                      optional_ptr<ColumnSegmentState> segment_state) {
	if (!segment_state) {
		ThrowCorrupt("segment has no state");
	}
	auto &state = segment_state->Cast<NumericStringSegmentState>();
	if (!IsInnerCompressionType(state.inner_type)) {
		ThrowCorrupt("unsupported inner compression method");
	}
	if (state.layout.mode == NumericStringMode::FIXED_WIDTH &&
	    (state.layout.width == 0 || (state.layout.scale > 0 && state.layout.width <= state.layout.scale + 1))) {
		ThrowCorrupt("fixed-width layout has no room for its digits");
	}
	auto &config = DBConfig::GetConfig(segment.GetDatabase());
	auto &inner = config.GetCompressionFunction(state.inner_type, MANTISSA_PHYSICAL_TYPE).get();
	return make_uniq<NumericStringCompressedState>(inner, state.layout);
}

static unique_ptr<ColumnSegmentState> SerializeState(ColumnSegment &segment) {
	auto &state = segment.GetSegmentState()->Cast<NumericStringCompressedState>();
	return make_uniq<NumericStringSegmentState>(state.inner.type, state.layout);
}

static unique_ptr<ColumnSegmentState> DeserializeState(Deserializer &deserializer) {
	return NumericStringSegmentState::Deserialize(deserializer);
}

//===--------------------------------------------------------------------===//
// Mantissa conversion
//===--------------------------------------------------------------------===//
//! Tracks which layouts are still possible for the values seen so far
struct LayoutDetector {
	bool canonical_ok = true;
	bool fixed_ok = true;
	bool have_value = false;
	uint8_t scale = 0;
	uint8_t width = 0;

	//! Returns false if the values seen so far no longer share a layout
	bool Accept(const ParsedNumber &number) {
		if (!have_value) {
			have_value = true;
			scale = number.scale;
			width = number.length;
		} else if (number.scale != scale) {
			return false;
		}
		if (number.leading_zero) {
			canonical_ok = false;
		}
		if (number.negative || number.length != width) {
			fixed_ok = false;
		}
		return canonical_ok || fixed_ok;
	}

	NumericStringLayout GetLayout() const {
		NumericStringLayout layout;
		layout.scale = scale;
		if (canonical_ok) {
			layout.mode = NumericStringMode::CANONICAL;
		} else {
			layout.mode = NumericStringMode::FIXED_WIDTH;
			layout.width = width;
		}
		return layout;
	}
};

//! Converts a vector of numeric strings into a flat vector of mantissas with the same validity. NULL slots get the
//! previous mantissa so they do not widen the range the integer method sees. Returns false on a value that does not
//! parse or does not fit the layout.
static bool ConvertToMantissas(const Vector &input, Vector &mantissas, LayoutDetector &detector, int64_t &last_mantissa,
                               optional_ptr<StatsWriter<string_t>> stats) {
	UnifiedVectorFormat vdata;
	input.ToUnifiedFormat(vdata);
	auto data = UnifiedVectorFormat::GetData<string_t>(vdata);
	auto count = input.size();

	mantissas.SetVectorType(VectorType::FLAT_VECTOR);
	FlatVector::SetSize(mantissas, count);
	auto out = FlatVector::GetDataMutable<int64_t>(mantissas);
	auto &out_validity = FlatVector::ValidityMutable(mantissas);
	out_validity.Reset(count);

	for (idx_t i = 0; i < count; i++) {
		auto idx = vdata.sel->get_index(i);
		if (!vdata.validity.RowIsValid(idx)) {
			out_validity.SetInvalid(i);
			out[i] = last_mantissa;
			if (stats) {
				stats->SetHasNull();
			}
			continue;
		}
		ParsedNumber number;
		if (!ParseNumericString(data[idx].GetData(), data[idx].GetSize(), number)) {
			return false;
		}
		if (!detector.Accept(number)) {
			return false;
		}
		if (stats) {
			stats->Update(data[idx]);
		}
		out[i] = number.mantissa;
		last_mantissa = number.mantissa;
	}
	return true;
}

//===--------------------------------------------------------------------===//
// Analyze
//===--------------------------------------------------------------------===//
struct InnerCandidate {
	InnerCandidate(reference<const CompressionFunction> function, unique_ptr<AnalyzeState> state)
	    : function(function), state(std::move(state)) {
	}

	reference<const CompressionFunction> function;
	unique_ptr<AnalyzeState> state;
};

struct NumericStringAnalyzeState : public AnalyzeState {
	explicit NumericStringAnalyzeState(ColumnData &col_data)
	    : AnalyzeState(col_data.GetBlockManager()), mantissas(LogicalType::BIGINT) {
		auto &config = DBConfig::GetConfig(col_data.GetDatabase());
		for (auto function : config.GetCompressionFunctions(MANTISSA_PHYSICAL_TYPE)) {
			if (!IsInnerCompressionType(function.get().type)) {
				continue;
			}
			auto state = function.get().init_analyze(col_data, MANTISSA_PHYSICAL_TYPE);
			if (!state) {
				continue;
			}
			candidates.emplace_back(function, std::move(state));
		}
	}

	//! The integer methods still in the running
	vector<InnerCandidate> candidates;
	//! Set by FinalAnalyze: the candidate that analyzed smallest
	idx_t best_candidate = DConstants::INVALID_INDEX;
	LayoutDetector detector;
	Vector mantissas;
	int64_t last_mantissa = 0;
	idx_t total_count = 0;

	InnerCandidate &Best() {
		D_ASSERT(best_candidate < candidates.size());
		return candidates[best_candidate];
	}
};

static unique_ptr<AnalyzeState> InitAnalyze(ColumnData &col_data, PhysicalType type) {
	if (col_data.type.id() != LogicalTypeId::VARCHAR) {
		// BLOB shares the physical type but its bytes are not text
		return nullptr;
	}
	auto &storage_manager = col_data.GetStorageManager();
	if (!StorageManager::TargetAtLeastVersion(StorageVersion::V2_1_0, storage_manager.GetStorageVersion())) {
		// not supported by the target storage version
		return nullptr;
	}
	auto state = make_uniq<NumericStringAnalyzeState>(col_data);
	if (state->candidates.empty()) {
		// no integer method available to store the mantissas with
		return nullptr;
	}
	return std::move(state);
}

static bool Analyze(AnalyzeState &state_p, const Vector &input) {
	auto &state = state_p.Cast<NumericStringAnalyzeState>();
	if (!ConvertToMantissas(input, state.mantissas, state.detector, state.last_mantissa, nullptr)) {
		return false;
	}
	state.total_count += input.size();
	for (idx_t i = 0; i < state.candidates.size();) {
		auto &candidate = state.candidates[i];
		if (candidate.function.get().analyze(*candidate.state, state.mantissas)) {
			i++;
		} else {
			state.candidates.erase_at(i);
		}
	}
	return !state.candidates.empty();
}

static idx_t FinalAnalyze(AnalyzeState &state_p) {
	auto &state = state_p.Cast<NumericStringAnalyzeState>();
	if (state.total_count == 0 || !state.detector.have_value) {
		return NumericLimits<idx_t>::Maximum();
	}
	idx_t best_size = NumericLimits<idx_t>::Maximum();
	for (idx_t i = 0; i < state.candidates.size(); i++) {
		auto &candidate = state.candidates[i];
		auto size = candidate.function.get().final_analyze(*candidate.state);
		if (size < best_size) {
			best_size = size;
			state.best_candidate = i;
		}
	}
	return best_size;
}

//===--------------------------------------------------------------------===//
// Compress
//===--------------------------------------------------------------------===//
struct NumericStringCompressionState;

//! Receives the segments the integer method flushes so they can be re-wrapped as NumericString segments
struct InnerCheckpointState : public ColumnCheckpointState {
	InnerCheckpointState(ColumnDataCheckpointData &checkpoint_data, NumericStringCompressionState &owner)
	    : ColumnCheckpointState(checkpoint_data.GetRowGroup(), checkpoint_data.GetColumnData(),
	                            checkpoint_data.GetCheckpointState().GetPartialBlockManager()),
	      owner(owner) {
	}

	NumericStringCompressionState &owner;

	void FlushSegment(unique_ptr<ColumnSegment> segment, BufferHandle handle, idx_t segment_size) override;
};

//! String statistics of one appended vector, so segment statistics can be assembled for whatever row ranges the
//! integer method decides to cut its segments at
struct VectorStats {
	VectorStats(idx_t row_start, idx_t count, BaseStatistics stats)
	    : row_start(row_start), count(count), stats(std::move(stats)) {
	}

	idx_t row_start;
	idx_t count;
	BaseStatistics stats;
};

struct NumericStringCompressionState : public CompressionState {
	NumericStringCompressionState(ColumnDataCheckpointData &checkpoint_data,
	                              unique_ptr<NumericStringAnalyzeState> analyze_state)
	    : CompressionState(checkpoint_data, CompressionType::COMPRESSION_NUMERIC_STRING),
	      layout(analyze_state->detector.GetLayout()), inner(analyze_state->Best().function.get()),
	      inner_checkpoint_state(checkpoint_data, *this),
	      inner_checkpoint_data(inner_checkpoint_state, checkpoint_data.GetColumnData(), checkpoint_data.GetDatabase(),
	                            checkpoint_data.GetRowGroup(), checkpoint_data.GetStorageManager(),
	                            LogicalType::BIGINT),
	      mantissas(LogicalType::BIGINT), stats_writer(checkpoint_data.GetColumnData().type) {
		detector = analyze_state->detector;
		inner_state = inner.init_compression(inner_checkpoint_data, std::move(analyze_state->Best().state));
	}

	NumericStringLayout layout;
	//! The integer compression method that stores the mantissas
	const CompressionFunction &inner;
	InnerCheckpointState inner_checkpoint_state;
	ColumnDataCheckpointData inner_checkpoint_data;
	unique_ptr<CompressionState> inner_state;

	LayoutDetector detector;
	Vector mantissas;
	int64_t last_mantissa = 0;
	StatsWriter<string_t> stats_writer;
	//! Statistics of the appended vectors that have not been fully flushed yet
	deque<VectorStats> pending_stats;
	idx_t rows_appended = 0;
	idx_t rows_flushed = 0;

	void Append(const Vector &input) {
		stats_writer.Clear();
		if (!ConvertToMantissas(input, mantissas, detector, last_mantissa, stats_writer)) {
			throw InternalException("NumericString compression: value does not match the analyzed layout");
		}
		auto stats = BaseStatistics::CreateEmpty(checkpoint_data.GetColumnData().type);
		stats_writer.Merge(stats);
		pending_stats.emplace_back(rows_appended, input.size(), std::move(stats));
		rows_appended += input.size();
		inner.compress(*inner_state, mantissas);
	}

	void Finalize() {
		inner.compress_finalize(*inner_state);
	}

	//! Statistics for the rows [rows_flushed, rows_flushed + count): the union of the statistics of every appended
	//! vector that overlaps the range. Segment boundaries need not align with vectors, so the result can be slightly
	//! wider than the segment itself.
	BaseStatistics SegmentStats(idx_t count) {
		auto segment_end = rows_flushed + count;
		auto stats = BaseStatistics::CreateEmpty(checkpoint_data.GetColumnData().type);
		for (auto &entry : pending_stats) {
			if (entry.row_start >= segment_end) {
				break;
			}
			stats.Merge(entry.stats);
		}
		while (!pending_stats.empty() && pending_stats.front().row_start + pending_stats.front().count <= segment_end) {
			pending_stats.pop_front();
		}
		return stats;
	}

	void FlushInnerSegment(unique_ptr<ColumnSegment> inner_segment, BufferHandle handle, idx_t segment_size) {
		auto count = inner_segment->count.load();
		if (count == 0) {
			return;
		}
		if (rows_flushed + count > rows_appended) {
			throw InternalException("NumericString compression: the integer method flushed more rows than appended");
		}
		auto stats = SegmentStats(count);
		auto segment_state = make_uniq<NumericStringSegmentState>(inner.type, layout);
		// the segment keeps the block the integer method wrote, but is a VARCHAR segment of this compression method
		auto segment = make_uniq<ColumnSegment>(
		    checkpoint_data.GetDatabase(), inner_segment->GetBlockHandle(), ColumnSegmentType::TRANSIENT, count,
		    function, std::move(stats), INVALID_BLOCK, 0, inner_segment->SegmentSize(), std::move(segment_state));
		inner_segment.reset();
		rows_flushed += count;
		checkpoint_data.GetCheckpointState().FlushSegment(std::move(segment), std::move(handle), segment_size);
	}
};

void InnerCheckpointState::FlushSegment(unique_ptr<ColumnSegment> segment, BufferHandle handle, idx_t segment_size) {
	owner.FlushInnerSegment(std::move(segment), std::move(handle), segment_size);
}

static unique_ptr<CompressionState> InitCompression(ColumnDataCheckpointData &checkpoint_data,
                                                    unique_ptr<AnalyzeState> analyze_state_p) {
	auto analyze_state = unique_ptr_cast<AnalyzeState, NumericStringAnalyzeState>(std::move(analyze_state_p));
	if (analyze_state->best_candidate == DConstants::INVALID_INDEX) {
		throw InternalException("NumericString compression: no integer method selected");
	}
	return make_uniq<NumericStringCompressionState>(checkpoint_data, std::move(analyze_state));
}

static void Compress(CompressionState &state_p, const Vector &scan_vector) {
	auto &state = state_p.Cast<NumericStringCompressionState>();
	state.Append(scan_vector);
}

static void FinalizeCompress(CompressionState &state_p) {
	auto &state = state_p.Cast<NumericStringCompressionState>();
	state.Finalize();
}

//===--------------------------------------------------------------------===//
// Scan
//===--------------------------------------------------------------------===//
struct NumericStringScanState : public SegmentScanState {
	explicit NumericStringScanState(unique_ptr<SegmentScanState> inner_state)
	    : inner_state(std::move(inner_state)), mantissas(LogicalType::BIGINT), capacity(STANDARD_VECTOR_SIZE) {
	}

	//! The scan state of the integer method
	unique_ptr<SegmentScanState> inner_state;
	//! Scratch space the integer method scans into
	Vector mantissas;
	idx_t capacity;

	//! List children scan more than STANDARD_VECTOR_SIZE rows at once: grow the scratch space to fit
	Vector &Mantissas(idx_t count) {
		if (count > capacity) {
			mantissas.Initialize(VectorDataInitialization::UNINITIALIZED, count);
			capacity = count;
		}
		return mantissas;
	}
};

//! The integer method expects its own scan state in the column scan state: swap it in for the duration of the call
struct InnerScanScope {
	InnerScanScope(ColumnScanState &state, NumericStringScanState &scan_state) : state(state), scan_state(scan_state) {
		std::swap(state.scan_state, scan_state.inner_state);
	}
	~InnerScanScope() {
		std::swap(state.scan_state, scan_state.inner_state);
	}

	ColumnScanState &state;
	NumericStringScanState &scan_state;
};

static NumericStringCompressedState &GetCompressedState(ColumnSegment &segment) {
	return segment.GetSegmentState()->Cast<NumericStringCompressedState>();
}

static unique_ptr<SegmentScanState> InitScan(const QueryContext &context, ColumnSegment &segment) {
	auto &compressed = GetCompressedState(segment);
	return make_uniq<NumericStringScanState>(compressed.inner.init_scan(context, segment));
}

static void FormatMantissas(const NumericStringLayout &layout, Vector &mantissas, idx_t count, Vector &result,
                            idx_t result_offset) {
	D_ASSERT(result.GetVectorType() == VectorType::FLAT_VECTOR);
	D_ASSERT(mantissas.GetVectorType() == VectorType::FLAT_VECTOR);
	auto mantissa_data = FlatVector::GetData<int64_t>(mantissas);
	auto result_data = FlatVector::GetDataMutable<string_t>(result);
	for (idx_t i = 0; i < count; i++) {
		result_data[result_offset + i] = FormatInto(result, layout, mantissa_data[i]);
	}
}

static void ScanPartial(ColumnSegment &segment, ColumnScanState &state, idx_t scan_count, Vector &result,
                        idx_t result_offset) {
	auto &compressed = GetCompressedState(segment);
	auto &scan_state = state.scan_state->Cast<NumericStringScanState>();
	auto &mantissas = scan_state.Mantissas(scan_count);
	{
		InnerScanScope scope(state, scan_state);
		compressed.inner.scan_partial(segment, state, scan_count, mantissas, 0);
	}
	FormatMantissas(compressed.layout, mantissas, scan_count, result, result_offset);
}

static void Scan(ColumnSegment &segment, ColumnScanState &state, idx_t scan_count, Vector &result) {
	ScanPartial(segment, state, scan_count, result, 0);
}

static void Skip(ColumnSegment &segment, ColumnScanState &state, idx_t skip_count) {
	auto &compressed = GetCompressedState(segment);
	auto &scan_state = state.scan_state->Cast<NumericStringScanState>();
	InnerScanScope scope(state, scan_state);
	compressed.inner.skip(segment, state, skip_count);
}

//===--------------------------------------------------------------------===//
// Segment info
//===--------------------------------------------------------------------===//
static InsertionOrderPreservingMap<string> GetSegmentInfo(QueryContext context, ColumnSegment &segment) {
	auto &compressed = GetCompressedState(segment);
	InsertionOrderPreservingMap<string> result;
	result["inner_compression"] = CompressionTypeToString(compressed.inner.type);
	if (compressed.layout.mode == NumericStringMode::FIXED_WIDTH) {
		result["mode"] = "fixed_width";
		result["width"] = to_string(compressed.layout.width);
	} else {
		result["mode"] = "canonical";
	}
	result["scale"] = to_string(compressed.layout.scale);
	if (compressed.inner.get_segment_info) {
		for (auto &entry : compressed.inner.get_segment_info(context, segment)) {
			result[entry.first] = entry.second;
		}
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
	    numeric_string::Skip, numeric_string::InitSegment, nullptr, nullptr, nullptr, nullptr,
	    numeric_string::SerializeState, numeric_string::DeserializeState);
	function.get_segment_info = numeric_string::GetSegmentInfo;
	return function;
}

bool NumericStringCompressionFun::TypeIsSupported(const PhysicalType physical_type) {
	return physical_type == PhysicalType::VARCHAR;
}

} // namespace duckdb
