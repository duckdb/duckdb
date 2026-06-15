#include "duckdb/common/vector/array_vector.hpp"
#include "duckdb/common/vector/constant_vector.hpp"
#include "duckdb/common/vector/flat_vector.hpp"
#include "duckdb/common/vector/list_vector.hpp"
#include "duckdb/common/vector/map_vector.hpp"
#include "duckdb/common/vector/string_vector.hpp"
#include "duckdb/common/vector/struct_vector.hpp"
#include "duckdb/function/create_sort_key.hpp"

#include "duckdb/common/enums/order_type.hpp"
#include "duckdb/common/radix.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/common/types/datetime.hpp"
#include "duckdb/common/types/interval.hpp"
#include "duckdb/common/types/uhugeint.hpp"
#include "duckdb/common/types/bignum.hpp"
#include "duckdb/common/types/variant.hpp"
#include "duckdb/common/enum_util.hpp"
#include "duckdb/function/scalar/variant_utils.hpp"
#include "duckdb/function/scalar/generic_functions.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression_binder.hpp"
#include "duckdb/parser/parser.hpp"

#include <algorithm>

namespace duckdb {

namespace {
struct SortKeyBindData : public FunctionData {
	vector<OrderModifiers> modifiers;
	bool all_constant = false;

	bool Equals(const FunctionData &other_p) const override {
		auto &other = other_p.Cast<SortKeyBindData>();
		return modifiers == other.modifiers && all_constant == other.all_constant;
	}
	unique_ptr<FunctionData> Copy() const override {
		auto result = make_uniq<SortKeyBindData>();
		result->modifiers = modifiers;
		result->all_constant = all_constant;
		return std::move(result);
	}
};

unique_ptr<FunctionData> CreateSortKeyBind(BindScalarFunctionInput &input) {
	auto &arguments = input.GetArguments();
	auto &function = input.GetBoundFunction();

	if (arguments.size() % 2 != 0) {
		throw BinderException(
		    "Arguments to create_sort_key must be [key1, sort_specifier1, key2, sort_specifier2, ...]");
	}
	auto result = make_uniq<SortKeyBindData>();
	for (idx_t i = 1; i < arguments.size(); i += 2) {
		if (!arguments[i]->IsFoldable()) {
			throw BinderException("sort_specifier must be a constant value - but got %s", arguments[i]->ToString());
		}

		// Rebind to return a date if we are truncating that far
		Value sort_specifier = ExpressionExecutor::EvaluateScalar(input.GetClientContext(), *arguments[i]);
		if (sort_specifier.IsNull()) {
			throw BinderException("sort_specifier cannot be NULL");
		}
		auto sort_specifier_str = sort_specifier.ToString();
		result->modifiers.push_back(OrderModifiers::Parse(sort_specifier_str));
	}
	// check if all types are constant
	bool all_constant = true;
	idx_t constant_size = 0;
	for (idx_t i = 0; i < arguments.size(); i += 2) {
		auto physical_type = arguments[i]->GetReturnType().InternalType();
		if (!TypeIsConstantSize(physical_type)) {
			all_constant = false;
		} else {
			// we always add one byte for the validity
			constant_size += GetTypeIdSize(physical_type) + 1;
		}
	}
	if (all_constant) {
		if (constant_size <= sizeof(int64_t)) {
			function.SetReturnType(LogicalType::BIGINT);
		}
	}
	result->all_constant = all_constant;
	return std::move(result);
}

//===--------------------------------------------------------------------===//
// Operators
//===--------------------------------------------------------------------===//
struct SortKeyVectorData {
	static constexpr data_t NULL_FIRST_BYTE = 1;
	static constexpr data_t NULL_LAST_BYTE = 2;
	static constexpr data_t STRING_DELIMITER = 0;
	static constexpr data_t LIST_DELIMITER = 0;
	static constexpr data_t BLOB_ESCAPE_CHARACTER = 1;

	SortKeyVectorData(const Vector &input, idx_t size, OrderModifiers modifiers) : vec(input) {
		if (size != 0) {
			input.ToUnifiedFormat(format);
		} else {
			format.physical_type = input.GetType().InternalType();
		}
		this->size = size;

		null_byte = NULL_FIRST_BYTE;
		valid_byte = NULL_LAST_BYTE;
		if (modifiers.null_type == OrderByNullType::NULLS_LAST) {
			std::swap(null_byte, valid_byte);
		}

		// NULLS FIRST/NULLS LAST passed in by the user are only respected at the top level
		// within nested types NULLS LAST/NULLS FIRST is dependent on ASC/DESC order instead
		// don't blame me this is what Postgres does
		auto child_null_type =
		    modifiers.order_type == OrderType::ASCENDING ? OrderByNullType::NULLS_LAST : OrderByNullType::NULLS_FIRST;
		OrderModifiers child_modifiers(modifiers.order_type, child_null_type);
		switch (input.GetType().InternalType()) {
		case PhysicalType::STRUCT: {
			auto &children = StructVector::GetEntries(input);
			for (auto &child : children) {
				child_data.push_back(make_uniq<SortKeyVectorData>(child, size, child_modifiers));
			}
			break;
		}
		case PhysicalType::ARRAY: {
			const auto &child_entry = ArrayVector::GetChild(input);
			auto array_size = ArrayType::GetSize(input.GetType());
			child_data.push_back(make_uniq<SortKeyVectorData>(child_entry, size * array_size, child_modifiers));
			break;
		}
		case PhysicalType::LIST: {
			const auto &child_entry = ListVector::GetChild(input);
			auto child_size = size == 0 ? 0 : ListVector::GetListSize(input);
			child_data.push_back(make_uniq<SortKeyVectorData>(child_entry, child_size, child_modifiers));
			break;
		}
		default:
			break;
		}
	}
	// disable copy constructors
	SortKeyVectorData(const SortKeyVectorData &other) = delete;
	SortKeyVectorData &operator=(const SortKeyVectorData &) = delete;

	void Initialize() {
	}

	PhysicalType GetPhysicalType() {
		return vec.GetType().InternalType();
	}

	const Vector &vec;
	idx_t size;
	UnifiedVectorFormat format;
	vector<unique_ptr<SortKeyVectorData>> child_data;
	data_t null_byte;
	data_t valid_byte;
};

template <class T>
struct SortKeyConstantOperator {
	using TYPE = T;

	static idx_t GetEncodeLength(TYPE input) {
		return sizeof(T);
	}

	static idx_t Encode(data_ptr_t result, TYPE input) {
		Radix::EncodeData<T>(result, input);
		return sizeof(T);
	}

	template <bool FLIP_BYTES>
	static idx_t Encode(data_ptr_t result, TYPE input) {
		Radix::EncodeData<T>(result, input);
		if (FLIP_BYTES) {
			for (idx_t b = 0; b < sizeof(T); b++) {
				result[b] = static_cast<data_t>(~result[b]);
			}
		}
		return sizeof(T);
	}

	static idx_t Decode(const_data_ptr_t input, Vector &result, TYPE &result_value, bool flip_bytes) {
		if (flip_bytes) {
			// descending order - so flip bytes
			data_t flipped_bytes[sizeof(T)];
			for (idx_t b = 0; b < sizeof(T); b++) {
				flipped_bytes[b] = static_cast<data_t>(~input[b]);
			}
			result_value = Radix::DecodeData<T>(flipped_bytes);
		} else {
			result_value = Radix::DecodeData<T>(input);
		}
		return sizeof(T);
	}

	template <bool FLIP_BYTES>
	static idx_t Decode(const_data_ptr_t input, idx_t input_size, Vector &result, TYPE &result_value) {
		D_ASSERT(input_size >= sizeof(T));
		if (FLIP_BYTES) {
			// descending order - so flip bytes
			data_t flipped_bytes[sizeof(T)];
			for (idx_t b = 0; b < sizeof(T); b++) {
				flipped_bytes[b] = static_cast<data_t>(~input[b]);
			}
			result_value = Radix::DecodeData<T>(flipped_bytes);
		} else {
			result_value = Radix::DecodeData<T>(input);
		}
		return sizeof(T);
	}
};

struct SortKeyVarcharOperator {
	using TYPE = string_t;

	static idx_t GetEncodeLength(TYPE input) {
		return input.GetSize() + 1;
	}

	static idx_t Encode(data_ptr_t result, TYPE input) {
		auto input_data = const_data_ptr_cast(input.GetDataUnsafe());
		auto input_size = input.GetSize();
		for (idx_t r = 0; r < input_size; r++) {
			result[r] = input_data[r] + 1;
		}
		result[input_size] = SortKeyVectorData::STRING_DELIMITER; // null-byte delimiter
		return input_size + 1;
	}

	template <bool FLIP_BYTES>
	static idx_t Encode(data_ptr_t result, TYPE input) {
		auto input_data = const_data_ptr_cast(input.GetDataUnsafe());
		auto input_size = input.GetSize();
		for (idx_t r = 0; r < input_size; r++) {
			auto encoded_byte = static_cast<data_t>(input_data[r] + 1);
			result[r] = FLIP_BYTES ? static_cast<data_t>(~encoded_byte) : encoded_byte;
		}
		auto delimiter = SortKeyVectorData::STRING_DELIMITER;
		result[input_size] = FLIP_BYTES ? static_cast<data_t>(~delimiter) : delimiter;
		return input_size + 1;
	}

	static idx_t Decode(const_data_ptr_t input, Vector &result, TYPE &result_value, bool flip_bytes) {
		// iterate until we encounter the string delimiter to figure out the string length
		data_t string_delimiter = SortKeyVectorData::STRING_DELIMITER;
		if (flip_bytes) {
			string_delimiter = static_cast<data_t>(~string_delimiter);
		}
		idx_t pos;
		for (pos = 0; input[pos] != string_delimiter; pos++) {
		}
		idx_t str_len = pos;
		// now allocate the string data and fill it with the decoded data
		result_value = StringVector::EmptyString(result, str_len);
		auto str_data = data_ptr_cast(result_value.GetDataWriteable());
		for (pos = 0; pos < str_len; pos++) {
			if (flip_bytes) {
				str_data[pos] = static_cast<data_t>((~input[pos]) - 1);
			} else {
				str_data[pos] = static_cast<data_t>(input[pos] - 1);
			}
		}
		result_value.Finalize();
		return pos + 1;
	}

	template <bool FLIP_BYTES>
	static idx_t Decode(const_data_ptr_t input, idx_t input_size, Vector &result, TYPE &result_value) {
		// iterate until we encounter the string delimiter to figure out the string length
		data_t string_delimiter = SortKeyVectorData::STRING_DELIMITER;
		if (FLIP_BYTES) {
			string_delimiter = static_cast<data_t>(~string_delimiter);
		}
		idx_t pos;
		for (pos = 0; pos < input_size && input[pos] != string_delimiter; pos++) {
		}
		D_ASSERT(pos < input_size);
		idx_t str_len = pos;
		// now allocate the string data and fill it with the decoded data
		result_value = StringVector::EmptyString(result, str_len);
		auto str_data = data_ptr_cast(result_value.GetDataWriteable());
		for (pos = 0; pos < str_len; pos++) {
			if (FLIP_BYTES) {
				str_data[pos] = static_cast<data_t>((~input[pos]) - 1);
			} else {
				str_data[pos] = static_cast<data_t>(input[pos] - 1);
			}
		}
		result_value.Finalize();
		return pos + 1;
	}
};

struct SortKeyBlobOperator {
	using TYPE = string_t;

	static idx_t GetEncodeLength(TYPE input) {
		auto input_data = data_ptr_t(input.GetDataUnsafe());
		auto input_size = input.GetSize();
		idx_t escaped_characters = 0;
		for (idx_t r = 0; r < input_size; r++) {
			if (input_data[r] <= 1) {
				// we escape both \x00 and \x01
				escaped_characters++;
			}
		}
		return input.GetSize() + escaped_characters + 1;
	}

	static idx_t Encode(data_ptr_t result, TYPE input) {
		auto input_data = data_ptr_t(input.GetDataUnsafe());
		auto input_size = input.GetSize();
		idx_t result_offset = 0;
		for (idx_t r = 0; r < input_size; r++) {
			if (input_data[r] <= 1) {
				// we escape both \x00 and \x01 with \x01
				result[result_offset++] = SortKeyVectorData::BLOB_ESCAPE_CHARACTER;
				result[result_offset++] = input_data[r];
			} else {
				result[result_offset++] = input_data[r];
			}
		}
		result[result_offset++] = SortKeyVectorData::STRING_DELIMITER; // null-byte delimiter
		return result_offset;
	}

	template <bool FLIP_BYTES>
	static idx_t Encode(data_ptr_t result, TYPE input) {
		auto input_data = data_ptr_t(input.GetDataUnsafe());
		auto input_size = input.GetSize();
		idx_t result_offset = 0;
		for (idx_t r = 0; r < input_size; r++) {
			if (input_data[r] <= 1) {
				auto escape = SortKeyVectorData::BLOB_ESCAPE_CHARACTER;
				result[result_offset++] = FLIP_BYTES ? static_cast<data_t>(~escape) : escape;
			}
			result[result_offset++] = FLIP_BYTES ? static_cast<data_t>(~input_data[r]) : input_data[r];
		}
		auto delimiter = SortKeyVectorData::STRING_DELIMITER;
		result[result_offset++] = FLIP_BYTES ? static_cast<data_t>(~delimiter) : delimiter;
		return result_offset;
	}

	static idx_t Decode(const_data_ptr_t input, Vector &result, TYPE &result_value, bool flip_bytes) {
		// scan until we find the delimiter, keeping in mind escapes
		data_t string_delimiter = SortKeyVectorData::STRING_DELIMITER;
		data_t escape_character = SortKeyVectorData::BLOB_ESCAPE_CHARACTER;
		if (flip_bytes) {
			string_delimiter = static_cast<data_t>(~string_delimiter);
			escape_character = static_cast<data_t>(~escape_character);
		}
		idx_t blob_len = 0;
		idx_t pos;
		for (pos = 0; input[pos] != string_delimiter; pos++) {
			blob_len++;
			if (input[pos] == escape_character) {
				// escape character - skip the next byte
				pos++;
			}
		}
		// now allocate the blob data and fill it with the decoded data
		result_value = StringVector::EmptyString(result, blob_len);
		auto str_data = data_ptr_cast(result_value.GetDataWriteable());
		for (idx_t input_pos = 0, result_pos = 0; input_pos < pos; input_pos++) {
			if (input[input_pos] == escape_character) {
				// if we encounter an escape character - copy the NEXT byte
				input_pos++;
			}
			if (flip_bytes) {
				str_data[result_pos++] = static_cast<data_t>(~input[input_pos]);
			} else {
				str_data[result_pos++] = input[input_pos];
			}
		}
		result_value.Finalize();
		return pos + 1;
	}

	template <bool FLIP_BYTES>
	static idx_t Decode(const_data_ptr_t input, idx_t input_size, Vector &result, TYPE &result_value) {
		// scan until we find the delimiter, keeping in mind escapes
		data_t string_delimiter = SortKeyVectorData::STRING_DELIMITER;
		data_t escape_character = SortKeyVectorData::BLOB_ESCAPE_CHARACTER;
		if (FLIP_BYTES) {
			string_delimiter = static_cast<data_t>(~string_delimiter);
			escape_character = static_cast<data_t>(~escape_character);
		}
		idx_t blob_len = 0;
		idx_t pos;
		for (pos = 0; pos < input_size && input[pos] != string_delimiter; pos++) {
			blob_len++;
			if (input[pos] == escape_character) {
				// escape character - skip the next byte
				pos++;
				D_ASSERT(pos < input_size);
			}
		}
		D_ASSERT(pos < input_size);
		// now allocate the blob data and fill it with the decoded data
		result_value = StringVector::EmptyString(result, blob_len);
		auto str_data = data_ptr_cast(result_value.GetDataWriteable());
		for (idx_t input_pos = 0, result_pos = 0; input_pos < pos; input_pos++) {
			if (input[input_pos] == escape_character) {
				// if we encounter an escape character - copy the NEXT byte
				input_pos++;
			}
			if (FLIP_BYTES) {
				str_data[result_pos++] = static_cast<data_t>(~input[input_pos]);
			} else {
				str_data[result_pos++] = input[input_pos];
			}
		}
		result_value.Finalize();
		return pos + 1;
	}
};

//===--------------------------------------------------------------------===//
// Variant Encoding
//===--------------------------------------------------------------------===//
// VARIANT is physically a STRUCT (keys/children/values/data), but ordering it by that physical
// representation does not match the logical ordering of the values it stores. Instead we traverse
// the variant and emit a sort key per value as [type-rank][value]. Sorting is performed by type
// first and value second. Object keys are processed in string-sorted order so that objects that
// only differ in key order (e.g. {'b': 10, 'a': 1} and {'a': 1, 'b': 10}) compare equal.
//
// Types are grouped into "ranks" the way a semi-structured system (e.g. Snowflake) groups them:
// all integers, decimals and bignums fold into a single NUMBER rank (compared by numeric value) and
// FLOAT/DOUBLE fold into a single REAL rank. Everything else keeps its own rank.

//! The sort-key rank of a variant value - determines the cross-type ordering (type-first)
enum class VariantSortRank : data_t {
	NULL_VALUE = 0,
	BOOLEAN,
	NUMBER,
	REAL,
	VARCHAR,
	BLOB,
	UUID,
	DATE,
	TIME_MICROS,
	TIME_NANOS,
	TIME_MICROS_TZ,
	TIMESTAMP_SEC,
	TIMESTAMP_MILIS,
	TIMESTAMP_MICROS,
	TIMESTAMP_NANOS,
	TIMESTAMP_MICROS_TZ,
	TIMESTAMP_NANOS_TZ,
	INTERVAL,
	GEOMETRY,
	BITSTRING,
	ARRAY,
	OBJECT
};

static data_t GetVariantTypeRank(VariantLogicalType type_id) {
	switch (type_id) {
	case VariantLogicalType::VARIANT_NULL:
		return static_cast<data_t>(VariantSortRank::NULL_VALUE);
	case VariantLogicalType::BOOL_TRUE:
	case VariantLogicalType::BOOL_FALSE:
		return static_cast<data_t>(VariantSortRank::BOOLEAN);
	// all integers, decimals and bignums fold into a single NUMBER rank (compared by numeric value)
	case VariantLogicalType::INT8:
	case VariantLogicalType::INT16:
	case VariantLogicalType::INT32:
	case VariantLogicalType::INT64:
	case VariantLogicalType::INT128:
	case VariantLogicalType::UINT8:
	case VariantLogicalType::UINT16:
	case VariantLogicalType::UINT32:
	case VariantLogicalType::UINT64:
	case VariantLogicalType::UINT128:
	case VariantLogicalType::DECIMAL:
	case VariantLogicalType::BIGNUM:
		return static_cast<data_t>(VariantSortRank::NUMBER);
	// FLOAT and DOUBLE fold into a single REAL rank
	case VariantLogicalType::FLOAT:
	case VariantLogicalType::DOUBLE:
		return static_cast<data_t>(VariantSortRank::REAL);
	case VariantLogicalType::VARCHAR:
		return static_cast<data_t>(VariantSortRank::VARCHAR);
	case VariantLogicalType::BLOB:
		return static_cast<data_t>(VariantSortRank::BLOB);
	case VariantLogicalType::UUID:
		return static_cast<data_t>(VariantSortRank::UUID);
	case VariantLogicalType::DATE:
		return static_cast<data_t>(VariantSortRank::DATE);
	case VariantLogicalType::TIME_MICROS:
		return static_cast<data_t>(VariantSortRank::TIME_MICROS);
	case VariantLogicalType::TIME_NANOS:
		return static_cast<data_t>(VariantSortRank::TIME_NANOS);
	case VariantLogicalType::TIME_MICROS_TZ:
		return static_cast<data_t>(VariantSortRank::TIME_MICROS_TZ);
	case VariantLogicalType::TIMESTAMP_SEC:
		return static_cast<data_t>(VariantSortRank::TIMESTAMP_SEC);
	case VariantLogicalType::TIMESTAMP_MILIS:
		return static_cast<data_t>(VariantSortRank::TIMESTAMP_MILIS);
	case VariantLogicalType::TIMESTAMP_MICROS:
		return static_cast<data_t>(VariantSortRank::TIMESTAMP_MICROS);
	case VariantLogicalType::TIMESTAMP_NANOS:
		return static_cast<data_t>(VariantSortRank::TIMESTAMP_NANOS);
	case VariantLogicalType::TIMESTAMP_MICROS_TZ:
		return static_cast<data_t>(VariantSortRank::TIMESTAMP_MICROS_TZ);
	case VariantLogicalType::TIMESTAMP_NANOS_TZ:
		return static_cast<data_t>(VariantSortRank::TIMESTAMP_NANOS_TZ);
	case VariantLogicalType::INTERVAL:
		return static_cast<data_t>(VariantSortRank::INTERVAL);
	case VariantLogicalType::GEOMETRY:
		return static_cast<data_t>(VariantSortRank::GEOMETRY);
	case VariantLogicalType::BITSTRING:
		return static_cast<data_t>(VariantSortRank::BITSTRING);
	case VariantLogicalType::ARRAY:
		return static_cast<data_t>(VariantSortRank::ARRAY);
	case VariantLogicalType::OBJECT:
		return static_cast<data_t>(VariantSortRank::OBJECT);
	default:
		throw NotImplementedException("Variant type %s is not supported in create_sort_key",
		                              EnumUtil::ToString(type_id));
	}
}

//===--------------------------------------------------------------------===//
// NUMBER rank encoding
//===--------------------------------------------------------------------===//
// All integers, decimals and bignums fold into a single NUMBER rank and are compared by numeric
// value. To make values of different scales/widths comparable (and equal when numerically equal),
// each value is reduced to an order-preserving "decimal-scientific" form:
//
//     value = sign * 0.d1 d2 ... dk * 10^adjexp     (d1 != 0, trailing zeros stripped)
//
// and encoded as [class][adjexp][digits][terminator], where everything after the class byte is
// complemented for negatives. This makes e.g. 1::TINYINT, 1::BIGINT, 1.00::DECIMAL and 1::BIGNUM all
// encode identically, and orders them numerically (so -100.5 < 0 < 1.5 < 15 < ...).
struct VariantNumberKey {
	//! 0 = negative, 1 = zero, 2 = positive (so negatives sort before zero before positives)
	data_t number_class;
	int64_t adjusted_exponent;
	//! significant decimal digits ('0'..'9'), leading digit non-zero, trailing zeros stripped
	string digits;
};

//! Build a number key from a sign, the decimal digits of the magnitude (no leading zeros, as e.g.
//! produced by Uhugeint::ToString) and a base-10 scale (value = magnitude / 10^scale)
static VariantNumberKey BuildNumberKey(bool negative, const string &magnitude_digits, int64_t scale) {
	VariantNumberKey key;
	// find the first non-zero digit (magnitude strings have no leading zeros, but be defensive)
	idx_t first = 0;
	while (first < magnitude_digits.size() && magnitude_digits[first] == '0') {
		first++;
	}
	if (first == magnitude_digits.size()) {
		// the value is zero
		key.number_class = 1;
		key.adjusted_exponent = 0;
		return key;
	}
	key.number_class = negative ? 0 : 2;
	// position of the leading digit: (#digits after leading zeros - 1) - scale
	key.adjusted_exponent = static_cast<int64_t>(magnitude_digits.size() - first) - 1 - scale;
	// strip trailing zeros (keep at least the leading digit)
	idx_t last = magnitude_digits.size();
	while (last > first + 1 && magnitude_digits[last - 1] == '0') {
		last--;
	}
	key.digits = magnitude_digits.substr(first, last - first);
	return key;
}

template <class SINK>
static void VariantEncodeNumber(SINK &sink, const VariantNumberKey &key) {
	// the class byte is the primary discriminator and is never complemented
	sink.Write(key.number_class);
	if (key.number_class == 1) {
		// zero - the class fully describes the value
		return;
	}
	const bool negative = key.number_class == 0;
	// for negatives we complement every payload byte so that larger magnitudes sort earlier
	auto emit = [&](data_t b) {
		sink.Write(negative ? static_cast<data_t>(~b) : b);
	};

	// adjusted exponent (order-preserving signed encoding) orders magnitude
	data_t exp_buffer[sizeof(int64_t)];
	Radix::EncodeData<int64_t>(exp_buffer, key.adjusted_exponent);
	for (idx_t i = 0; i < sizeof(int64_t); i++) {
		emit(exp_buffer[i]);
	}
	// significant digits (1..10 so the 0x00 terminator is smaller than any digit -> prefix sorts first)
	for (auto c : key.digits) {
		emit(static_cast<data_t>((c - '0') + 1));
	}
	emit(0); // terminator
}

//! Decompose a 128-bit signed value (hugeint) into (sign, magnitude)
static uhugeint_t HugeintMagnitude(hugeint_t value, bool &negative) {
	negative = value.upper < 0;
	uint64_t hi = static_cast<uint64_t>(value.upper);
	uint64_t lo = value.lower;
	if (negative) {
		// two's complement negate to get the magnitude
		hi = ~hi;
		lo = ~lo;
		if (++lo == 0) {
			++hi;
		}
	}
	return uhugeint_t(hi, lo);
}

//! Decompose a variant integer value into (sign, 128-bit magnitude)
static uhugeint_t VariantIntegralMagnitude(VariantLogicalType type_id, const_data_ptr_t ptr, bool &negative) {
	negative = false;
	switch (type_id) {
	case VariantLogicalType::INT8:
		return HugeintMagnitude(hugeint_t(Load<int8_t>(ptr)), negative);
	case VariantLogicalType::INT16:
		return HugeintMagnitude(hugeint_t(Load<int16_t>(ptr)), negative);
	case VariantLogicalType::INT32:
		return HugeintMagnitude(hugeint_t(Load<int32_t>(ptr)), negative);
	case VariantLogicalType::INT64:
		return HugeintMagnitude(hugeint_t(Load<int64_t>(ptr)), negative);
	case VariantLogicalType::INT128:
		return HugeintMagnitude(Load<hugeint_t>(ptr), negative);
	case VariantLogicalType::UINT8:
		return uhugeint_t(0, Load<uint8_t>(ptr));
	case VariantLogicalType::UINT16:
		return uhugeint_t(0, Load<uint16_t>(ptr));
	case VariantLogicalType::UINT32:
		return uhugeint_t(0, Load<uint32_t>(ptr));
	case VariantLogicalType::UINT64:
		return uhugeint_t(0, Load<uint64_t>(ptr));
	case VariantLogicalType::UINT128:
		return Load<uhugeint_t>(ptr);
	default:
		throw InternalException("VariantIntegralMagnitude called on non-integer type");
	}
}

//! Compute the number key for any value in the NUMBER rank (integer, decimal or bignum)
static VariantNumberKey VariantGetNumberKey(VariantLogicalType type_id, const UnifiedVariantVectorData &variant,
                                            idx_t row, uint32_t values_idx, const_data_ptr_t value_ptr) {
	switch (type_id) {
	case VariantLogicalType::DECIMAL: {
		auto decimal_data = VariantUtils::DecodeDecimalData(variant, row, values_idx);
		hugeint_t unscaled;
		switch (decimal_data.GetPhysicalType()) {
		case PhysicalType::INT16:
			unscaled = hugeint_t(Load<int16_t>(decimal_data.value_ptr));
			break;
		case PhysicalType::INT32:
			unscaled = hugeint_t(Load<int32_t>(decimal_data.value_ptr));
			break;
		case PhysicalType::INT64:
			unscaled = hugeint_t(Load<int64_t>(decimal_data.value_ptr));
			break;
		default:
			unscaled = Load<hugeint_t>(decimal_data.value_ptr);
			break;
		}
		bool negative;
		auto magnitude = HugeintMagnitude(unscaled, negative);
		return BuildNumberKey(negative, Uhugeint::ToString(magnitude), decimal_data.scale);
	}
	case VariantLogicalType::BIGNUM: {
		auto bignum_blob = VariantUtils::DecodeStringData(variant, row, values_idx);
		auto decimal_string = Bignum::BignumToVarchar(bignum_t(bignum_blob));
		bool negative = !decimal_string.empty() && decimal_string[0] == '-';
		idx_t offset = negative ? 1 : 0;
		return BuildNumberKey(negative, decimal_string.substr(offset), 0);
	}
	default: {
		// integer types
		bool negative;
		auto magnitude = VariantIntegralMagnitude(type_id, value_ptr, negative);
		return BuildNumberKey(negative, Uhugeint::ToString(magnitude), 0);
	}
	}
}

//! Sink that only computes the encoded length of a variant value
struct VariantSortKeyLengthSink {
	idx_t length = 0;

	inline void Write(data_t) {
		length++;
	}
	inline void WriteBytes(const_data_ptr_t, idx_t count) {
		length += count;
	}
};

//! Sink that writes the encoded bytes of a variant value (flipping bytes for DESCENDING order)
struct VariantSortKeyWriteSink {
	VariantSortKeyWriteSink(data_ptr_t result_ptr, idx_t offset, bool flip_bytes)
	    : result_ptr(result_ptr), offset(offset), flip_bytes(flip_bytes) {
	}

	data_ptr_t result_ptr;
	idx_t offset;
	bool flip_bytes;

	inline void Write(data_t b) {
		result_ptr[offset++] = flip_bytes ? static_cast<data_t>(~b) : b;
	}
	inline void WriteBytes(const_data_ptr_t src, idx_t count) {
		for (idx_t i = 0; i < count; i++) {
			Write(src[i]);
		}
	}
};

template <class T, class SINK>
static void VariantEncodeFixed(SINK &sink, T value) {
	data_t buffer[sizeof(T)];
	Radix::EncodeData<T>(buffer, value);
	sink.WriteBytes(buffer, sizeof(T));
}

template <class SINK>
static void VariantEncodeString(SINK &sink, const string_t &str, bool is_varchar) {
	auto input_data = const_data_ptr_cast(str.GetData());
	auto input_size = str.GetSize();
	if (is_varchar) {
		// VARCHAR is valid UTF-8 (never contains 0xFF) so we use the +1 / null-delimiter scheme
		for (idx_t i = 0; i < input_size; i++) {
			sink.Write(static_cast<data_t>(input_data[i] + 1));
		}
	} else {
		// arbitrary bytes - escape \x00 and \x01 so they cannot collide with the delimiter
		for (idx_t i = 0; i < input_size; i++) {
			if (input_data[i] <= 1) {
				sink.Write(SortKeyVectorData::BLOB_ESCAPE_CHARACTER);
			}
			sink.Write(input_data[i]);
		}
	}
	sink.Write(SortKeyVectorData::STRING_DELIMITER);
}

template <class SINK>
static void EncodeVariantValue(const UnifiedVariantVectorData &variant, idx_t row, uint32_t values_idx, SINK &sink) {
	auto type_id = variant.GetTypeId(row, values_idx);
	// write the type rank - this guarantees values are ordered by type first
	sink.Write(GetVariantTypeRank(type_id));

	auto blob_ptr = const_data_ptr_cast(variant.GetData(row).GetData());
	auto value_ptr = blob_ptr + variant.GetByteOffset(row, values_idx);
	switch (type_id) {
	case VariantLogicalType::VARIANT_NULL:
		// no payload - the rank fully describes the value
		break;
	case VariantLogicalType::BOOL_TRUE:
		sink.Write(1);
		break;
	case VariantLogicalType::BOOL_FALSE:
		sink.Write(0);
		break;
	// all integers, decimals and bignums fold into the NUMBER rank and compare by numeric value
	case VariantLogicalType::INT8:
	case VariantLogicalType::INT16:
	case VariantLogicalType::INT32:
	case VariantLogicalType::INT64:
	case VariantLogicalType::INT128:
	case VariantLogicalType::UINT8:
	case VariantLogicalType::UINT16:
	case VariantLogicalType::UINT32:
	case VariantLogicalType::UINT64:
	case VariantLogicalType::UINT128:
	case VariantLogicalType::DECIMAL:
	case VariantLogicalType::BIGNUM:
		VariantEncodeNumber(sink, VariantGetNumberKey(type_id, variant, row, values_idx, value_ptr));
		break;
	case VariantLogicalType::FLOAT:
		// fold FLOAT into the REAL rank by widening to double (lossless)
		VariantEncodeFixed<double>(sink, static_cast<double>(Load<float>(value_ptr)));
		break;
	case VariantLogicalType::DOUBLE:
		VariantEncodeFixed<double>(sink, Load<double>(value_ptr));
		break;
	case VariantLogicalType::UUID:
		VariantEncodeFixed<hugeint_t>(sink, Load<hugeint_t>(value_ptr));
		break;
	case VariantLogicalType::DATE:
		VariantEncodeFixed<int32_t>(sink, Load<int32_t>(value_ptr));
		break;
	case VariantLogicalType::TIME_MICROS:
	case VariantLogicalType::TIME_NANOS:
	case VariantLogicalType::TIMESTAMP_SEC:
	case VariantLogicalType::TIMESTAMP_MILIS:
	case VariantLogicalType::TIMESTAMP_MICROS:
	case VariantLogicalType::TIMESTAMP_NANOS:
	case VariantLogicalType::TIMESTAMP_MICROS_TZ:
	case VariantLogicalType::TIMESTAMP_NANOS_TZ:
		// all of these are stored as a single (UTC-normalized) int64 that compares directly
		VariantEncodeFixed<int64_t>(sink, Load<int64_t>(value_ptr));
		break;
	case VariantLogicalType::TIME_MICROS_TZ:
		// TIME WITH TIME ZONE requires a dedicated byte-comparable transform
		VariantEncodeFixed<uint64_t>(sink, Load<dtime_tz_t>(value_ptr).sort_key());
		break;
	case VariantLogicalType::INTERVAL:
		// normalize the interval so that equal intervals encode identically
		VariantEncodeFixed<interval_t>(sink, Load<interval_t>(value_ptr).Normalize());
		break;
	case VariantLogicalType::VARCHAR:
		VariantEncodeString(sink, VariantUtils::DecodeStringData(variant, row, values_idx), true);
		break;
	case VariantLogicalType::BLOB:
	case VariantLogicalType::GEOMETRY:
	case VariantLogicalType::BITSTRING:
		VariantEncodeString(sink, VariantUtils::DecodeStringData(variant, row, values_idx), false);
		break;
	case VariantLogicalType::ARRAY: {
		auto nested_data = VariantUtils::DecodeNestedData(variant, row, values_idx);
		for (idx_t i = 0; i < nested_data.child_count; i++) {
			auto child_values_idx = variant.GetValuesIndex(row, nested_data.children_idx + i);
			EncodeVariantValue(variant, row, child_values_idx, sink);
		}
		sink.Write(SortKeyVectorData::LIST_DELIMITER);
		break;
	}
	case VariantLogicalType::OBJECT: {
		auto nested_data = VariantUtils::DecodeNestedData(variant, row, values_idx);
		// gather (key, value) pairs and process them in string-sorted key order so that objects
		// that only differ in key order compare equal
		vector<std::pair<string_t, uint32_t>> entries;
		entries.reserve(nested_data.child_count);
		for (idx_t i = 0; i < nested_data.child_count; i++) {
			auto key_idx = variant.GetKeysIndex(row, nested_data.children_idx + i);
			auto child_values_idx = variant.GetValuesIndex(row, nested_data.children_idx + i);
			entries.emplace_back(variant.GetKey(row, key_idx), child_values_idx);
		}
		std::sort(entries.begin(), entries.end(),
		          [](const std::pair<string_t, uint32_t> &a, const std::pair<string_t, uint32_t> &b) {
			          return a.first < b.first;
		          });
		for (auto &entry : entries) {
			VariantEncodeString(sink, entry.first, true);
			EncodeVariantValue(variant, row, entry.second, sink);
		}
		sink.Write(SortKeyVectorData::LIST_DELIMITER);
		break;
	}
	default:
		throw NotImplementedException("Variant type %s is not supported in create_sort_key",
		                              EnumUtil::ToString(type_id));
	}
}

struct SortKeyListEntry {
	static bool IsArray() {
		return false;
	}

	static list_entry_t GetListEntry(SortKeyVectorData &vector_data, idx_t idx) {
		auto data = UnifiedVectorFormat::GetData<list_entry_t>(vector_data.format);
		return data[idx];
	}
};

struct SortKeyArrayEntry {
	static bool IsArray() {
		return true;
	}

	static list_entry_t GetListEntry(SortKeyVectorData &vector_data, idx_t idx) {
		auto array_size = ArrayType::GetSize(vector_data.vec.GetType());
		return list_entry_t(array_size * idx, array_size);
	}
};

struct SortKeyChunk {
	SortKeyChunk(idx_t start, idx_t end) : start(start), end(end), has_result_index(false) {
	}
	SortKeyChunk(idx_t start, idx_t end, idx_t result_index)
	    : start(start), end(end), result_index(result_index), has_result_index(true) {
	}

	idx_t start;
	idx_t end;
	idx_t result_index;
	bool has_result_index;

	inline idx_t GetResultIndex(idx_t r) const {
		return has_result_index ? result_index : r;
	}
};

//===--------------------------------------------------------------------===//
// Get Sort Key Length
//===--------------------------------------------------------------------===//
struct SortKeyLengthInfo {
	explicit SortKeyLengthInfo(idx_t size) : constant_length(0) {
		variable_lengths.resize(size, 0);
	}

	idx_t constant_length;
	unsafe_vector<idx_t> variable_lengths;
};

static void GetSortKeyLengthRecursive(SortKeyVectorData &vector_data, SortKeyChunk chunk, SortKeyLengthInfo &result);

template <class OP>
void TemplatedGetSortKeyLength(SortKeyVectorData &vector_data, SortKeyChunk chunk, SortKeyLengthInfo &result) {
	auto &format = vector_data.format;
	auto data = UnifiedVectorFormat::GetData<typename OP::TYPE>(vector_data.format);
	for (idx_t r = chunk.start; r < chunk.end; r++) {
		auto idx = format.sel->get_index(r);
		auto result_index = chunk.GetResultIndex(r);
		result.variable_lengths[result_index]++; // every value is prefixed by a validity byte

		if (!format.validity.RowIsValid(idx)) {
			continue;
		}
		result.variable_lengths[result_index] += OP::GetEncodeLength(data[idx]);
	}
}

void GetSortKeyLengthStruct(SortKeyVectorData &vector_data, SortKeyChunk chunk, SortKeyLengthInfo &result) {
	for (idx_t r = chunk.start; r < chunk.end; r++) {
		auto result_index = chunk.GetResultIndex(r);
		result.variable_lengths[result_index]++; // every struct is prefixed by a validity byte
	}
	// now recursively call GetSortKeyLength on the child elements
	for (auto &child_data : vector_data.child_data) {
		GetSortKeyLengthRecursive(*child_data, chunk, result);
	}
}

template <class OP>
void GetSortKeyLengthList(SortKeyVectorData &vector_data, SortKeyChunk chunk, SortKeyLengthInfo &result) {
	auto &child_data = vector_data.child_data[0];
	for (idx_t r = chunk.start; r < chunk.end; r++) {
		auto idx = vector_data.format.sel->get_index(r);
		auto result_index = chunk.GetResultIndex(r);
		result.variable_lengths[result_index]++; // every list is prefixed by a validity byte

		if (!vector_data.format.validity.RowIsValid(idx)) {
			if (!OP::IsArray()) {
				// for arrays we need to fill in the child vector for all elements, even if the top-level array is NULL
				continue;
			}
		}
		auto list_entry = OP::GetListEntry(vector_data, idx);
		// for each non-null list we have an "end of list" delimiter
		result.variable_lengths[result_index]++;
		if (list_entry.length > 0) {
			// recursively call GetSortKeyLength for the children of this list
			SortKeyChunk child_chunk(list_entry.offset, list_entry.offset + list_entry.length, result_index);
			GetSortKeyLengthRecursive(*child_data, child_chunk, result);
		}
	}
}

void GetSortKeyLengthRecursive(SortKeyVectorData &vector_data, SortKeyChunk chunk, SortKeyLengthInfo &result) {
	auto physical_type = vector_data.GetPhysicalType();
	// handle variable lengths
	switch (physical_type) {
	case PhysicalType::BOOL:
		TemplatedGetSortKeyLength<SortKeyConstantOperator<bool>>(vector_data, chunk, result);
		break;
	case PhysicalType::UINT8:
		TemplatedGetSortKeyLength<SortKeyConstantOperator<uint8_t>>(vector_data, chunk, result);
		break;
	case PhysicalType::INT8:
		TemplatedGetSortKeyLength<SortKeyConstantOperator<int8_t>>(vector_data, chunk, result);
		break;
	case PhysicalType::UINT16:
		TemplatedGetSortKeyLength<SortKeyConstantOperator<uint16_t>>(vector_data, chunk, result);
		break;
	case PhysicalType::INT16:
		TemplatedGetSortKeyLength<SortKeyConstantOperator<int16_t>>(vector_data, chunk, result);
		break;
	case PhysicalType::UINT32:
		TemplatedGetSortKeyLength<SortKeyConstantOperator<uint32_t>>(vector_data, chunk, result);
		break;
	case PhysicalType::INT32:
		TemplatedGetSortKeyLength<SortKeyConstantOperator<int32_t>>(vector_data, chunk, result);
		break;
	case PhysicalType::UINT64:
		TemplatedGetSortKeyLength<SortKeyConstantOperator<uint64_t>>(vector_data, chunk, result);
		break;
	case PhysicalType::INT64:
		TemplatedGetSortKeyLength<SortKeyConstantOperator<int64_t>>(vector_data, chunk, result);
		break;
	case PhysicalType::FLOAT:
		TemplatedGetSortKeyLength<SortKeyConstantOperator<float>>(vector_data, chunk, result);
		break;
	case PhysicalType::DOUBLE:
		TemplatedGetSortKeyLength<SortKeyConstantOperator<double>>(vector_data, chunk, result);
		break;
	case PhysicalType::INTERVAL:
		TemplatedGetSortKeyLength<SortKeyConstantOperator<interval_t>>(vector_data, chunk, result);
		break;
	case PhysicalType::UINT128:
		TemplatedGetSortKeyLength<SortKeyConstantOperator<uhugeint_t>>(vector_data, chunk, result);
		break;
	case PhysicalType::INT128:
		TemplatedGetSortKeyLength<SortKeyConstantOperator<hugeint_t>>(vector_data, chunk, result);
		break;
	case PhysicalType::VARCHAR:
		if (vector_data.vec.GetType().id() == LogicalTypeId::VARCHAR) {
			TemplatedGetSortKeyLength<SortKeyVarcharOperator>(vector_data, chunk, result);
		} else {
			TemplatedGetSortKeyLength<SortKeyBlobOperator>(vector_data, chunk, result);
		}
		break;
	case PhysicalType::STRUCT:
		GetSortKeyLengthStruct(vector_data, chunk, result);
		break;
	case PhysicalType::LIST:
		GetSortKeyLengthList<SortKeyListEntry>(vector_data, chunk, result);
		break;
	case PhysicalType::ARRAY:
		GetSortKeyLengthList<SortKeyArrayEntry>(vector_data, chunk, result);
		break;
	default:
		throw NotImplementedException("Unsupported physical type %s in GetSortKeyLength", physical_type);
	}
}

void GetSortKeyLength(SortKeyVectorData &vector_data, SortKeyLengthInfo &result, SortKeyChunk chunk) {
	// top-level method
	auto physical_type = vector_data.GetPhysicalType();
	if (TypeIsConstantSize(physical_type)) {
		// every row is prefixed by a validity byte
		result.constant_length += 1;
		result.constant_length += GetTypeIdSize(physical_type);
		return;
	}
	GetSortKeyLengthRecursive(vector_data, chunk, result);
}

void GetSortKeyLength(SortKeyVectorData &vector_data, SortKeyLengthInfo &result) {
	GetSortKeyLength(vector_data, result, SortKeyChunk(0, vector_data.size));
}

//===--------------------------------------------------------------------===//
// Construct Sort Key
//===--------------------------------------------------------------------===//
struct SortKeyConstructInfo {
	SortKeyConstructInfo(OrderModifiers modifiers_p, unsafe_vector<idx_t> &offsets, data_ptr_t *result_data)
	    : modifiers(modifiers_p), offsets(offsets), result_data(result_data) {
		flip_bytes = modifiers.order_type == OrderType::DESCENDING;
	}

	OrderModifiers modifiers;
	unsafe_vector<idx_t> &offsets;
	data_ptr_t *result_data;
	bool flip_bytes;
};

static void ConstructSortKeyRecursive(SortKeyVectorData &vector_data, SortKeyChunk chunk, SortKeyConstructInfo &info);

template <class OP, bool ALL_VALID, bool NO_SELS, bool FLIP_BYTES>
void TemplatedConstructSortKeyInternal(const SortKeyVectorData &vector_data, const SortKeyChunk chunk,
                                       const SortKeyConstructInfo &info) {
	auto data = UnifiedVectorFormat::GetData<typename OP::TYPE>(vector_data.format);
	auto &offsets = info.offsets;
	for (idx_t r = chunk.start; r < chunk.end; r++) {
		const auto result_index = NO_SELS ? r : chunk.GetResultIndex(r);
		const auto idx = NO_SELS ? r : vector_data.format.sel->get_index(r);
		auto offset = offsets[result_index];
		auto result_ptr = info.result_data[result_index];
		if (!ALL_VALID && !vector_data.format.validity.RowIsValidUnsafe(idx)) {
			// NULL value - write the null byte and skip
			result_ptr[offset++] = vector_data.null_byte;
			offsets[result_index] = offset;
			continue;
		}
		// valid value - write the validity byte
		result_ptr[offset++] = vector_data.valid_byte;
		idx_t encode_len = OP::template Encode<FLIP_BYTES>(result_ptr + offset, data[idx]);
		offset += encode_len;
		offsets[result_index] = offset;
	}
}

template <class OP, bool ALL_VALID, bool NO_SELS>
void TemplatedConstructSortKeyDispatchFlip(SortKeyVectorData &vector_data, SortKeyChunk chunk,
                                           SortKeyConstructInfo &info) {
	if (info.flip_bytes) {
		TemplatedConstructSortKeyInternal<OP, ALL_VALID, NO_SELS, true>(vector_data, chunk, info);
	} else {
		TemplatedConstructSortKeyInternal<OP, ALL_VALID, NO_SELS, false>(vector_data, chunk, info);
	}
}

template <class OP>
void TemplatedConstructSortKey(SortKeyVectorData &vector_data, SortKeyChunk chunk, SortKeyConstructInfo &info) {
	if (chunk.start == chunk.end) {
		return;
	}
	const auto all_valid = vector_data.format.validity.CannotHaveNull();
	const auto no_sels = !chunk.has_result_index && !vector_data.format.sel->IsSet();
	if (all_valid && no_sels) {
		TemplatedConstructSortKeyDispatchFlip<OP, true, true>(vector_data, chunk, info);
	} else if (all_valid) {
		TemplatedConstructSortKeyDispatchFlip<OP, true, false>(vector_data, chunk, info);
	} else if (no_sels) {
		TemplatedConstructSortKeyDispatchFlip<OP, false, true>(vector_data, chunk, info);
	} else {
		TemplatedConstructSortKeyDispatchFlip<OP, false, false>(vector_data, chunk, info);
	}
}

void ConstructSortKeyStruct(SortKeyVectorData &vector_data, SortKeyChunk chunk, SortKeyConstructInfo &info) {
	bool list_of_structs = chunk.has_result_index;
	// write the validity data of the struct
	auto &offsets = info.offsets;
	for (idx_t r = chunk.start; r < chunk.end; r++) {
		auto result_index = chunk.GetResultIndex(r);
		auto idx = vector_data.format.sel->get_index(r);
		auto &offset = offsets[result_index];
		auto result_ptr = info.result_data[result_index];
		if (!vector_data.format.validity.RowIsValid(idx)) {
			// NULL value - write the null byte and skip
			result_ptr[offset++] = vector_data.null_byte;
		} else {
			// valid value - write the validity byte
			result_ptr[offset++] = vector_data.valid_byte;
		}
		if (list_of_structs) {
			// for a list of structs we need to write the child data for every iteration
			// since the final layout needs to be
			// [struct1][struct2][...]
			for (auto &child : vector_data.child_data) {
				SortKeyChunk child_chunk(r, r + 1, result_index);
				ConstructSortKeyRecursive(*child, child_chunk, info);
			}
		}
	}
	if (!list_of_structs) {
		for (auto &child : vector_data.child_data) {
			ConstructSortKeyRecursive(*child, chunk, info);
		}
	}
}

template <class OP>
void ConstructSortKeyList(SortKeyVectorData &vector_data, SortKeyChunk chunk, SortKeyConstructInfo &info) {
	auto &offsets = info.offsets;
	for (idx_t r = chunk.start; r < chunk.end; r++) {
		auto result_index = chunk.GetResultIndex(r);
		auto idx = vector_data.format.sel->get_index(r);
		auto &offset = offsets[result_index];
		auto result_ptr = info.result_data[result_index];
		if (!vector_data.format.validity.RowIsValid(idx)) {
			// NULL value - write the null byte and skip
			result_ptr[offset++] = vector_data.null_byte;
			if (!OP::IsArray()) {
				// for arrays we always write the child elements - also if the top-level array is NULL
				continue;
			}
		} else {
			// valid value - write the validity byte
			result_ptr[offset++] = vector_data.valid_byte;
		}

		auto list_entry = OP::GetListEntry(vector_data, idx);
		// recurse and write the list elements
		if (list_entry.length > 0) {
			SortKeyChunk child_chunk(list_entry.offset, list_entry.offset + list_entry.length, result_index);
			ConstructSortKeyRecursive(*vector_data.child_data[0], child_chunk, info);
		}

		// write the end-of-list delimiter
		result_ptr[offset++] = static_cast<data_t>(info.flip_bytes ? ~SortKeyVectorData::LIST_DELIMITER
		                                                           : SortKeyVectorData::LIST_DELIMITER);
	}
}

void ConstructSortKeyRecursive(SortKeyVectorData &vector_data, SortKeyChunk chunk, SortKeyConstructInfo &info) {
	switch (vector_data.GetPhysicalType()) {
	case PhysicalType::BOOL:
		TemplatedConstructSortKey<SortKeyConstantOperator<bool>>(vector_data, chunk, info);
		break;
	case PhysicalType::UINT8:
		TemplatedConstructSortKey<SortKeyConstantOperator<uint8_t>>(vector_data, chunk, info);
		break;
	case PhysicalType::INT8:
		TemplatedConstructSortKey<SortKeyConstantOperator<int8_t>>(vector_data, chunk, info);
		break;
	case PhysicalType::UINT16:
		TemplatedConstructSortKey<SortKeyConstantOperator<uint16_t>>(vector_data, chunk, info);
		break;
	case PhysicalType::INT16:
		TemplatedConstructSortKey<SortKeyConstantOperator<int16_t>>(vector_data, chunk, info);
		break;
	case PhysicalType::UINT32:
		TemplatedConstructSortKey<SortKeyConstantOperator<uint32_t>>(vector_data, chunk, info);
		break;
	case PhysicalType::INT32:
		TemplatedConstructSortKey<SortKeyConstantOperator<int32_t>>(vector_data, chunk, info);
		break;
	case PhysicalType::UINT64:
		TemplatedConstructSortKey<SortKeyConstantOperator<uint64_t>>(vector_data, chunk, info);
		break;
	case PhysicalType::INT64:
		TemplatedConstructSortKey<SortKeyConstantOperator<int64_t>>(vector_data, chunk, info);
		break;
	case PhysicalType::FLOAT:
		TemplatedConstructSortKey<SortKeyConstantOperator<float>>(vector_data, chunk, info);
		break;
	case PhysicalType::DOUBLE:
		TemplatedConstructSortKey<SortKeyConstantOperator<double>>(vector_data, chunk, info);
		break;
	case PhysicalType::INTERVAL:
		TemplatedConstructSortKey<SortKeyConstantOperator<interval_t>>(vector_data, chunk, info);
		break;
	case PhysicalType::UINT128:
		TemplatedConstructSortKey<SortKeyConstantOperator<uhugeint_t>>(vector_data, chunk, info);
		break;
	case PhysicalType::INT128:
		TemplatedConstructSortKey<SortKeyConstantOperator<hugeint_t>>(vector_data, chunk, info);
		break;
	case PhysicalType::VARCHAR:
		if (vector_data.vec.GetType().id() == LogicalTypeId::VARCHAR) {
			TemplatedConstructSortKey<SortKeyVarcharOperator>(vector_data, chunk, info);
		} else {
			TemplatedConstructSortKey<SortKeyBlobOperator>(vector_data, chunk, info);
		}
		break;
	case PhysicalType::STRUCT:
		ConstructSortKeyStruct(vector_data, chunk, info);
		break;
	case PhysicalType::LIST:
		ConstructSortKeyList<SortKeyListEntry>(vector_data, chunk, info);
		break;
	case PhysicalType::ARRAY:
		ConstructSortKeyList<SortKeyArrayEntry>(vector_data, chunk, info);
		break;
	default:
		throw NotImplementedException("Unsupported type %s in ConstructSortKey", vector_data.vec.GetType());
	}
}

void ConstructSortKey(SortKeyVectorData &vector_data, SortKeyConstructInfo &info) {
	ConstructSortKeyRecursive(vector_data, SortKeyChunk(0, vector_data.size), info);
}

void PrepareSortData(Vector &result, idx_t size, SortKeyLengthInfo &key_lengths, data_ptr_t *data_pointers,
                     const bool &all_constant) {
	switch (result.GetType().id()) {
	case LogicalTypeId::BLOB: {
		auto result_data = FlatVector::Writer<string_t>(result, size);
		if (all_constant && key_lengths.constant_length <= string_t::INLINE_LENGTH) {
			// Fast path
			const auto length = key_lengths.constant_length;
			for (idx_t r = 0; r < size; r++) {
				auto &empty_string = result_data.WriteEmptyString(length);
				data_pointers[r] = data_ptr_cast(empty_string.GetPrefixWriteable());
#ifdef DEBUG
				memset(data_pointers[r], 0xFF, length);
#endif
			}
		} else {
			for (idx_t r = 0; r < size; r++) {
				auto blob_size = key_lengths.variable_lengths[r] + key_lengths.constant_length;
				auto &empty_string = result_data.WriteEmptyString(blob_size);
				data_pointers[r] = data_ptr_cast(empty_string.GetDataWriteable());
#ifdef DEBUG
				memset(data_pointers[r], 0xFF, blob_size);
#endif
			}
		}
		break;
	}
	case LogicalTypeId::BIGINT: {
		auto result_data = FlatVector::GetDataMutable<int64_t>(result);
		for (idx_t r = 0; r < size; r++) {
			result_data[r] = 0;
			data_pointers[r] = data_ptr_cast(&result_data[r]);
		}
		break;
	}
	default:
		throw InternalException("Unsupported key type for CreateSortKey");
	}
}

void FinalizeSortData(Vector &result, idx_t size, const SortKeyLengthInfo &key_lengths,
                      const unsafe_vector<idx_t> &offsets) {
	switch (result.GetType().id()) {
	case LogicalTypeId::BLOB: {
		auto result_data = FlatVector::GetDataMutable<string_t>(result);
		// call Finalize on the result
		for (idx_t r = 0; r < size; r++) {
			result_data[r].SetSizeAndFinalize(UnsafeNumericCast<uint32_t>(offsets[r]),
			                                  key_lengths.variable_lengths[r] + key_lengths.constant_length);
		}
		break;
	}
	case LogicalTypeId::BIGINT: {
		auto result_data = FlatVector::GetDataMutable<int64_t>(result);
		for (idx_t r = 0; r < size; r++) {
			result_data[r] = BSwapIfLE(result_data[r]);
		}
		break;
	}
	default:
		throw InternalException("Unsupported key type for CreateSortKey");
	}
}

void CreateSortKeyInternal(vector<unique_ptr<SortKeyVectorData>> &sort_key_data,
                           const vector<OrderModifiers> &modifiers, const bool &all_constant, Vector &result,
                           idx_t row_count) {
	// two phases
	// a) get the length of the final sorted key
	// b) allocate the sorted key and construct
	// we do all of this in a vectorized manner
	SortKeyLengthInfo key_lengths(row_count);
	for (auto &vector_data : sort_key_data) {
		GetSortKeyLength(*vector_data, key_lengths);
	}
	// allocate the empty sort keys
	auto data_pointers = unique_ptr<data_ptr_t[]>(new data_ptr_t[row_count]);
	PrepareSortData(result, row_count, key_lengths, data_pointers.get(), all_constant);

	unsafe_vector<idx_t> offsets;
	offsets.resize(row_count, 0);
	// now construct the sort keys
	for (idx_t c = 0; c < sort_key_data.size(); c++) {
		SortKeyConstructInfo info(modifiers[c], offsets, data_pointers.get());
		ConstructSortKey(*sort_key_data[c], info);
	}
	FinalizeSortData(result, row_count, key_lengths, offsets);
}

#ifdef DEBUG
static void AssertSortKeyRoundTrip(vector<unique_ptr<SortKeyVectorData>> &sort_key_data,
                                   const vector<OrderModifiers> &modifiers, const Vector &result, idx_t row_count) {
	D_ASSERT(sort_key_data.size() == modifiers.size());
	UnifiedVectorFormat result_format;
	result.ToUnifiedFormat(result_format);
	const auto result_is_blob = result.GetType() == LogicalType::BLOB;
	const auto result_blob_data = result_is_blob ? UnifiedVectorFormat::GetData<string_t>(result_format) : nullptr;
	const auto result_int_data = result_is_blob ? nullptr : UnifiedVectorFormat::GetData<int64_t>(result_format);
	idx_t constant_encoded_size = 0;
	if (!result_is_blob) {
		for (auto &column : sort_key_data) {
			constant_encoded_size += 1 + GetTypeIdSize(column->vec.GetType().InternalType());
		}
		D_ASSERT(constant_encoded_size <= sizeof(int64_t));
	}

	vector<Vector> decoded_columns;
	decoded_columns.reserve(sort_key_data.size());
	for (auto &column : sort_key_data) {
		decoded_columns.emplace_back(column->vec.GetType());
	}

	for (idx_t r = 0; r < row_count; r++) {
		auto key_idx = result_format.sel->get_index(r);
		D_ASSERT(result_format.validity.RowIsValid(key_idx));

		string_t full_key;
		int64_t bswapped_key = 0;
		if (result_is_blob) {
			full_key = result_blob_data[key_idx];
		} else {
			bswapped_key = BSwapIfLE(result_int_data[key_idx]);
			full_key = string_t(const_char_ptr_cast(reinterpret_cast<const char *>(&bswapped_key)), sizeof(int64_t));
		}

		const auto full_key_data = full_key.GetData();
		const auto full_key_size = full_key.GetSize();
		const auto expected_size = result_is_blob ? full_key_size : constant_encoded_size;
		D_ASSERT(expected_size <= full_key_size);
		idx_t offset = 0;
		for (idx_t c = 0; c < sort_key_data.size(); c++) {
			D_ASSERT(offset <= expected_size);
			const auto sliced_data = full_key_data + offset;
			const auto sliced_size = expected_size - offset;
			auto sliced_key = string_t(sliced_data, UnsafeNumericCast<uint32_t>(sliced_size));
			offset += CreateSortKeyHelpers::DecodeSortKey(sliced_key, decoded_columns[c], r, modifiers[c]);
		}
		D_ASSERT(offset <= expected_size);

		for (idx_t c = 0; c < sort_key_data.size(); c++) {
			auto &source_column = sort_key_data[c];
			auto source_val = source_column->vec.GetValue(r);
			auto decoded_val = decoded_columns[c].GetValue(r);
			D_ASSERT(source_val.IsNull() == decoded_val.IsNull());
			if (!source_val.IsNull()) {
				D_ASSERT(source_val == decoded_val);
			}
		}
	}
}
#endif

} // namespace

void CreateSortKeyHelpers::CreateSortKey(const Vector &input, OrderModifiers order_modifier, Vector &result) {
	// prepare the sort key data
	const auto input_count = input.size();
	vector<OrderModifiers> modifiers {order_modifier};
	vector<unique_ptr<SortKeyVectorData>> sort_key_data;
	sort_key_data.push_back(make_uniq<SortKeyVectorData>(input, input_count, order_modifier));

	CreateSortKeyInternal(sort_key_data, modifiers, false, result, input_count);
}

void CreateSortKeyHelpers::CreateVariantSortKey(const Vector &input, idx_t count, OrderModifiers modifiers,
                                                Vector &result) {
	// VARIANT is physically a STRUCT, but the generic create_sort_key encodes that physical
	// representation (which is reversible, so it can be used by aggregates to store/decode values).
	// For comparison / ordering we instead encode the *logical* value of the variant - this encoding
	// is intentionally not reversible (e.g. all integer widths fold together), so it lives here and is
	// only used by the variant_comparator collation. NULLs are propagated into the result validity.
	RecursiveUnifiedVectorFormat variant_format;
	Vector::RecursiveToUnifiedFormat(input, variant_format);
	UnifiedVariantVectorData variant(variant_format);

	data_t null_byte = SortKeyVectorData::NULL_FIRST_BYTE;
	data_t valid_byte = SortKeyVectorData::NULL_LAST_BYTE;
	if (modifiers.null_type == OrderByNullType::NULLS_LAST) {
		std::swap(null_byte, valid_byte);
	}
	const bool flip_bytes = modifiers.order_type == OrderType::DESCENDING;

	result.SetVectorType(VectorType::FLAT_VECTOR);
	auto result_data = FlatVector::GetDataMutable<string_t>(result);
	auto &result_validity = FlatVector::ValidityMutable(result);

	for (idx_t r = 0; r < count; r++) {
		const bool valid = variant.RowIsValid(r);
		// phase 1 - compute the encoded length
		idx_t length = 1; // validity byte
		if (valid) {
			VariantSortKeyLengthSink length_sink;
			EncodeVariantValue(variant, r, 0, length_sink);
			length += length_sink.length;
		}
		// phase 2 - allocate and construct
		result_data[r] = StringVector::EmptyString(result, length);
		auto ptr = data_ptr_cast(result_data[r].GetDataWriteable());
		if (!valid) {
			ptr[0] = null_byte;
			result_data[r].Finalize();
			// propagate NULL so that NULL = NULL stays NULL and ORDER BY ... NULLS FIRST/LAST is honored
			result_validity.SetInvalid(r);
			continue;
		}
		ptr[0] = valid_byte;
		VariantSortKeyWriteSink write_sink(ptr, 1, flip_bytes);
		EncodeVariantValue(variant, r, 0, write_sink);
		result_data[r].Finalize();
	}
}

void CreateSortKeyHelpers::CreateSortKey(DataChunk &input, const vector<OrderModifiers> &modifiers, Vector &result) {
	vector<unique_ptr<SortKeyVectorData>> sort_key_data;
	D_ASSERT(modifiers.size() == input.ColumnCount());
	for (idx_t r = 0; r < modifiers.size(); r++) {
		sort_key_data.push_back(make_uniq<SortKeyVectorData>(input.data[r], input.size(), modifiers[r]));
	}
	CreateSortKeyInternal(sort_key_data, modifiers, false, result, input.size());
}

void CreateSortKeyHelpers::CreateSortKey(const Vector &input, idx_t input_count, OrderModifiers order_modifier,
                                         Vector &result) {
	vector<OrderModifiers> modifiers {order_modifier};
	vector<unique_ptr<SortKeyVectorData>> sort_key_data;
	sort_key_data.push_back(make_uniq<SortKeyVectorData>(input, input_count, order_modifier));
	CreateSortKeyInternal(sort_key_data, modifiers, false, result, input_count);
}

void CreateSortKeyHelpers::CreateSortKeyWithValidity(const Vector &input, Vector &result,
                                                     const OrderModifiers &modifiers) {
	CreateSortKey(input, modifiers, result);
	UnifiedVectorFormat format;
	input.ToUnifiedFormat(format);
	auto &validity = FlatVector::ValidityMutable(result);

	const auto count = input.size();
	for (idx_t i = 0; i < count; i++) {
		auto idx = format.sel->get_index(i);
		if (!format.validity.RowIsValid(idx)) {
			validity.SetInvalid(i);
		}
	}
}

void CreateSortKeyHelpers::CreateSortKeyWithValidity(const Vector &input, Vector &result,
                                                     const OrderModifiers &modifiers, idx_t count) {
	CreateSortKey(input, count, modifiers, result);
	UnifiedVectorFormat format;
	input.ToUnifiedFormat(format);
	auto &validity = FlatVector::ValidityMutable(result);

	for (idx_t i = 0; i < count; i++) {
		auto idx = format.sel->get_index(i);
		if (!format.validity.RowIsValid(idx)) {
			validity.SetInvalid(i);
		}
	}
}

static void CreateSortKeyFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &bind_data = state.expr.Cast<BoundFunctionExpression>().BindInfo()->Cast<SortKeyBindData>();

	// prepare the sort key data
	vector<unique_ptr<SortKeyVectorData>> sort_key_data;
	for (idx_t c = 0; c < args.ColumnCount(); c += 2) {
		sort_key_data.push_back(make_uniq<SortKeyVectorData>(args.data[c], args.size(), bind_data.modifiers[c / 2]));
	}
	CreateSortKeyInternal(sort_key_data, bind_data.modifiers, bind_data.all_constant, result, args.size());
#ifdef DEBUG
	AssertSortKeyRoundTrip(sort_key_data, bind_data.modifiers, result, args.size());
#endif
}

//===--------------------------------------------------------------------===//
// Decode Sort Key
//===--------------------------------------------------------------------===//
namespace {

unique_ptr<FunctionData> DecodeSortKeyBind(BindScalarFunctionInput &input) {
	auto &context = input.GetClientContext();
	auto &arguments = input.GetArguments();
	auto &function = input.GetBoundFunction();

	if ((arguments.size() - 1) % 2 != 0) {
		throw BinderException(
		    "Arguments to decode_sort_key must be [sort_key, col1, sort_specifier1, col2, sort_specifier2, ...]");
	}
	bool all_constant = true;
	idx_t constant_size = 0;
	child_list_t<LogicalType> children;
	auto result = make_uniq<SortKeyBindData>();
	for (idx_t i = 1; i < arguments.size(); i += 2) {
		// Parse column definition
		const auto &col_arg = *arguments[i];
		if (!col_arg.IsFoldable()) {
			throw BinderException("col must be a constant value - but got %s", col_arg.ToString());
		}
		Value col = ExpressionExecutor::EvaluateScalar(context, col_arg);
		const auto col_list = Parser::ParseColumnList(col.ToString());
		if (col_list.LogicalColumnCount() != 1) {
			throw BinderException("decode_sort_key col must contain exactly one column");
		}
		const auto &col_def = col_list.GetColumn(PhysicalIndex(0));
		const auto &col_name = col_def.GetName();
		const auto &col_type = TransformStringToLogicalType(col_def.GetType().ToString(), context);

		// Keep track of this to validate the arguments
		const auto physical_type = col_type.InternalType();
		if (!TypeIsConstantSize(physical_type)) {
			all_constant = false;
		} else {
			// we always add one byte for the validity
			constant_size += GetTypeIdSize(physical_type) + 1;
		}

		// Add name/type to create a struct
		children.emplace_back(col_name, col_type);

		// Parse sort specifier
		const auto &specifier_arg = *arguments[i + 1];
		if (!specifier_arg.IsFoldable()) {
			throw BinderException("sort_specifier must be a constant value - but got %s", specifier_arg.ToString());
		}
		Value sort_specifier = ExpressionExecutor::EvaluateScalar(context, specifier_arg);
		if (sort_specifier.IsNull()) {
			throw BinderException("sort_specifier cannot be NULL");
		}
		const auto sort_specifier_str = sort_specifier.ToString();
		result->modifiers.push_back(OrderModifiers::Parse(sort_specifier_str));
	}

	const auto &sort_key_arg = *arguments[0];
	if (sort_key_arg.GetReturnType() == LogicalType::BIGINT) {
		if (!all_constant || constant_size > sizeof(int64_t)) {
			throw BinderException("sort_key has type BIGINT but arguments require BLOB");
		}
	} else if (sort_key_arg.GetReturnType() == LogicalType::BLOB) {
		if (all_constant && constant_size <= sizeof(int64_t)) {
			throw BinderException("sort_key has type BLOB but arguments require BIGINT");
		}
	} else {
		throw BinderException("sort_key must be either BIGINT or BLOB, got %s instead",
		                      sort_key_arg.GetReturnType().ToString());
	}
	function.SetReturnType(LogicalType::STRUCT(std::move(children)));

	return std::move(result);
}

struct DecodeSortKeyVectorData {
	DecodeSortKeyVectorData(const LogicalType &type, OrderModifiers modifiers)
	    : flip_bytes(modifiers.order_type == OrderType::DESCENDING) {
		null_byte = SortKeyVectorData::NULL_FIRST_BYTE;
		valid_byte = SortKeyVectorData::NULL_LAST_BYTE;
		if (modifiers.null_type == OrderByNullType::NULLS_LAST) {
			std::swap(null_byte, valid_byte);
		}

		// NULLS FIRST/NULLS LAST passed in by the user are only respected at the top level
		// within nested types NULLS LAST/NULLS FIRST is dependent on ASC/DESC order instead
		// don't blame me this is what Postgres does
		auto child_null_type =
		    modifiers.order_type == OrderType::ASCENDING ? OrderByNullType::NULLS_LAST : OrderByNullType::NULLS_FIRST;
		OrderModifiers child_modifiers(modifiers.order_type, child_null_type);
		switch (type.InternalType()) {
		case PhysicalType::STRUCT: {
			auto &children = StructType::GetChildTypes(type);
			for (auto &child_type : children) {
				child_data.emplace_back(child_type.second, child_modifiers);
			}
			break;
		}
		case PhysicalType::ARRAY: {
			auto &child_type = ArrayType::GetChildType(type);
			child_data.emplace_back(child_type, child_modifiers);
			break;
		}
		case PhysicalType::LIST: {
			auto &child_type = ListType::GetChildType(type);
			child_data.emplace_back(child_type, child_modifiers);
			break;
		}
		default:
			break;
		}
	}

	data_t null_byte;
	data_t valid_byte;
	vector<DecodeSortKeyVectorData> child_data;
	bool flip_bytes;
};

struct DecodeSortKeyData {
#ifdef DEBUG
	DecodeSortKeyData() : data(nullptr), size(DConstants::INVALID_INDEX), position(DConstants::INVALID_INDEX) {
	}
#else
	DecodeSortKeyData() {
	}
#endif

	explicit DecodeSortKeyData(const string_t &sort_key)
	    : data(const_data_ptr_cast(sort_key.GetData())), size(sort_key.GetSize()), position(0) {
	}

	explicit DecodeSortKeyData(const int64_t &sort_key)
	    : data(const_data_ptr_cast(&sort_key)), size(sizeof(int64_t)), position(0) {
	}

	const_data_ptr_t data;
	idx_t size;
	idx_t position;

	inline void RequireRemaining(idx_t required, const char *context) const {
		(void)context;
		D_ASSERT(position <= size);
		D_ASSERT(required <= size - position);
	}

	inline data_t ReadByte(const char *context) {
		RequireRemaining(1, context);
		if (position >= size) {
			return 0;
		}
		return data[position++];
	}
};

void DecodeSortKeyRecursive(DecodeSortKeyData decode_data[], DecodeSortKeyVectorData &vector_data, Vector &result,
                            idx_t result_offset, idx_t count);

template <class OP, bool FLIP_BYTES>
void TemplatedDecodeSortKeyInternal(DecodeSortKeyData decode_data_arr[], DecodeSortKeyVectorData &vector_data,
                                    Vector &result, const idx_t result_offset, const idx_t count) {
	const auto is_const = result.GetVectorType() == VectorType::CONSTANT_VECTOR;
	auto &result_validity = is_const ? ConstantVector::Validity(result) : FlatVector::ValidityMutable(result);
	const auto result_data = is_const ? ConstantVector::GetData<typename OP::TYPE>(result)
	                                  : FlatVector::GetDataMutable<typename OP::TYPE>(result);
	const auto null_byte = vector_data.null_byte;
	for (idx_t i = 0; i < count; i++) {
		const auto result_idx = result_offset + i;
		auto &decode_data = decode_data_arr[i];
		auto validity_byte = decode_data.ReadByte("reading validity byte");
		if (validity_byte == null_byte) {
			// NULL value
			result_validity.SetInvalid(result_idx);
			continue;
		}
		auto remaining = decode_data.size - decode_data.position;
		idx_t increment = OP::template Decode<FLIP_BYTES>(decode_data.data + decode_data.position, remaining, result,
		                                                  result_data[result_idx]);
		decode_data.position += increment;
		D_ASSERT(decode_data.position <= decode_data.size);
	}
}

template <class OP>
void TemplatedDecodeSortKey(DecodeSortKeyData decode_data_arr[], DecodeSortKeyVectorData &vector_data, Vector &result,
                            const idx_t result_offset, const idx_t count) {
	if (vector_data.flip_bytes) {
		TemplatedDecodeSortKeyInternal<OP, true>(decode_data_arr, vector_data, result, result_offset, count);
	} else {
		TemplatedDecodeSortKeyInternal<OP, false>(decode_data_arr, vector_data, result, result_offset, count);
	}
}

void DecodeSortKeyStruct(DecodeSortKeyData decode_data_arr[], DecodeSortKeyVectorData &vector_data, Vector &result,
                         const idx_t result_offset, const idx_t count) {
	const auto is_const = result.GetVectorType() == VectorType::CONSTANT_VECTOR;
	auto &result_validity = is_const ? ConstantVector::Validity(result) : FlatVector::ValidityMutable(result);
	for (idx_t i = 0; i < count; i++) {
		const auto result_idx = result_offset + i;
		auto &decode_data = decode_data_arr[i];
		// check if the top-level is valid or not
		auto validity_byte = decode_data.ReadByte("reading struct validity byte");
		if (validity_byte == vector_data.null_byte) {
			// entire struct is NULL
			// note that we still deserialize the children
			result_validity.SetInvalid(result_idx);
		}
	}
	// recurse into children
	auto &child_entries = StructVector::GetEntries(result);
	for (idx_t c = 0; c < child_entries.size(); c++) {
		auto &child_entry = child_entries[c];
		DecodeSortKeyRecursive(decode_data_arr, vector_data.child_data[c], child_entry, result_offset, count);
	}
}

void DecodeSortKeyList(DecodeSortKeyData decode_data_arr[], DecodeSortKeyVectorData &vector_data, Vector &result,
                       const idx_t result_offset, const idx_t count) {
	const auto is_const = result.GetVectorType() == VectorType::CONSTANT_VECTOR;
	auto &result_validity = is_const ? ConstantVector::Validity(result) : FlatVector::ValidityMutable(result);
	const auto list_data =
	    is_const ? ConstantVector::GetData<list_entry_t>(result) : FlatVector::GetDataMutable<list_entry_t>(result);
	auto &child_vector = ListVector::GetChildMutable(result);
	for (idx_t i = 0; i < count; i++) {
		const auto result_idx = result_offset + i;
		auto &decode_data = decode_data_arr[i];
		// check if the top-level is valid or not
		auto validity_byte = decode_data.ReadByte("reading list validity byte");
		if (validity_byte == vector_data.null_byte) {
			// entire list is NULL
			result_validity.SetInvalid(result_idx);
			continue;
		}
		// list is valid - decode child elements
		// we don't know how many there will be
		// decode child elements until we encounter the list delimiter
		data_t list_delimiter = SortKeyVectorData::LIST_DELIMITER;
		if (vector_data.flip_bytes) {
			list_delimiter = static_cast<data_t>(~list_delimiter);
		}

		// get the current list size
		auto start_list_size = ListVector::GetListSize(result);
		auto new_list_size = start_list_size;
		// loop until we find the list delimiter
		while (true) {
			decode_data.RequireRemaining(1, "scanning list delimiter");
			if (decode_data.data[decode_data.position] == list_delimiter) {
				break;
			}
			// found a valid entry here - decode it
			// first reserve space for it
			new_list_size++;
			ListVector::Reserve(result, new_list_size);

			// now decode the entry
			DecodeSortKeyRecursive(&decode_data, vector_data.child_data[0], child_vector, new_list_size - 1, 1);
		}
		// skip the list delimiter
		decode_data.ReadByte("consuming list delimiter");
		// set the list_entry_t information and update the list size
		list_data[result_idx].length = new_list_size - start_list_size;
		list_data[result_idx].offset = start_list_size;
		ListVector::SetListSize(result, new_list_size);
	}
}

void DecodeSortKeyArray(DecodeSortKeyData decode_data_arr[], DecodeSortKeyVectorData &vector_data, Vector &result,
                        const idx_t result_offset, const idx_t count) {
	const auto is_const = result.GetVectorType() == VectorType::CONSTANT_VECTOR;
	auto &result_validity = is_const ? ConstantVector::Validity(result) : FlatVector::ValidityMutable(result);
	for (idx_t i = 0; i < count; i++) {
		const auto result_idx = result_offset + i;
		auto &decode_data = decode_data_arr[i];
		// check if the top-level is valid or not
		auto validity_byte = decode_data.ReadByte("reading array validity byte");
		if (validity_byte == vector_data.null_byte) {
			// entire array is NULL
			// note that we still read the child elements
			result_validity.SetInvalid(result_idx);
		}
		// array is valid - decode child elements
		// arrays need to encode exactly array_size child elements
		// however the decoded data still contains a list delimiter
		// we use this delimiter to verify we successfully decoded the entire array
		data_t list_delimiter = SortKeyVectorData::LIST_DELIMITER;
		if (vector_data.flip_bytes) {
			list_delimiter = static_cast<data_t>(~list_delimiter);
		}
		auto &child_vector = ArrayVector::GetChildMutable(result);
		auto array_size = ArrayType::GetSize(result.GetType());

		idx_t found_elements = 0;
		auto child_start = array_size * result_idx;
		// loop until we find the list delimiter
		while (true) {
			decode_data.RequireRemaining(1, "scanning array delimiter");
			if (decode_data.data[decode_data.position] == list_delimiter) {
				break;
			}
			found_elements++;
			if (found_elements > array_size) {
				// error - found too many elements
				break;
			}
			// now decode the entry
			DecodeSortKeyRecursive(&decode_data, vector_data.child_data[0], child_vector,
			                       child_start + found_elements - 1, 1);
		}
		// skip the list delimiter
		decode_data.ReadByte("consuming array delimiter");
		if (found_elements != array_size) {
			throw InvalidInputException("Failed to decode array - found %d elements but expected %d", found_elements,
			                            array_size);
		}
	}
}

void DecodeSortKeyRecursive(DecodeSortKeyData decode_data[], DecodeSortKeyVectorData &vector_data, Vector &result,
                            const idx_t result_offset, const idx_t count) {
	switch (result.GetType().InternalType()) {
	case PhysicalType::BOOL:
		TemplatedDecodeSortKey<SortKeyConstantOperator<bool>>(decode_data, vector_data, result, result_offset, count);
		break;
	case PhysicalType::UINT8:
		TemplatedDecodeSortKey<SortKeyConstantOperator<uint8_t>>(decode_data, vector_data, result, result_offset,
		                                                         count);
		break;
	case PhysicalType::INT8:
		TemplatedDecodeSortKey<SortKeyConstantOperator<int8_t>>(decode_data, vector_data, result, result_offset, count);
		break;
	case PhysicalType::UINT16:
		TemplatedDecodeSortKey<SortKeyConstantOperator<uint16_t>>(decode_data, vector_data, result, result_offset,
		                                                          count);
		break;
	case PhysicalType::INT16:
		TemplatedDecodeSortKey<SortKeyConstantOperator<int16_t>>(decode_data, vector_data, result, result_offset,
		                                                         count);
		break;
	case PhysicalType::UINT32:
		TemplatedDecodeSortKey<SortKeyConstantOperator<uint32_t>>(decode_data, vector_data, result, result_offset,
		                                                          count);
		break;
	case PhysicalType::INT32:
		TemplatedDecodeSortKey<SortKeyConstantOperator<int32_t>>(decode_data, vector_data, result, result_offset,
		                                                         count);
		break;
	case PhysicalType::UINT64:
		TemplatedDecodeSortKey<SortKeyConstantOperator<uint64_t>>(decode_data, vector_data, result, result_offset,
		                                                          count);
		break;
	case PhysicalType::INT64:
		TemplatedDecodeSortKey<SortKeyConstantOperator<int64_t>>(decode_data, vector_data, result, result_offset,
		                                                         count);
		break;
	case PhysicalType::FLOAT:
		TemplatedDecodeSortKey<SortKeyConstantOperator<float>>(decode_data, vector_data, result, result_offset, count);
		break;
	case PhysicalType::DOUBLE:
		TemplatedDecodeSortKey<SortKeyConstantOperator<double>>(decode_data, vector_data, result, result_offset, count);
		break;
	case PhysicalType::INTERVAL:
		TemplatedDecodeSortKey<SortKeyConstantOperator<interval_t>>(decode_data, vector_data, result, result_offset,
		                                                            count);
		break;
	case PhysicalType::UINT128:
		TemplatedDecodeSortKey<SortKeyConstantOperator<uhugeint_t>>(decode_data, vector_data, result, result_offset,
		                                                            count);
		break;
	case PhysicalType::INT128:
		TemplatedDecodeSortKey<SortKeyConstantOperator<hugeint_t>>(decode_data, vector_data, result, result_offset,
		                                                           count);
		break;
	case PhysicalType::VARCHAR:
		if (result.GetType().id() == LogicalTypeId::VARCHAR) {
			TemplatedDecodeSortKey<SortKeyVarcharOperator>(decode_data, vector_data, result, result_offset, count);
		} else {
			TemplatedDecodeSortKey<SortKeyBlobOperator>(decode_data, vector_data, result, result_offset, count);
		}
		break;
	case PhysicalType::STRUCT:
		DecodeSortKeyStruct(decode_data, vector_data, result, result_offset, count);
		break;
	case PhysicalType::LIST:
		DecodeSortKeyList(decode_data, vector_data, result, result_offset, count);
		break;
	case PhysicalType::ARRAY:
		DecodeSortKeyArray(decode_data, vector_data, result, result_offset, count);
		break;
	default:
		throw NotImplementedException("Unsupported type %s in DecodeSortKey", result.GetType());
	}
}

} // namespace

idx_t CreateSortKeyHelpers::DecodeSortKey(string_t sort_key, Vector &result, idx_t result_idx,
                                          OrderModifiers modifiers) {
	DecodeSortKeyVectorData sort_key_data(result.GetType(), modifiers);
	DecodeSortKeyData decode_data(sort_key);
	DecodeSortKeyRecursive(&decode_data, sort_key_data, result, result_idx, 1);

	return decode_data.position;
}

void CreateSortKeyHelpers::DecodeSortKey(string_t sort_key, DataChunk &result, idx_t result_idx,
                                         const vector<OrderModifiers> &modifiers) {
	DecodeSortKeyData decode_data(sort_key);
	D_ASSERT(modifiers.size() == result.ColumnCount());
	for (idx_t c = 0; c < result.ColumnCount(); c++) {
		auto &vec = result.data[c];
		DecodeSortKeyVectorData vector_data(vec.GetType(), modifiers[c]);
		DecodeSortKeyRecursive(&decode_data, vector_data, vec, result_idx, 1);
	}
}

static void DecodeSortKeyFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &bind_data = state.expr.Cast<BoundFunctionExpression>().BindInfo()->Cast<SortKeyBindData>();

	const auto count = args.size();
	const auto &sort_key_vec = args.data[0];
	UnifiedVectorFormat sort_key_vec_format;
	sort_key_vec.ToUnifiedFormat(sort_key_vec_format);

	// When doing aggressive vector verification, the "sort_key_vec_format.validity.CannotHaveNull()" is not always true
	// However, all the actual values should be valid, so we assert that

	// Construct utility for all sort keys that we will decode
	vector<DecodeSortKeyData> decode_data(count);
	vector<int64_t> bswapped_ints(count);
	if (sort_key_vec.GetType() == LogicalType::BLOB) {
		const auto sort_keys = UnifiedVectorFormat::GetData<string_t>(sort_key_vec_format);
		if (sort_key_vec_format.sel->IsSet()) {
			for (idx_t i = 0; i < count; i++) {
				const auto idx = sort_key_vec_format.sel->get_index(i);
				D_ASSERT(sort_key_vec_format.validity.RowIsValid(idx));
				decode_data[i] = DecodeSortKeyData(sort_keys[idx]);
			}
		} else {
			for (idx_t i = 0; i < count; i++) {
				D_ASSERT(sort_key_vec_format.validity.RowIsValid(i));
				decode_data[i] = DecodeSortKeyData(sort_keys[i]);
			}
		}
	} else {
		D_ASSERT(sort_key_vec.GetType() == LogicalType::BIGINT);
		const auto sort_keys = UnifiedVectorFormat::GetData<int64_t>(sort_key_vec_format);
		if (sort_key_vec_format.sel->IsSet()) {
			for (idx_t i = 0; i < count; i++) {
				const auto idx = sort_key_vec_format.sel->get_index(i);
				D_ASSERT(sort_key_vec_format.validity.RowIsValid(idx));
				bswapped_ints[i] = BSwapIfLE(sort_keys[idx]);
				decode_data[i] = DecodeSortKeyData(bswapped_ints[i]);
			}
		} else {
			for (idx_t i = 0; i < count; i++) {
				D_ASSERT(sort_key_vec_format.validity.RowIsValid(i));
				bswapped_ints[i] = BSwapIfLE(sort_keys[i]);
				decode_data[i] = DecodeSortKeyData(bswapped_ints[i]);
			}
		}
	}

	// Loop through the columns
	const auto &result_type = result.GetType();
	auto &child_vectors = StructVector::GetEntries(result);
	for (idx_t c = 0; c < StructType::GetChildCount(result_type); c++) {
		auto &child_vector = child_vectors[c];
		DecodeSortKeyVectorData sort_key_data(child_vector.GetType(), bind_data.modifiers[c]);
		DecodeSortKeyRecursive(decode_data.data(), sort_key_data, child_vector, 0, count);
	}
}

//===--------------------------------------------------------------------===//
// Get Functions
//===--------------------------------------------------------------------===//
ScalarFunction CreateSortKeyFun::GetFunction() {
	ScalarFunction sort_key_function("create_sort_key", {LogicalType::ANY}, LogicalType::BLOB, CreateSortKeyFunction,
	                                 CreateSortKeyBind);
	sort_key_function.SetVarArgs(LogicalType::ANY);
	sort_key_function.SetNullHandling(FunctionNullHandling::SPECIAL_HANDLING);
	return sort_key_function;
}

ScalarFunction DecodeSortKeyFun::GetFunction() {
	ScalarFunction sort_key_function("decode_sort_key", {LogicalType::ANY, LogicalType::VARCHAR, LogicalType::VARCHAR},
	                                 LogicalType::STRUCT({{"any", LogicalType::ANY}}), DecodeSortKeyFunction,
	                                 DecodeSortKeyBind);
	sort_key_function.SetVarArgs(LogicalType::VARCHAR);
	return sort_key_function;
}

} // namespace duckdb
