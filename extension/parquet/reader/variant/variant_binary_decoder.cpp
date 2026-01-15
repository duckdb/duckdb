#include "reader/variant/variant_binary_decoder.hpp"
#include "duckdb/common/printer.hpp"
#include "utf8proc_wrapper.hpp"

#include "reader/uuid_column_reader.hpp"

#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/common/types/decimal.hpp"
#include "duckdb/common/types/uuid.hpp"
#include "duckdb/common/types/time.hpp"
#include "duckdb/common/types/date.hpp"
#include "duckdb/common/types/blob.hpp"

static constexpr uint8_t VERSION_MASK = 0xF;
static constexpr uint8_t SORTED_STRINGS_MASK = 0x1;
static constexpr uint8_t SORTED_STRINGS_SHIFT = 4;
static constexpr uint8_t OFFSET_SIZE_MINUS_ONE_MASK = 0x3;
static constexpr uint8_t OFFSET_SIZE_MINUS_ONE_SHIFT = 6;

static constexpr uint8_t BASIC_TYPE_MASK = 0x3;
static constexpr uint8_t VALUE_HEADER_SHIFT = 2;

//! Object and Array header
static constexpr uint8_t FIELD_OFFSET_SIZE_MINUS_ONE_MASK = 0x3;

//! Object header
static constexpr uint8_t FIELD_ID_SIZE_MINUS_ONE_MASK = 0x3;
static constexpr uint8_t FIELD_ID_SIZE_MINUS_ONE_SHIFT = 2;

static constexpr uint8_t OBJECT_IS_LARGE_MASK = 0x1;
static constexpr uint8_t OBJECT_IS_LARGE_SHIFT = 4;

//! Array header
static constexpr uint8_t ARRAY_IS_LARGE_MASK = 0x1;
static constexpr uint8_t ARRAY_IS_LARGE_SHIFT = 2;

using namespace duckdb_yyjson;

namespace duckdb {

namespace {

static idx_t ReadVariableLengthLittleEndian(idx_t length_in_bytes, const_data_ptr_t &ptr) {
	if (length_in_bytes > sizeof(idx_t)) {
		throw NotImplementedException("Can't read little-endian value of %d bytes", length_in_bytes);
	}
	idx_t result = 0;
	memcpy(reinterpret_cast<uint8_t *>(&result), ptr, length_in_bytes);
	ptr += length_in_bytes;
	return result;
}

} // namespace

VariantMetadataHeader VariantMetadataHeader::FromHeaderByte(uint8_t byte) {
	VariantMetadataHeader header;
	header.version = byte & VERSION_MASK;
	header.sorted_strings = (byte >> SORTED_STRINGS_SHIFT) & SORTED_STRINGS_MASK;
	header.offset_size = ((byte >> OFFSET_SIZE_MINUS_ONE_SHIFT) & OFFSET_SIZE_MINUS_ONE_MASK) + 1;

	if (header.version != 1) {
		throw NotImplementedException("Only version 1 of the Variant encoding scheme is supported, found version: %d",
		                              header.version);
	}

	return header;
}

VariantMetadata::VariantMetadata(const string_t &metadata) : metadata(metadata) {
	auto metadata_data = metadata.GetData();

	header = VariantMetadataHeader::FromHeaderByte(metadata_data[0]);

	const_data_ptr_t ptr = reinterpret_cast<const_data_ptr_t>(metadata_data + sizeof(uint8_t));
	idx_t dictionary_size = ReadVariableLengthLittleEndian(header.offset_size, ptr);

	auto offsets = ptr;
	auto bytes = offsets + ((dictionary_size + 1) * header.offset_size);
	idx_t last_offset = ReadVariableLengthLittleEndian(header.offset_size, ptr);
	for (idx_t i = 0; i < dictionary_size; i++) {
		auto next_offset = ReadVariableLengthLittleEndian(header.offset_size, ptr);
		strings.emplace_back(reinterpret_cast<const char *>(bytes + last_offset), next_offset - last_offset);
		last_offset = next_offset;
	}
}

VariantValueMetadata VariantValueMetadata::FromHeaderByte(uint8_t byte) {
	VariantValueMetadata result;
	result.basic_type = VariantBasicTypeFromByte(byte & BASIC_TYPE_MASK);
	uint8_t value_header = byte >> VALUE_HEADER_SHIFT;
	switch (result.basic_type) {
	case VariantBasicType::PRIMITIVE: {
		result.primitive_type = VariantPrimitiveTypeFromByte(value_header);
		break;
	}
	case VariantBasicType::SHORT_STRING: {
		result.string_size = value_header;
		break;
	}
	case VariantBasicType::OBJECT: {
		result.field_offset_size = (value_header & FIELD_OFFSET_SIZE_MINUS_ONE_MASK) + 1;
		result.field_id_size = ((value_header >> FIELD_ID_SIZE_MINUS_ONE_SHIFT) & FIELD_ID_SIZE_MINUS_ONE_MASK) + 1;
		result.is_large = (value_header >> OBJECT_IS_LARGE_SHIFT) & OBJECT_IS_LARGE_MASK;
		break;
	}
	case VariantBasicType::ARRAY: {
		result.field_offset_size = (value_header & FIELD_OFFSET_SIZE_MINUS_ONE_MASK) + 1;
		result.is_large = (value_header >> ARRAY_IS_LARGE_SHIFT) & ARRAY_IS_LARGE_MASK;
		break;
	}
	default:
		throw InternalException("VariantBasicType (%d) not handled", static_cast<uint8_t>(result.basic_type));
	}
	return result;
}

template <class T>
static T DecodeDecimal(const_data_ptr_t data, uint8_t &scale, uint8_t &width) {
	scale = Load<uint8_t>(data);
	data++;

	auto result = Load<T>(data);
	//! FIXME: The spec says:
	//! The implied precision of a decimal value is `floor(log_10(val)) + 1`
	width = DecimalWidth<T>::max;
	return result;
}

template <>
hugeint_t DecodeDecimal(const_data_ptr_t data, uint8_t &scale, uint8_t &width) {
	scale = Load<uint8_t>(data);
	data++;

	hugeint_t result;
	result.lower = Load<uint64_t>(data);
	result.upper = Load<int64_t>(data + sizeof(uint64_t));
	//! FIXME: The spec says:
	//! The implied precision of a decimal value is `floor(log_10(val)) + 1`
	width = DecimalWidth<hugeint_t>::max;
	return result;
}

VariantValue VariantBinaryDecoder::PrimitiveTypeDecode(const VariantValueMetadata &value_metadata,
                                                       const_data_ptr_t data) {
	switch (value_metadata.primitive_type) {
	case VariantPrimitiveType::NULL_TYPE: {
		return VariantValue(Value());
	}
	case VariantPrimitiveType::BOOLEAN_TRUE: {
		return VariantValue(Value::BOOLEAN(true));
	}
	case VariantPrimitiveType::BOOLEAN_FALSE: {
		return VariantValue(Value::BOOLEAN(false));
	}
	case VariantPrimitiveType::INT8: {
		auto value = Load<int8_t>(data);
		return VariantValue(Value::TINYINT(value));
	}
	case VariantPrimitiveType::INT16: {
		auto value = Load<int16_t>(data);
		return VariantValue(Value::SMALLINT(value));
	}
	case VariantPrimitiveType::INT32: {
		auto value = Load<int32_t>(data);
		return VariantValue(Value::INTEGER(value));
	}
	case VariantPrimitiveType::INT64: {
		auto value = Load<int64_t>(data);
		return VariantValue(Value::BIGINT(value));
	}
	case VariantPrimitiveType::DOUBLE: {
		double value = Load<double>(data);
		return VariantValue(Value::DOUBLE(value));
	}
	case VariantPrimitiveType::FLOAT: {
		float value = Load<float>(data);
		return VariantValue(Value::FLOAT(value));
	}
	case VariantPrimitiveType::DECIMAL4: {
		uint8_t scale;
		uint8_t width;

		auto value = DecodeDecimal<int32_t>(data, scale, width);
		auto value_str = Decimal::ToString(value, width, scale);
		return VariantValue(Value(value_str));
	}
	case VariantPrimitiveType::DECIMAL8: {
		uint8_t scale;
		uint8_t width;

		auto value = DecodeDecimal<int64_t>(data, scale, width);
		auto value_str = Decimal::ToString(value, width, scale);
		return VariantValue(Value(value_str));
	}
	case VariantPrimitiveType::DECIMAL16: {
		uint8_t scale;
		uint8_t width;

		auto value = DecodeDecimal<hugeint_t>(data, scale, width);
		auto value_str = Decimal::ToString(value, width, scale);
		return VariantValue(Value(value_str));
	}
	case VariantPrimitiveType::DATE: {
		date_t value;
		value.days = Load<int32_t>(data);
		return VariantValue(Value::DATE(value));
	}
	case VariantPrimitiveType::TIMESTAMP_MICROS: {
		timestamp_tz_t micros_ts_tz;
		micros_ts_tz.value = Load<int64_t>(data);
		return VariantValue(Value::TIMESTAMPTZ(micros_ts_tz));
	}
	case VariantPrimitiveType::TIMESTAMP_NTZ_MICROS: {
		timestamp_t micros_ts;
		micros_ts.value = Load<int64_t>(data);

		auto value = Value::TIMESTAMP(micros_ts);
		auto value_str = value.ToString();
		return VariantValue(Value(value_str));
	}
	case VariantPrimitiveType::BINARY: {
		//! Follow the JSON serialization guide by converting BINARY to Base64:
		//! For example: `"dmFyaWFudAo="`
		auto size = Load<uint32_t>(data);
		auto string_data = reinterpret_cast<const char *>(data + sizeof(uint32_t));
		auto base64_string = Blob::ToBase64(string_t(string_data, size));
		return VariantValue(Value(base64_string));
	}
	case VariantPrimitiveType::STRING: {
		auto size = Load<uint32_t>(data);
		auto string_data = reinterpret_cast<const char *>(data + sizeof(uint32_t));
		if (!Utf8Proc::IsValid(string_data, size)) {
			throw InternalException("Can't decode Variant short-string, string isn't valid UTF8");
		}
		return VariantValue(Value(string(string_data, size)));
	}
	case VariantPrimitiveType::TIME_NTZ_MICROS: {
		dtime_t micros_time;
		micros_time.micros = Load<int64_t>(data);
		return VariantValue(Value::TIME(micros_time));
	}
	case VariantPrimitiveType::TIMESTAMP_NANOS: {
		timestamp_ns_t nanos_ts;
		nanos_ts.value = Load<int64_t>(data);

		//! Convert the nanos timestamp to a micros timestamp (not lossless)
		auto micros_ts = Timestamp::FromEpochNanoSeconds(nanos_ts.value);
		return VariantValue(Value::TIMESTAMPTZ(timestamp_tz_t(micros_ts)));
	}
	case VariantPrimitiveType::TIMESTAMP_NTZ_NANOS: {
		timestamp_ns_t nanos_ts;
		nanos_ts.value = Load<int64_t>(data);

		auto value = Value::TIMESTAMPNS(nanos_ts);
		auto value_str = value.ToString();
		return VariantValue(Value(value_str));
	}
	case VariantPrimitiveType::UUID: {
		auto uuid_value = UUIDValueConversion::ReadParquetUUID(data);
		auto value_str = UUID::ToString(uuid_value);
		return VariantValue(Value(value_str));
	}
	default:
		throw NotImplementedException("Variant PrimitiveTypeDecode not implemented for type (%d)",
		                              static_cast<uint8_t>(value_metadata.primitive_type));
	}
}

VariantValue VariantBinaryDecoder::ShortStringDecode(const VariantValueMetadata &value_metadata,
                                                     const_data_ptr_t data) {
	D_ASSERT(value_metadata.string_size < 64);
	auto string_data = reinterpret_cast<const char *>(data);
	if (!Utf8Proc::IsValid(string_data, value_metadata.string_size)) {
		throw InternalException("Can't decode Variant short-string, string isn't valid UTF8");
	}
	return VariantValue(Value(string(string_data, value_metadata.string_size)));
}

VariantValue VariantBinaryDecoder::ObjectDecode(const VariantMetadata &metadata,
                                                const VariantValueMetadata &value_metadata, const_data_ptr_t data) {
	VariantValue ret(VariantValueType::OBJECT);

	auto field_offset_size = value_metadata.field_offset_size;
	auto field_id_size = value_metadata.field_id_size;
	auto is_large = value_metadata.is_large;

	idx_t num_elements;
	if (is_large) {
		num_elements = Load<uint32_t>(data);
		data += sizeof(uint32_t);
	} else {
		num_elements = Load<uint8_t>(data);
		data += sizeof(uint8_t);
	}

	auto field_ids = data;
	auto field_offsets = data + (num_elements * field_id_size);
	auto values = field_offsets + ((num_elements + 1) * field_offset_size);

	idx_t last_offset = ReadVariableLengthLittleEndian(field_offset_size, field_offsets);
	for (idx_t i = 0; i < num_elements; i++) {
		auto field_id = ReadVariableLengthLittleEndian(field_id_size, field_ids);
		auto next_offset = ReadVariableLengthLittleEndian(field_offset_size, field_offsets);

		auto value = Decode(metadata, values + last_offset);
		auto &key = metadata.strings[field_id];

		ret.AddChild(key, std::move(value));
		last_offset = next_offset;
	}
	return ret;
}

VariantValue VariantBinaryDecoder::ArrayDecode(const VariantMetadata &metadata,
                                               const VariantValueMetadata &value_metadata, const_data_ptr_t data) {
	VariantValue ret(VariantValueType::ARRAY);

	auto field_offset_size = value_metadata.field_offset_size;
	auto is_large = value_metadata.is_large;

	uint32_t num_elements;
	if (is_large) {
		num_elements = Load<uint32_t>(data);
		data += sizeof(uint32_t);
	} else {
		num_elements = Load<uint8_t>(data);
		data += sizeof(uint8_t);
	}

	auto field_offsets = data;
	auto values = field_offsets + ((num_elements + 1) * field_offset_size);

	idx_t last_offset = ReadVariableLengthLittleEndian(field_offset_size, field_offsets);
	for (idx_t i = 0; i < num_elements; i++) {
		auto next_offset = ReadVariableLengthLittleEndian(field_offset_size, field_offsets);

		ret.AddItem(Decode(metadata, values + last_offset));
		last_offset = next_offset;
	}
	return ret;
}

VariantValue VariantBinaryDecoder::Decode(const VariantMetadata &variant_metadata, const_data_ptr_t data) {
	auto value_metadata = VariantValueMetadata::FromHeaderByte(data[0]);

	data++;
	switch (value_metadata.basic_type) {
	case VariantBasicType::PRIMITIVE: {
		return PrimitiveTypeDecode(value_metadata, data);
	}
	case VariantBasicType::SHORT_STRING: {
		return ShortStringDecode(value_metadata, data);
	}
	case VariantBasicType::OBJECT: {
		return ObjectDecode(variant_metadata, value_metadata, data);
	}
	case VariantBasicType::ARRAY: {
		return ArrayDecode(variant_metadata, value_metadata, data);
	}
	default:
		throw InternalException("Unexpected value for VariantBasicType");
	}
}

} // namespace duckdb
