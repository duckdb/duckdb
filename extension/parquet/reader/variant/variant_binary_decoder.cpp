#include "reader/variant/variant_binary_decoder.hpp"
#include "duckdb/common/printer.hpp"
#include "utf8proc_wrapper.hpp"

#include "reader/uuid_column_reader.hpp"

#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/common/types/decimal.hpp"
#include "duckdb/common/types/uuid.hpp"
#include "duckdb/common/types/time.hpp"
#include "duckdb/common/types/date.hpp"

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

static idx_t ReadVariableLengthLittleEndian(idx_t length_in_bytes, const_data_ptr_t ptr, idx_t &offset,
                                            const idx_t capacity) {
	if (length_in_bytes > sizeof(idx_t)) {
		throw NotImplementedException("Can't read little-endian value of %d bytes", length_in_bytes);
	}
	if (offset + length_in_bytes > capacity) {
		throw IOException("Data corruption detected, read of length_in_bytes (%d) would exceed buffer capacity",
		                  length_in_bytes);
	}
	idx_t result = 0;
	memcpy(reinterpret_cast<uint8_t *>(&result), ptr + offset, length_in_bytes);
	offset += length_in_bytes;
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
	auto metadata_data = reinterpret_cast<const_data_ptr_t>(metadata.GetData());
	const auto metadata_buffer_capacity = metadata.GetSize();
	if (!metadata_data || metadata.GetSize() < 1) {
		throw IOException("Corrupted VARIANT 'metadata' buffer, empty or nullptr");
	}

	idx_t metadata_offset = 0;
	header = VariantMetadataHeader::FromHeaderByte(metadata_data[metadata_offset]);
	metadata_offset += sizeof(uint8_t);

	idx_t dictionary_size =
	    ReadVariableLengthLittleEndian(header.offset_size, metadata_data, metadata_offset, metadata_buffer_capacity);

	auto data_start = metadata_offset + ((dictionary_size + 1) * header.offset_size);
	idx_t last_offset =
	    ReadVariableLengthLittleEndian(header.offset_size, metadata_data, metadata_offset, metadata_buffer_capacity);
	for (idx_t i = 0; i < dictionary_size; i++) {
		auto next_offset = ReadVariableLengthLittleEndian(header.offset_size, metadata_data, metadata_offset,
		                                                  metadata_buffer_capacity);
		const idx_t string_size = next_offset - last_offset;
		if (data_start + last_offset + string_size > metadata_buffer_capacity) {
			throw IOException("Corrupted VARIANT 'metadata' buffer");
		}
		strings.emplace_back(reinterpret_cast<const char *>(metadata_data + data_start + last_offset), string_size);
		last_offset = next_offset;
	}
	//! header byte + offsets region + string bytes
	total_size = metadata_offset + last_offset;
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
static T DecodeDecimal(const_data_ptr_t data, idx_t data_offset, idx_t data_size, uint8_t &scale, uint8_t &width) {
	if (data_offset + sizeof(uint8_t) + sizeof(T) > data_size) {
		throw IOException("Corrupted VARIANT 'value' buffer");
	}
	scale = Load<uint8_t>(data + data_offset);
	data_offset += sizeof(uint8_t);

	auto result = Load<T>(data + data_offset);
	auto abs_val = result;
	if (abs_val < 0) {
		abs_val = -abs_val;
	}
	uint8_t digits = floor(log10(abs_val)) + 1;
	width = digits;
	return result;
}

template <>
hugeint_t DecodeDecimal(const_data_ptr_t data, idx_t data_offset, idx_t data_size, uint8_t &scale, uint8_t &width) {
	if (data_offset + sizeof(uint8_t) + sizeof(uint64_t) + sizeof(int64_t) > data_size) {
		throw IOException("Corrupted VARIANT 'value' buffer");
	}
	scale = Load<uint8_t>(data + data_offset);
	data_offset += sizeof(uint8_t);

	hugeint_t result;
	result.lower = Load<uint64_t>(data + data_offset);
	data_offset += sizeof(uint64_t);
	result.upper = Load<int64_t>(data + data_offset);
	//! FIXME: The spec says:
	//! The implied precision of a decimal value is `floor(log_10(val)) + 1`
	width = DecimalWidth<hugeint_t>::max;
	return result;
}

VariantValue VariantBinaryDecoder::PrimitiveTypeDecode(const VariantValueMetadata &value_metadata,
                                                       const_data_ptr_t data, idx_t data_offset, idx_t data_size) {
	switch (value_metadata.primitive_type) {
	case VariantPrimitiveType::NULL_TYPE: {
		return VariantValue::NullValue();
	}
	case VariantPrimitiveType::BOOLEAN_TRUE: {
		return VariantValue(Value::BOOLEAN(true));
	}
	case VariantPrimitiveType::BOOLEAN_FALSE: {
		return VariantValue(Value::BOOLEAN(false));
	}
	case VariantPrimitiveType::INT8: {
		if (data_offset + sizeof(int8_t) > data_size) {
			throw IOException("Corrupted VARIANT 'value' buffer");
		}
		auto value = Load<int8_t>(data + data_offset);
		return VariantValue(Value::TINYINT(value));
	}
	case VariantPrimitiveType::INT16: {
		if (data_offset + sizeof(int16_t) > data_size) {
			throw IOException("Corrupted VARIANT 'value' buffer");
		}
		auto value = Load<int16_t>(data + data_offset);
		return VariantValue(Value::SMALLINT(value));
	}
	case VariantPrimitiveType::INT32: {
		if (data_offset + sizeof(int32_t) > data_size) {
			throw IOException("Corrupted VARIANT 'value' buffer");
		}
		auto value = Load<int32_t>(data + data_offset);
		return VariantValue(Value::INTEGER(value));
	}
	case VariantPrimitiveType::INT64: {
		if (data_offset + sizeof(int64_t) > data_size) {
			throw IOException("Corrupted VARIANT 'value' buffer");
		}
		auto value = Load<int64_t>(data + data_offset);
		return VariantValue(Value::BIGINT(value));
	}
	case VariantPrimitiveType::DOUBLE: {
		if (data_offset + sizeof(double) > data_size) {
			throw IOException("Corrupted VARIANT 'value' buffer");
		}
		double value = Load<double>(data + data_offset);
		return VariantValue(Value::DOUBLE(value));
	}
	case VariantPrimitiveType::FLOAT: {
		if (data_offset + sizeof(float) > data_size) {
			throw IOException("Corrupted VARIANT 'value' buffer");
		}
		float value = Load<float>(data + data_offset);
		return VariantValue(Value::FLOAT(value));
	}
	case VariantPrimitiveType::DECIMAL4: {
		uint8_t scale;
		uint8_t width;

		auto value = DecodeDecimal<int32_t>(data, data_offset, data_size, scale, width);
		return VariantValue(Value::DECIMAL(value, width, scale));
	}
	case VariantPrimitiveType::DECIMAL8: {
		uint8_t scale;
		uint8_t width;

		auto value = DecodeDecimal<int64_t>(data, data_offset, data_size, scale, width);
		return VariantValue(Value::DECIMAL(value, width, scale));
	}
	case VariantPrimitiveType::DECIMAL16: {
		uint8_t scale;
		uint8_t width;

		auto value = DecodeDecimal<hugeint_t>(data, data_offset, data_size, scale, width);
		return VariantValue(Value::DECIMAL(value, width, scale));
	}
	case VariantPrimitiveType::DATE: {
		date_t value;
		if (data_offset + sizeof(int32_t) > data_size) {
			throw IOException("Corrupted VARIANT 'value' buffer");
		}
		value.days = Load<int32_t>(data + data_offset);
		return VariantValue(Value::DATE(value));
	}
	case VariantPrimitiveType::TIMESTAMP_MICROS: {
		timestamp_tz_t micros_ts_tz;
		if (data_offset + sizeof(int64_t) > data_size) {
			throw IOException("Corrupted VARIANT 'value' buffer");
		}
		micros_ts_tz.value = Load<int64_t>(data + data_offset);
		return VariantValue(Value::TIMESTAMPTZ(micros_ts_tz));
	}
	case VariantPrimitiveType::TIMESTAMP_NTZ_MICROS: {
		timestamp_t micros_ts;
		if (data_offset + sizeof(int64_t) > data_size) {
			throw IOException("Corrupted VARIANT 'value' buffer");
		}
		micros_ts.value = Load<int64_t>(data + data_offset);

		auto value = Value::TIMESTAMP(micros_ts);
		return VariantValue(std::move(value));
	}
	case VariantPrimitiveType::BINARY: {
		//! Keep the raw bytes as a BLOB so the type is preserved when reconstructing a VARIANT. The conversion to
		//! Base64 happens now in VariantValue::ToJSON.
		if (data_offset + sizeof(uint32_t) > data_size) {
			throw IOException("Corrupted VARIANT 'value' buffer");
		}
		auto size = Load<uint32_t>(data + data_offset);
		data_offset += sizeof(uint32_t);

		if (data_offset + size > data_size) {
			throw IOException("Corrupted VARIANT 'value' buffer");
		}
		return VariantValue(Value::BLOB(data + data_offset, size));
	}
	case VariantPrimitiveType::STRING: {
		if (data_offset + sizeof(uint32_t) > data_size) {
			throw IOException("Corrupted VARIANT 'value' buffer");
		}
		auto size = Load<uint32_t>(data + data_offset);
		data_offset += sizeof(uint32_t);

		auto string_data = reinterpret_cast<const char *>(data + data_offset);
		if (data_offset + size > data_size) {
			throw IOException("Corrupted VARIANT 'value' buffer");
		}
		if (!Utf8Proc::IsValid(string_data, size)) {
			throw IOException("Can't decode Variant short-string, string isn't valid UTF8");
		}
		return VariantValue(Value(string(string_data, size)));
	}
	case VariantPrimitiveType::TIME_NTZ_MICROS: {
		dtime_t micros_time;
		if (data_offset + sizeof(int64_t) > data_size) {
			throw IOException("Corrupted VARIANT 'value' buffer");
		}
		micros_time.micros = Load<int64_t>(data + data_offset);
		return VariantValue(Value::TIME(micros_time));
	}
	case VariantPrimitiveType::TIMESTAMP_NANOS: {
		timestamp_ns_t nanos_ts;
		if (data_offset + sizeof(int64_t) > data_size) {
			throw IOException("Corrupted VARIANT 'value' buffer");
		}
		nanos_ts.value = Load<int64_t>(data + data_offset);

		//! Convert the nanos timestamp to a micros timestamp (not lossless)
		auto micros_ts = Timestamp::FromEpochNanoSeconds(nanos_ts.value);
		return VariantValue(Value::TIMESTAMPTZ(timestamp_tz_t(micros_ts)));
	}
	case VariantPrimitiveType::TIMESTAMP_NTZ_NANOS: {
		timestamp_ns_t nanos_ts;
		if (data_offset + sizeof(int64_t) > data_size) {
			throw IOException("Corrupted VARIANT 'value' buffer");
		}
		nanos_ts.value = Load<int64_t>(data + data_offset);

		auto value = Value::TIMESTAMPNS(nanos_ts);
		return VariantValue(std::move(value));
	}
	case VariantPrimitiveType::UUID: {
		if (data_offset + sizeof(hugeint_t) > data_size) {
			throw IOException("Corrupted VARIANT 'value' buffer");
		}
		auto uuid_value = UUIDValueConversion::ReadParquetUUID(data + data_offset);
		return VariantValue(Value::UUID(uuid_value));
	}
	default:
		throw NotImplementedException("Variant PrimitiveTypeDecode not implemented for type (%d)",
		                              static_cast<uint8_t>(value_metadata.primitive_type));
	}
}

VariantValue VariantBinaryDecoder::ShortStringDecode(const VariantValueMetadata &value_metadata, const_data_ptr_t data,
                                                     idx_t data_offset, idx_t data_size) {
	if (value_metadata.string_size >= 64) {
		throw IOException("Corrupted VARIANT 'metadata' buffer");
	}
	auto string_data = reinterpret_cast<const char *>(data + data_offset);
	if (data_offset + value_metadata.string_size > data_size) {
		throw IOException("Corrupted VARIANT 'value' buffer");
	}
	if (!Utf8Proc::IsValid(string_data, value_metadata.string_size)) {
		throw IOException("Can't decode Variant short-string, string isn't valid UTF8");
	}
	return VariantValue(Value(string(string_data, value_metadata.string_size)));
}

VariantValue VariantBinaryDecoder::ObjectDecode(const VariantMetadata &metadata,
                                                const VariantValueMetadata &value_metadata, const_data_ptr_t data,
                                                idx_t data_offset, idx_t data_size) {
	VariantValue ret(VariantValueType::OBJECT);

	auto field_offset_size = value_metadata.field_offset_size;
	auto field_id_size = value_metadata.field_id_size;
	auto is_large = value_metadata.is_large;

	idx_t num_elements;
	if (is_large) {
		if (data_offset + sizeof(uint32_t) > data_size) {
			throw IOException("Corrupted VARIANT 'value' buffer");
		}
		num_elements = Load<uint32_t>(data + data_offset);
		data_offset += sizeof(uint32_t);
	} else {
		if (data_offset + sizeof(uint8_t) > data_size) {
			throw IOException("Corrupted VARIANT 'value' buffer");
		}
		num_elements = Load<uint8_t>(data + data_offset);
		data_offset += sizeof(uint8_t);
	}

	auto field_ids_offset = data_offset;
	auto field_offsets_offset = data_offset + (num_elements * field_id_size);
	auto values_offset = field_offsets_offset + ((num_elements + 1) * field_offset_size);

	idx_t last_offset = ReadVariableLengthLittleEndian(field_offset_size, data, field_offsets_offset, data_size);
	for (idx_t i = 0; i < num_elements; i++) {
		auto field_id = ReadVariableLengthLittleEndian(field_id_size, data, field_ids_offset, data_size);
		auto next_offset = ReadVariableLengthLittleEndian(field_offset_size, data, field_offsets_offset, data_size);

		auto value = Decode(metadata, data, values_offset + last_offset, data_size);
		if (field_id >= metadata.strings.size()) {
			throw IOException("Corrupted VARIANT 'value' buffer");
		}
		auto &key = metadata.strings[field_id];

		ret.AddChild(key, std::move(value));
		last_offset = next_offset;
	}
	return ret;
}

VariantValue VariantBinaryDecoder::ArrayDecode(const VariantMetadata &metadata,
                                               const VariantValueMetadata &value_metadata, const_data_ptr_t data,
                                               idx_t data_offset, idx_t data_size) {
	VariantValue ret(VariantValueType::ARRAY);

	auto field_offset_size = value_metadata.field_offset_size;
	auto is_large = value_metadata.is_large;

	uint32_t num_elements;
	if (is_large) {
		if (data_offset + sizeof(uint32_t) > data_size) {
			throw IOException("Corrupted VARIANT 'value' buffer");
		}
		num_elements = Load<uint32_t>(data + data_offset);
		data_offset += sizeof(uint32_t);
	} else {
		if (data_offset + sizeof(uint8_t) > data_size) {
			throw IOException("Corrupted VARIANT 'value' buffer");
		}
		num_elements = Load<uint8_t>(data + data_offset);
		data_offset += sizeof(uint8_t);
	}

	auto field_offsets_offset = data_offset;
	auto values_offset = field_offsets_offset + ((num_elements + 1) * field_offset_size);

	idx_t last_offset = ReadVariableLengthLittleEndian(field_offset_size, data, field_offsets_offset, data_size);
	for (idx_t i = 0; i < num_elements; i++) {
		auto next_offset = ReadVariableLengthLittleEndian(field_offset_size, data, field_offsets_offset, data_size);

		ret.AddItem(Decode(metadata, data, values_offset + last_offset, data_size));
		last_offset = next_offset;
	}
	return ret;
}

VariantValue VariantBinaryDecoder::Decode(const VariantMetadata &variant_metadata, const_data_ptr_t data,
                                          idx_t data_offset, idx_t data_size) {
	if (data_offset + 1 > data_size) {
		throw IOException("Corrupted VARIANT 'value' buffer");
	}
	auto value_metadata = VariantValueMetadata::FromHeaderByte(data[data_offset]);
	data_offset += sizeof(uint8_t);

	switch (value_metadata.basic_type) {
	case VariantBasicType::PRIMITIVE: {
		return PrimitiveTypeDecode(value_metadata, data, data_offset, data_size);
	}
	case VariantBasicType::SHORT_STRING: {
		return ShortStringDecode(value_metadata, data, data_offset, data_size);
	}
	case VariantBasicType::OBJECT: {
		return ObjectDecode(variant_metadata, value_metadata, data, data_offset, data_size);
	}
	case VariantBasicType::ARRAY: {
		return ArrayDecode(variant_metadata, value_metadata, data, data_offset, data_size);
	}
	default:
		throw IOException("Unexpected value for VariantBasicType");
	}
}

} // namespace duckdb
