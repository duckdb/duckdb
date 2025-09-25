#include "writer/variant_column_writer.hpp"
#include "duckdb/common/types/variant.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/function/scalar/variant_utils.hpp"
#include "reader/variant/variant_binary_decoder.hpp"

namespace duckdb {

static idx_t CalculateByteLength(idx_t value) {
	if (value == 0) {
		return 1;
	}
	auto value_data = reinterpret_cast<data_ptr_t>(&value);
	idx_t irrelevant_bytes = 0;
	//! Check how many of the most significant bytes are 0
	for (idx_t i = sizeof(idx_t); i > 0 && value_data[i - 1] == 0; i--) {
		irrelevant_bytes++;
	}
	return sizeof(idx_t) - irrelevant_bytes;
}

static uint8_t EncodeMetadataHeader(idx_t byte_length) {
	D_ASSERT(byte_length <= 4);

	uint8_t header_byte = 0;
	//! Set 'version' to 1
	header_byte |= static_cast<uint8_t>(1);
	//! Set 'sorted_strings' to 1
	header_byte |= static_cast<uint8_t>(1) << 4;
	//! Set 'offset_size_minus_one' to byte_length-1
	header_byte |= (static_cast<uint8_t>(byte_length) - 1) << 6;

	return header_byte;
}

static void CreateMetadata(UnifiedVariantVectorData &variant, Vector &metadata, idx_t count) {
	auto &keys = variant.keys;
	auto keys_data = variant.keys_data;

	//! NOTE: the parquet variant is limited to a max dictionary size of NumericLimits<uint32_t>::Maximum()
	//! Whereas we can have NumericLimits<uint32_t>::Maximum() *per* string in DuckDB
	auto metadata_data = FlatVector::GetData<string_t>(metadata);
	auto &validity = FlatVector::Validity(metadata);
	for (idx_t row = 0; row < count; row++) {
		if (!variant.RowIsValid(row)) {
			validity.SetInvalid(row);
			continue;
		}
		auto list_entry = keys_data[keys.sel->get_index(row)];
		auto dictionary_count = list_entry.length;
		idx_t dictionary_size = 0;
		for (idx_t i = 0; i < dictionary_count; i++) {
			auto &key = variant.GetKey(row, i);
			dictionary_size += key.GetSize();
		}
		if (dictionary_size >= NumericLimits<uint32_t>::Maximum()) {
			throw InvalidInputException("The total length of the dictionary exceeds a 4 byte value (uint32_t), failed "
			                            "to export VARIANT to Parquet");
		}

		auto byte_length = CalculateByteLength(dictionary_size);
		auto total_length = 1 + (byte_length * (dictionary_count + 2)) + dictionary_size;

		metadata_data[row] = StringVector::EmptyString(metadata, total_length);
		auto &metadata_blob = metadata_data[row];
		auto metadata_blob_data = metadata_blob.GetDataWriteable();

		metadata_blob_data[0] = EncodeMetadataHeader(byte_length);
		memcpy(metadata_blob_data + 1, reinterpret_cast<data_ptr_t>(&dictionary_count), byte_length);

		auto offset_ptr = metadata_blob_data + 1 + byte_length;
		auto string_ptr = metadata_blob_data + 1 + byte_length + ((dictionary_count + 1) * byte_length);
		idx_t total_offset = 0;
		for (idx_t i = 0; i < dictionary_count; i++) {
			memcpy(offset_ptr + (i * byte_length), reinterpret_cast<data_ptr_t>(&total_offset), byte_length);
			auto &key = variant.GetKey(row, i);

			memcpy(string_ptr + total_offset, key.GetData(), key.GetSize());
			total_offset += key.GetSize();
		}
		memcpy(offset_ptr + (dictionary_count * byte_length), reinterpret_cast<data_ptr_t>(&total_offset), byte_length);
		D_ASSERT(offset_ptr + ((dictionary_count + 1) * byte_length) == string_ptr);
		D_ASSERT(string_ptr + total_offset == metadata_blob_data + total_length);
		metadata_blob.SetSizeAndFinalize(total_length, total_length);
	}
}

static idx_t AnalyzeValueData(const UnifiedVariantVectorData &variant, idx_t row, uint32_t values_index,
                              vector<uint32_t> &offsets) {
	D_ASSERT(variant.RowIsValid(row));

	idx_t total_size = 0;
	//! Every value has at least a value header
	total_size++;

	auto type_id = variant.GetTypeId(row, values_index);
	switch (type_id) {
	case VariantLogicalType::OBJECT: {
		auto nested_data = VariantUtils::DecodeNestedData(variant, row, values_index);

		//! Calculate value and key offsets for all children
		idx_t total_offset = 0;
		uint32_t highest_keys_index = 0;
		for (idx_t i = 0; i < nested_data.child_count; i++) {
			auto keys_index = variant.GetKeysIndex(row, i + nested_data.children_idx);
			auto values_index = variant.GetValuesIndex(row, i + nested_data.children_idx);
			offsets.push_back(total_offset);

			total_offset += AnalyzeValueData(variant, row, values_index, offsets);
			highest_keys_index = MaxValue(highest_keys_index, keys_index);
		}

		//! Calculate the sizes for the objects value data
		auto field_id_size = CalculateByteLength(highest_keys_index);
		auto field_offset_size = CalculateByteLength(total_offset);
		auto num_elements = nested_data.child_count;
		const bool is_large = num_elements > NumericLimits<uint8_t>::Maximum();

		//! Now add the sizes for the objects value data
		if (is_large) {
			total_size += sizeof(uint32_t);
		} else {
			total_size += sizeof(uint8_t);
		}
		total_size += num_elements * field_id_size;
		total_size += (num_elements + 1) * field_offset_size;
		total_size += total_offset;
		break;
	}
	case VariantLogicalType::ARRAY: {
		auto nested_data = VariantUtils::DecodeNestedData(variant, row, values_index);

		idx_t total_offset = 0;
		for (idx_t i = 0; i < nested_data.child_count; i++) {
			auto values_index = variant.GetValuesIndex(row, i + nested_data.children_idx);
			offsets.push_back(total_offset);

			total_offset += AnalyzeValueData(variant, row, values_index, offsets);
		}

		auto field_offset_size = CalculateByteLength(total_offset);
		auto num_elements = nested_data.child_count;
		const bool is_large = num_elements > NumericLimits<uint8_t>::Maximum();

		if (is_large) {
			total_size += sizeof(uint32_t);
		} else {
			total_size += sizeof(uint8_t);
		}
		total_size += (num_elements + 1) * field_offset_size;
		total_size += total_offset;
		break;
	}
	case VariantLogicalType::BLOB:
	case VariantLogicalType::VARCHAR: {
		auto string_value = VariantUtils::DecodeStringData(variant, row, values_index);
		if (type_id == VariantLogicalType::VARCHAR && string_value.GetSize() > 64) {
			//! Save as regular string value
			total_size += sizeof(uint32_t) + string_value.GetSize();
		} else {
			//! Save as "short string" type
			total_size += string_value.GetSize();
		}
		break;
	}
	case VariantLogicalType::VARIANT_NULL:
	case VariantLogicalType::BOOL_TRUE:
	case VariantLogicalType::BOOL_FALSE:
		break;
	case VariantLogicalType::INT8:
		total_size += sizeof(uint8_t);
		break;
	case VariantLogicalType::INT16:
		total_size += sizeof(uint16_t);
		break;
	case VariantLogicalType::INT32:
		total_size += sizeof(uint32_t);
		break;
	case VariantLogicalType::INT64:
		total_size += sizeof(uint64_t);
		break;
	case VariantLogicalType::FLOAT:
		total_size += sizeof(float);
		break;
	case VariantLogicalType::DOUBLE:
		total_size += sizeof(double);
		break;
	case VariantLogicalType::DECIMAL: {
		auto decimal_data = VariantUtils::DecodeDecimalData(variant, row, values_index);
		total_size += 1;
		if (decimal_data.width <= 9) {
			total_size += sizeof(int32_t);
		} else if (decimal_data.width <= 18) {
			total_size += sizeof(int64_t);
		} else if (decimal_data.width <= 38) {
			total_size += sizeof(uhugeint_t);
		} else {
			throw InvalidInputException("Can't convert VARIANT DECIMAL(%d, %d) to Parquet VARIANT", decimal_data.width,
			                            decimal_data.scale);
		}
		break;
	}
	case VariantLogicalType::UUID:
		total_size += sizeof(uhugeint_t);
		break;
	case VariantLogicalType::DATE:
		total_size += sizeof(uint32_t);
		break;
	case VariantLogicalType::TIME_MICROS:
	case VariantLogicalType::TIMESTAMP_MICROS:
	case VariantLogicalType::TIMESTAMP_NANOS:
	case VariantLogicalType::TIMESTAMP_MICROS_TZ:
		total_size += sizeof(uint64_t);
		break;
	case VariantLogicalType::INTERVAL:
	case VariantLogicalType::BIGNUM:
	case VariantLogicalType::BITSTRING:
	case VariantLogicalType::TIMESTAMP_MILIS:
	case VariantLogicalType::TIMESTAMP_SEC:
	case VariantLogicalType::TIME_MICROS_TZ:
	case VariantLogicalType::TIME_NANOS:
	case VariantLogicalType::UINT8:
	case VariantLogicalType::UINT16:
	case VariantLogicalType::UINT32:
	case VariantLogicalType::UINT64:
	case VariantLogicalType::UINT128:
	case VariantLogicalType::INT128:
	default:
		throw InvalidInputException("Can't convert VARIANT of type '%s' to Parquet VARIANT",
		                            EnumUtil::ToString(type_id));
	}
	return total_size;
}

template <VariantPrimitiveType TYPE_ID>
void WritePrimitiveTypeHeader(data_ptr_t &value_data) {
	uint8_t value_header = 0;
	value_header |= static_cast<uint8_t>(VariantBasicType::PRIMITIVE);
	value_header |= static_cast<uint8_t>(TYPE_ID) << 2;

	*value_data = value_header;
	value_data++;
}

template <class T>
void CopySimplePrimitiveData(const UnifiedVariantVectorData &variant, data_ptr_t &value_data, idx_t row,
                             uint32_t values_index) {
	auto byte_offset = variant.GetByteOffset(row, values_index);
	auto data = const_data_ptr_cast(variant.GetData(row).GetData());
	auto ptr = data + byte_offset;
	memcpy(value_data, ptr, sizeof(T));
	value_data += sizeof(T);
}

static void WritePrimitiveValueData(const UnifiedVariantVectorData &variant, idx_t row, uint32_t values_index,
                                    data_ptr_t &value_data, const vector<uint32_t> &offsets, idx_t &offset_index) {
	auto type_id = variant.GetTypeId(row, values_index);

	D_ASSERT(type_id != VariantLogicalType::OBJECT && type_id != VariantLogicalType::ARRAY);
	switch (type_id) {
	case VariantLogicalType::BLOB:
	case VariantLogicalType::VARCHAR: {
		auto string_value = VariantUtils::DecodeStringData(variant, row, values_index);
		auto string_size = string_value.GetSize();
		if (type_id == VariantLogicalType::BLOB || string_size > 64) {
			if (type_id == VariantLogicalType::BLOB) {
				WritePrimitiveTypeHeader<VariantPrimitiveType::BINARY>(value_data);
			} else {
				WritePrimitiveTypeHeader<VariantPrimitiveType::STRING>(value_data);
			}
			Store<uint32_t>(string_size, value_data);
			value_data += sizeof(uint32_t);
		} else {
			uint8_t value_header = 0;
			value_header |= static_cast<uint8_t>(VariantBasicType::SHORT_STRING);
			value_header |= static_cast<uint8_t>(string_size) << 2;

			*value_data = value_header;
			value_data++;
		}
		memcpy(value_data, reinterpret_cast<const char *>(string_value.GetData()), string_size);
		value_data += string_size;
		break;
	}
	case VariantLogicalType::VARIANT_NULL:
		WritePrimitiveTypeHeader<VariantPrimitiveType::NULL_TYPE>(value_data);
		break;
	case VariantLogicalType::BOOL_TRUE:
		WritePrimitiveTypeHeader<VariantPrimitiveType::BOOLEAN_TRUE>(value_data);
		break;
	case VariantLogicalType::BOOL_FALSE:
		WritePrimitiveTypeHeader<VariantPrimitiveType::BOOLEAN_FALSE>(value_data);
		break;
	case VariantLogicalType::INT8:
		WritePrimitiveTypeHeader<VariantPrimitiveType::INT8>(value_data);
		CopySimplePrimitiveData<int8_t>(variant, value_data, row, values_index);
		break;
	case VariantLogicalType::INT16:
		WritePrimitiveTypeHeader<VariantPrimitiveType::INT16>(value_data);
		CopySimplePrimitiveData<int16_t>(variant, value_data, row, values_index);
		break;
	case VariantLogicalType::INT32:
		WritePrimitiveTypeHeader<VariantPrimitiveType::INT32>(value_data);
		CopySimplePrimitiveData<int32_t>(variant, value_data, row, values_index);
		break;
	case VariantLogicalType::INT64:
		WritePrimitiveTypeHeader<VariantPrimitiveType::INT64>(value_data);
		CopySimplePrimitiveData<int64_t>(variant, value_data, row, values_index);
		break;
	case VariantLogicalType::FLOAT:
		WritePrimitiveTypeHeader<VariantPrimitiveType::FLOAT>(value_data);
		CopySimplePrimitiveData<float>(variant, value_data, row, values_index);
		break;
	case VariantLogicalType::DOUBLE:
		WritePrimitiveTypeHeader<VariantPrimitiveType::DOUBLE>(value_data);
		CopySimplePrimitiveData<double>(variant, value_data, row, values_index);
		break;
	case VariantLogicalType::UUID:
		WritePrimitiveTypeHeader<VariantPrimitiveType::UUID>(value_data);
		CopySimplePrimitiveData<uhugeint_t>(variant, value_data, row, values_index);
		break;
	case VariantLogicalType::DATE:
		WritePrimitiveTypeHeader<VariantPrimitiveType::DATE>(value_data);
		CopySimplePrimitiveData<int32_t>(variant, value_data, row, values_index);
		break;
	case VariantLogicalType::TIME_MICROS:
		WritePrimitiveTypeHeader<VariantPrimitiveType::TIME_NTZ_MICROS>(value_data);
		CopySimplePrimitiveData<int64_t>(variant, value_data, row, values_index);
		break;
	case VariantLogicalType::TIMESTAMP_MICROS:
		WritePrimitiveTypeHeader<VariantPrimitiveType::TIMESTAMP_NTZ_MICROS>(value_data);
		CopySimplePrimitiveData<int64_t>(variant, value_data, row, values_index);
		break;
	case VariantLogicalType::TIMESTAMP_NANOS:
		WritePrimitiveTypeHeader<VariantPrimitiveType::TIMESTAMP_NTZ_NANOS>(value_data);
		CopySimplePrimitiveData<int64_t>(variant, value_data, row, values_index);
		break;
	case VariantLogicalType::TIMESTAMP_MICROS_TZ:
		WritePrimitiveTypeHeader<VariantPrimitiveType::TIMESTAMP_MICROS>(value_data);
		CopySimplePrimitiveData<int64_t>(variant, value_data, row, values_index);
		break;
	case VariantLogicalType::DECIMAL: {
		auto decimal_data = VariantUtils::DecodeDecimalData(variant, row, values_index);
		if (decimal_data.width <= 9) {
			WritePrimitiveTypeHeader<VariantPrimitiveType::DECIMAL4>(value_data);
			Store<int8_t>(decimal_data.scale, value_data);
			value_data++;
			memcpy(value_data, decimal_data.value_ptr, sizeof(int32_t));
			value_data += sizeof(int32_t);
		} else if (decimal_data.width <= 18) {
			WritePrimitiveTypeHeader<VariantPrimitiveType::DECIMAL8>(value_data);
			Store<int8_t>(decimal_data.scale, value_data);
			value_data++;
			memcpy(value_data, decimal_data.value_ptr, sizeof(int64_t));
			value_data += sizeof(int64_t);
		} else if (decimal_data.width <= 38) {
			WritePrimitiveTypeHeader<VariantPrimitiveType::DECIMAL16>(value_data);
			Store<int8_t>(decimal_data.scale, value_data);
			value_data++;
			memcpy(value_data, decimal_data.value_ptr, sizeof(hugeint_t));
			value_data += sizeof(hugeint_t);
		} else {
			throw InvalidInputException("Can't convert VARIANT DECIMAL(%d, %d) to Parquet VARIANT", decimal_data.width,
			                            decimal_data.scale);
		}
		break;
	}
	case VariantLogicalType::INTERVAL:
	case VariantLogicalType::BIGNUM:
	case VariantLogicalType::BITSTRING:
	case VariantLogicalType::TIMESTAMP_MILIS:
	case VariantLogicalType::TIMESTAMP_SEC:
	case VariantLogicalType::TIME_MICROS_TZ:
	case VariantLogicalType::TIME_NANOS:
	case VariantLogicalType::UINT8:
	case VariantLogicalType::UINT16:
	case VariantLogicalType::UINT32:
	case VariantLogicalType::UINT64:
	case VariantLogicalType::UINT128:
	case VariantLogicalType::INT128:
	default:
		throw InvalidInputException("Can't convert VARIANT of type '%s' to Parquet VARIANT",
		                            EnumUtil::ToString(type_id));
	}
}

static void WriteValueData(const UnifiedVariantVectorData &variant, idx_t row, uint32_t values_index,
                           data_ptr_t &value_data, const vector<uint32_t> &offsets, idx_t &offset_index) {
	D_ASSERT(variant.RowIsValid(row));

	auto type_id = variant.GetTypeId(row, values_index);
	if (type_id == VariantLogicalType::OBJECT) {
		auto nested_data = VariantUtils::DecodeNestedData(variant, row, values_index);

		//! -- Object value header --

		//! Determine the 'field_id_size'
		uint32_t highest_keys_index = 0;
		for (idx_t i = 0; i < nested_data.child_count; i++) {
			auto keys_index = variant.GetKeysIndex(row, i + nested_data.children_idx);
			highest_keys_index = MaxValue(highest_keys_index, keys_index);
		}
		auto field_id_size = CalculateByteLength(highest_keys_index);

		uint32_t last_offset = 0;
		if (nested_data.child_count) {
			last_offset = offsets[offset_index + nested_data.child_count - 1];
		}
		offset_index += nested_data.child_count;
		auto field_offset_size = CalculateByteLength(last_offset);

		auto num_elements = nested_data.child_count;
		const bool is_large = num_elements > NumericLimits<uint8_t>::Maximum();

		uint8_t value_header = 0;
		value_header |= static_cast<uint8_t>(VariantBasicType::OBJECT);
		value_header |= static_cast<uint8_t>(is_large) << 6;
		value_header |= (static_cast<uint8_t>(field_id_size) - 1) << 4;
		value_header |= (static_cast<uint8_t>(field_offset_size) - 1) << 2;

		*value_data = value_header;
		value_data++;

		//! Write the 'num_elements'
		if (is_large) {
			Store<uint32_t>(static_cast<uint32_t>(num_elements), value_data);
			value_data += sizeof(uint32_t);
		} else {
			Store<uint8_t>(static_cast<uint8_t>(num_elements), value_data);
			value_data += sizeof(uint8_t);
		}

		//! Write the 'field_id' entries
		for (idx_t i = 0; i < nested_data.child_count; i++) {
			auto keys_index = variant.GetKeysIndex(row, i + nested_data.children_idx);
			memcpy(value_data, reinterpret_cast<data_ptr_t>(&keys_index), field_id_size);
			value_data += field_id_size;
		}

		//! Write the 'field_offset' entries and the child 'value's
		auto children_ptr = value_data + ((num_elements + 1) * field_offset_size);
		idx_t total_offset = 0;
		for (idx_t i = 0; i < nested_data.child_count; i++) {
			auto values_index = variant.GetValuesIndex(row, i + nested_data.children_idx);

			memcpy(value_data, reinterpret_cast<data_ptr_t>(&total_offset), field_offset_size);
			value_data += field_offset_size;
			auto start_ptr = children_ptr;
			WriteValueData(variant, row, values_index, children_ptr, offsets, offset_index);
			total_offset += (children_ptr - start_ptr);
		}
		memcpy(value_data, reinterpret_cast<data_ptr_t>(&total_offset), field_offset_size);
		value_data = children_ptr;
	} else if (type_id == VariantLogicalType::ARRAY) {
		auto nested_data = VariantUtils::DecodeNestedData(variant, row, values_index);

		//! -- Array value header --

		uint32_t last_offset = 0;
		if (nested_data.child_count) {
			last_offset = offsets[offset_index + nested_data.child_count - 1];
		}
		offset_index += nested_data.child_count;
		auto field_offset_size = CalculateByteLength(last_offset);

		auto num_elements = nested_data.child_count;
		const bool is_large = num_elements > NumericLimits<uint8_t>::Maximum();

		uint8_t value_header = 0;
		value_header |= static_cast<uint8_t>(VariantBasicType::ARRAY);
		value_header |= static_cast<uint8_t>(is_large) << 4;
		value_header |= (static_cast<uint8_t>(field_offset_size) - 1) << 2;

		*value_data = value_header;
		value_data++;

		//! Write the 'num_elements'
		if (is_large) {
			Store<uint32_t>(static_cast<uint32_t>(num_elements), value_data);
			value_data += sizeof(uint32_t);
		} else {
			Store<uint8_t>(static_cast<uint8_t>(num_elements), value_data);
			value_data += sizeof(uint8_t);
		}

		//! Write the 'field_offset' entries and the child 'value's
		auto children_ptr = value_data + ((num_elements + 1) * field_offset_size);
		idx_t total_offset = 0;
		for (idx_t i = 0; i < nested_data.child_count; i++) {
			auto values_index = variant.GetValuesIndex(row, i + nested_data.children_idx);

			memcpy(value_data, reinterpret_cast<data_ptr_t>(&total_offset), field_offset_size);
			value_data += field_offset_size;
			auto start_ptr = children_ptr;
			WriteValueData(variant, row, values_index, children_ptr, offsets, offset_index);
			total_offset += (children_ptr - start_ptr);
		}
		memcpy(value_data, reinterpret_cast<data_ptr_t>(&total_offset), field_offset_size);
		value_data = children_ptr;
	} else {
		WritePrimitiveValueData(variant, row, values_index, value_data, offsets, offset_index);
	}
}

static void CreateValues(UnifiedVariantVectorData &variant, Vector &value, idx_t count) {
	auto &validity = FlatVector::Validity(value);
	auto value_data = FlatVector::GetData<string_t>(value);

	for (idx_t row = 0; row < count; row++) {
		if (!variant.RowIsValid(row)) {
			validity.SetInvalid(row);
			continue;
		}

		//! The (relative) offsets for each value, in the case of nesting
		vector<uint32_t> offsets;
		//! Determine the size of this 'value' blob
		idx_t blob_length = AnalyzeValueData(variant, row, 0, offsets);
		if (!blob_length) {
			continue;
		}
		value_data[row] = StringVector::EmptyString(value, blob_length);
		auto &value_blob = value_data[row];
		auto value_blob_data = reinterpret_cast<data_ptr_t>(value_blob.GetDataWriteable());

		idx_t offset_index = 0;
		WriteValueData(variant, row, 0, value_blob_data, offsets, offset_index);
		value_blob.SetSizeAndFinalize(blob_length, blob_length);
	}
}

namespace {

struct ShreddingState {
public:
	explicit ShreddingState(idx_t total_count) : shredded_sel(total_count), values_index_sel(total_count) {
	}

public:
	//! row that is shredded
	SelectionVector shredded_sel;
	//! 'values_index' of the shredded value
	SelectionVector values_index_sel;
	//! The amount of rows that are shredded on
	idx_t count = 0;
};

} // namespace

static void WriteVariantValues(UnifiedVariantVectorData &variant, Vector &result, idx_t count) {
	optional_ptr<Vector> value;
	optional_ptr<Vector> typed_value;

	auto &result_type = result.GetType();
	D_ASSERT(result_type.id() == LogicalTypeId::STRUCT);
	auto &child_types = StructType::GetChildTypes(result_type);
	auto &child_vectors = StructVector::GetEntries(result);
	D_ASSERT(child_types.size() == child_vectors.size());
	for (idx_t i = 0; i < child_types.size(); i++) {
		auto &name = child_types[i].first;
		if (name == "value") {
			value = child_vectors[i].get();
		} else if (name == "typed_value") {
			typed_value = child_vectors[i].get();
		}
	}

	//! TODO: this should be able to be used recursively
	//! The method is not vectorized

	CreateValues(variant, *value, count);
}

static void ToParquetVariant(DataChunk &input, ExpressionState &state, Vector &result) {
	// DuckDB Variant:
	// - keys = VARCHAR[]
	// - children = STRUCT(keys_index UINTEGER, values_index UINTEGER)[]
	// - values = STRUCT(type_id UTINYINT, byte_offset UINTEGER)[]
	// - data = BLOB

	// Parquet VARIANT:
	// - metadata = BLOB
	// - value = BLOB

	auto &variant_vec = input.data[0];
	auto count = input.size();

	RecursiveUnifiedVectorFormat recursive_format;
	Vector::RecursiveToUnifiedFormat(variant_vec, count, recursive_format);
	UnifiedVariantVectorData variant(recursive_format);

	auto &result_vectors = StructVector::GetEntries(result);
	bool all_null = true;
	for (idx_t i = 0; i < count; i++) {
		if (!variant.RowIsValid(i)) {
			FlatVector::SetNull(result, i, true);
		} else {
			all_null = false;
		}
	}
	if (all_null) {
		return;
	}
	auto &metadata = *result_vectors[0];
	CreateMetadata(variant, metadata, count);
	WriteVariantValues(variant, result, count);
}

ScalarFunction VariantColumnWriter::GetTransformFunction() {
	ScalarFunction transform("variant_to_parquet_variant", {LogicalType::VARIANT()}, TransformedType(),
	                         ToParquetVariant);
	transform.null_handling = FunctionNullHandling::SPECIAL_HANDLING;
	return transform;
}

} // namespace duckdb
