#include "writer/variant_column_writer.hpp"
#include "duckdb/common/types/variant.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/function/scalar/variant_utils.hpp"
#include "reader/variant/variant_binary_decoder.hpp"
#include "parquet_shredding.hpp"
#include "duckdb/common/types/decimal.hpp"
#include "duckdb/common/types/uuid.hpp"

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

#ifdef DEBUG
	auto decoded_header = VariantMetadataHeader::FromHeaderByte(header_byte);
	D_ASSERT(decoded_header.offset_size == byte_length);
#endif

	return header_byte;
}

static void CreateMetadata(UnifiedVariantVectorData &variant, Vector &metadata, idx_t count) {
	auto &keys = variant.keys;
	auto keys_data = variant.keys_data;

	//! NOTE: the parquet variant is limited to a max dictionary size of NumericLimits<uint32_t>::Maximum()
	//! Whereas we can have NumericLimits<uint32_t>::Maximum() *per* string in DuckDB
	auto metadata_data = FlatVector::GetData<string_t>(metadata);
	for (idx_t row = 0; row < count; row++) {
		uint64_t dictionary_count = 0;
		if (variant.RowIsValid(row)) {
			auto list_entry = keys_data[keys.sel->get_index(row)];
			dictionary_count = list_entry.length;
		}
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

#ifdef DEBUG
		auto decoded_metadata = VariantMetadata(metadata_blob);
		D_ASSERT(decoded_metadata.strings.size() == dictionary_count);
		for (idx_t i = 0; i < dictionary_count; i++) {
			D_ASSERT(decoded_metadata.strings[i] == variant.GetKey(row, i).GetString());
		}
#endif
	}
}

namespace {

static unordered_set<VariantLogicalType> GetVariantType(const LogicalType &type) {
	if (type.id() == LogicalTypeId::ANY) {
		return {};
	}
	switch (type.id()) {
	case LogicalTypeId::STRUCT:
		return {VariantLogicalType::OBJECT};
	case LogicalTypeId::LIST:
		return {VariantLogicalType::ARRAY};
	case LogicalTypeId::BOOLEAN:
		return {VariantLogicalType::BOOL_TRUE, VariantLogicalType::BOOL_FALSE};
	case LogicalTypeId::TINYINT:
		return {VariantLogicalType::INT8};
	case LogicalTypeId::SMALLINT:
		return {VariantLogicalType::INT16};
	case LogicalTypeId::INTEGER:
		return {VariantLogicalType::INT32};
	case LogicalTypeId::BIGINT:
		return {VariantLogicalType::INT64};
	case LogicalTypeId::FLOAT:
		return {VariantLogicalType::FLOAT};
	case LogicalTypeId::DOUBLE:
		return {VariantLogicalType::DOUBLE};
	case LogicalTypeId::DECIMAL:
		return {VariantLogicalType::DECIMAL};
	case LogicalTypeId::DATE:
		return {VariantLogicalType::DATE};
	case LogicalTypeId::TIME:
		return {VariantLogicalType::TIME_MICROS};
	case LogicalTypeId::TIMESTAMP_TZ:
		return {VariantLogicalType::TIMESTAMP_MICROS_TZ};
	case LogicalTypeId::TIMESTAMP:
		return {VariantLogicalType::TIMESTAMP_MICROS};
	case LogicalTypeId::TIMESTAMP_NS:
		return {VariantLogicalType::TIMESTAMP_NANOS};
	case LogicalTypeId::BLOB:
		return {VariantLogicalType::BLOB};
	case LogicalTypeId::VARCHAR:
		return {VariantLogicalType::VARCHAR};
	case LogicalTypeId::UUID:
		return {VariantLogicalType::UUID};
	default:
		throw BinderException("Type '%s' can't be translated to a VARIANT type", type.ToString());
	}
}

struct ShreddingState {
public:
	explicit ShreddingState(const LogicalType &type, idx_t total_count)
	    : type(type), shredded_sel(total_count), values_index_sel(total_count), result_sel(total_count) {
		variant_types = GetVariantType(type);
	}

public:
	bool ValueIsShredded(UnifiedVariantVectorData &variant, idx_t row, idx_t values_index) {
		auto type_id = variant.GetTypeId(row, values_index);
		if (!variant_types.count(type_id)) {
			return false;
		}
		if (type_id == VariantLogicalType::DECIMAL) {
			auto physical_type = type.InternalType();
			auto decimal_data = VariantUtils::DecodeDecimalData(variant, row, values_index);
			auto decimal_physical_type = decimal_data.GetPhysicalType();
			return physical_type == decimal_physical_type;
		}
		return true;
	}
	void SetShredded(idx_t row, idx_t values_index, idx_t result_idx) {
		shredded_sel[count] = row;
		values_index_sel[count] = values_index;
		result_sel[count] = result_idx;
		count++;
	}
	case_insensitive_string_set_t ObjectFields() {
		D_ASSERT(type.id() == LogicalTypeId::STRUCT);
		case_insensitive_string_set_t res;
		auto &child_types = StructType::GetChildTypes(type);
		for (auto &entry : child_types) {
			auto &type = entry.first;
			res.emplace(string_t(type.c_str(), type.size()));
		}
		return res;
	}

public:
	//! The type the field is shredded on
	const LogicalType &type;
	unordered_set<VariantLogicalType> variant_types;
	//! row that is shredded
	SelectionVector shredded_sel;
	//! 'values_index' of the shredded value
	SelectionVector values_index_sel;
	//! result row of the shredded value
	SelectionVector result_sel;
	//! The amount of rows that are shredded on
	idx_t count = 0;
};

} // namespace

vector<idx_t> GetChildIndices(const UnifiedVariantVectorData &variant, idx_t row, const VariantNestedData &nested_data,
                              optional_ptr<ShreddingState> shredding_state) {
	vector<idx_t> child_indices;
	if (!shredding_state || shredding_state->type.id() != LogicalTypeId::STRUCT) {
		for (idx_t i = 0; i < nested_data.child_count; i++) {
			child_indices.push_back(i);
		}
		return child_indices;
	}
	//! FIXME: The variant spec says that field names should be case-sensitive, not insensitive
	case_insensitive_string_set_t shredded_fields = shredding_state->ObjectFields();

	for (idx_t i = 0; i < nested_data.child_count; i++) {
		auto keys_index = variant.GetKeysIndex(row, i + nested_data.children_idx);
		auto &key = variant.GetKey(row, keys_index);

		if (shredded_fields.count(key)) {
			//! This field is shredded on, omit it from the value
			continue;
		}
		child_indices.push_back(i);
	}
	return child_indices;
}

static idx_t AnalyzeValueData(const UnifiedVariantVectorData &variant, idx_t row, uint32_t values_index,
                              vector<uint32_t> &offsets, optional_ptr<ShreddingState> shredding_state) {
	idx_t total_size = 0;
	//! Every value has at least a value header
	total_size++;

	idx_t offset_size = offsets.size();
	VariantLogicalType type_id = VariantLogicalType::VARIANT_NULL;
	if (variant.RowIsValid(row)) {
		type_id = variant.GetTypeId(row, values_index);
	}
	switch (type_id) {
	case VariantLogicalType::OBJECT: {
		auto nested_data = VariantUtils::DecodeNestedData(variant, row, values_index);

		//! Calculate value and key offsets for all children
		idx_t total_offset = 0;
		uint32_t highest_keys_index = 0;

		auto child_indices = GetChildIndices(variant, row, nested_data, shredding_state);
		if (nested_data.child_count && child_indices.empty()) {
			//! All fields of the object are shredded, omit the object entirely
			return 0;
		}

		auto num_elements = child_indices.size();
		offsets.resize(offset_size + num_elements + 1);

		for (idx_t entry = 0; entry < child_indices.size(); entry++) {
			auto i = child_indices[entry];
			auto keys_index = variant.GetKeysIndex(row, i + nested_data.children_idx);
			auto values_index = variant.GetValuesIndex(row, i + nested_data.children_idx);
			offsets[offset_size + entry] = total_offset;

			total_offset += AnalyzeValueData(variant, row, values_index, offsets, nullptr);
			highest_keys_index = MaxValue(highest_keys_index, keys_index);
		}
		offsets[offset_size + num_elements] = total_offset;

		//! Calculate the sizes for the objects value data
		auto field_id_size = CalculateByteLength(highest_keys_index);
		auto field_offset_size = CalculateByteLength(total_offset);
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
		offsets.resize(offset_size + nested_data.child_count + 1);
		for (idx_t i = 0; i < nested_data.child_count; i++) {
			auto values_index = variant.GetValuesIndex(row, i + nested_data.children_idx);
			offsets[offset_size + i] = total_offset;

			total_offset += AnalyzeValueData(variant, row, values_index, offsets, nullptr);
		}
		offsets[offset_size + nested_data.child_count] = total_offset;

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
		total_size += string_value.GetSize();
		if (type_id == VariantLogicalType::BLOB || string_value.GetSize() > 64) {
			//! Save as regular string value
			total_size += sizeof(uint32_t);
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

void CopyUUIDData(const UnifiedVariantVectorData &variant, data_ptr_t &value_data, idx_t row, uint32_t values_index) {
	auto byte_offset = variant.GetByteOffset(row, values_index);
	auto data = const_data_ptr_cast(variant.GetData(row).GetData());
	auto ptr = data + byte_offset;

	auto uuid = Load<uhugeint_t>(ptr);
	BaseUUID::ToBlob(uuid, value_data);
	value_data += sizeof(uhugeint_t);
}

static void WritePrimitiveValueData(const UnifiedVariantVectorData &variant, idx_t row, uint32_t values_index,
                                    data_ptr_t &value_data, const vector<uint32_t> &offsets, idx_t &offset_index) {
	VariantLogicalType type_id = VariantLogicalType::VARIANT_NULL;
	if (variant.RowIsValid(row)) {
		type_id = variant.GetTypeId(row, values_index);
	}

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
		CopyUUIDData(variant, value_data, row, values_index);
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

		if (decimal_data.width <= 4 || decimal_data.width > 38) {
			throw InvalidInputException("Can't convert VARIANT DECIMAL(%d, %d) to Parquet VARIANT", decimal_data.width,
			                            decimal_data.scale);
		} else if (decimal_data.width <= 9) {
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
			throw InternalException(
			    "Uncovered VARIANT(DECIMAL) -> Parquet VARIANT conversion for type 'DECIMAL(%d, %d)'",
			    decimal_data.width, decimal_data.scale);
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
                           data_ptr_t &value_data, const vector<uint32_t> &offsets, idx_t &offset_index,
                           optional_ptr<ShreddingState> shredding_state) {
	VariantLogicalType type_id = VariantLogicalType::VARIANT_NULL;
	if (variant.RowIsValid(row)) {
		type_id = variant.GetTypeId(row, values_index);
	}
	if (type_id == VariantLogicalType::OBJECT) {
		auto nested_data = VariantUtils::DecodeNestedData(variant, row, values_index);

		//! -- Object value header --

		auto child_indices = GetChildIndices(variant, row, nested_data, shredding_state);
		if (nested_data.child_count && child_indices.empty()) {
			throw InternalException(
			    "The entire should be omitted, should have been handled by the Analyze step already");
		}
		auto num_elements = child_indices.size();

		//! Determine the 'field_id_size'
		uint32_t highest_keys_index = 0;
		for (auto &i : child_indices) {
			auto keys_index = variant.GetKeysIndex(row, i + nested_data.children_idx);
			highest_keys_index = MaxValue(highest_keys_index, keys_index);
		}
		auto field_id_size = CalculateByteLength(highest_keys_index);

		uint32_t last_offset = 0;
		if (num_elements) {
			last_offset = offsets[offset_index + num_elements];
		}
		offset_index += num_elements + 1;
		auto field_offset_size = CalculateByteLength(last_offset);

		const bool is_large = num_elements > NumericLimits<uint8_t>::Maximum();

		uint8_t value_header = 0;
		value_header |= static_cast<uint8_t>(VariantBasicType::OBJECT);
		value_header |= static_cast<uint8_t>(is_large) << 6;
		value_header |= (static_cast<uint8_t>(field_id_size) - 1) << 4;
		value_header |= (static_cast<uint8_t>(field_offset_size) - 1) << 2;

#ifdef DEBUG
		auto object_value_header = VariantValueMetadata::FromHeaderByte(value_header);
		D_ASSERT(object_value_header.basic_type == VariantBasicType::OBJECT);
		D_ASSERT(object_value_header.is_large == is_large);
		D_ASSERT(object_value_header.field_offset_size == field_offset_size);
		D_ASSERT(object_value_header.field_id_size == field_id_size);
#endif

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
		for (auto &i : child_indices) {
			auto keys_index = variant.GetKeysIndex(row, i + nested_data.children_idx);
			memcpy(value_data, reinterpret_cast<data_ptr_t>(&keys_index), field_id_size);
			value_data += field_id_size;
		}

		//! Write the 'field_offset' entries and the child 'value's
		auto children_ptr = value_data + ((num_elements + 1) * field_offset_size);
		idx_t total_offset = 0;
		for (auto &i : child_indices) {
			auto values_index = variant.GetValuesIndex(row, i + nested_data.children_idx);

			memcpy(value_data, reinterpret_cast<data_ptr_t>(&total_offset), field_offset_size);
			value_data += field_offset_size;
			auto start_ptr = children_ptr;
			WriteValueData(variant, row, values_index, children_ptr, offsets, offset_index, nullptr);
			total_offset += (children_ptr - start_ptr);
		}
		memcpy(value_data, reinterpret_cast<data_ptr_t>(&total_offset), field_offset_size);
		value_data += field_offset_size;
		D_ASSERT(children_ptr - total_offset == value_data);
		value_data = children_ptr;
	} else if (type_id == VariantLogicalType::ARRAY) {
		auto nested_data = VariantUtils::DecodeNestedData(variant, row, values_index);

		//! -- Array value header --

		uint32_t last_offset = 0;
		if (nested_data.child_count) {
			last_offset = offsets[offset_index + nested_data.child_count];
		}
		offset_index += nested_data.child_count + 1;
		auto field_offset_size = CalculateByteLength(last_offset);

		auto num_elements = nested_data.child_count;
		const bool is_large = num_elements > NumericLimits<uint8_t>::Maximum();

		uint8_t value_header = 0;
		value_header |= static_cast<uint8_t>(VariantBasicType::ARRAY);
		value_header |= static_cast<uint8_t>(is_large) << 4;
		value_header |= (static_cast<uint8_t>(field_offset_size) - 1) << 2;

#ifdef DEBUG
		auto array_value_header = VariantValueMetadata::FromHeaderByte(value_header);
		D_ASSERT(array_value_header.basic_type == VariantBasicType::ARRAY);
		D_ASSERT(array_value_header.is_large == is_large);
		D_ASSERT(array_value_header.field_offset_size == field_offset_size);
#endif

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
			WriteValueData(variant, row, values_index, children_ptr, offsets, offset_index, nullptr);
			total_offset += (children_ptr - start_ptr);
		}
		memcpy(value_data, reinterpret_cast<data_ptr_t>(&total_offset), field_offset_size);
		value_data += field_offset_size;
		D_ASSERT(children_ptr - total_offset == value_data);
		value_data = children_ptr;
	} else {
		WritePrimitiveValueData(variant, row, values_index, value_data, offsets, offset_index);
	}
}

static void CreateValues(UnifiedVariantVectorData &variant, Vector &value, optional_ptr<const SelectionVector> sel,
                         optional_ptr<const SelectionVector> value_index_sel,
                         optional_ptr<const SelectionVector> result_sel, optional_ptr<ShreddingState> shredding_state,
                         idx_t count) {
	auto &validity = FlatVector::Validity(value);
	auto value_data = FlatVector::GetData<string_t>(value);

	for (idx_t i = 0; i < count; i++) {
		idx_t value_index = 0;
		if (value_index_sel) {
			value_index = value_index_sel->get_index(i);
		}

		idx_t row = i;
		if (sel) {
			row = sel->get_index(i);
		}

		idx_t result_index = i;
		if (result_sel) {
			result_index = result_sel->get_index(i);
		}

		bool is_shredded = false;
		if (variant.RowIsValid(row) && shredding_state && shredding_state->ValueIsShredded(variant, row, value_index)) {
			shredding_state->SetShredded(row, value_index, result_index);
			is_shredded = true;
			if (shredding_state->type.id() != LogicalTypeId::STRUCT) {
				//! Value is shredded, directly write a NULL to the 'value' if the type is not an OBJECT
				//! When the type is OBJECT, all excess fields would still need to be written to the 'value'
				validity.SetInvalid(result_index);
				continue;
			}
		}

		//! The (relative) offsets for each value, in the case of nesting
		vector<uint32_t> offsets;
		//! Determine the size of this 'value' blob
		idx_t blob_length = AnalyzeValueData(variant, row, value_index, offsets, shredding_state);
		if (!blob_length) {
			//! This is only allowed to happen for a shredded OBJECT, where there are no excess fields to write for the
			//! OBJECT
			(void)is_shredded;
			D_ASSERT(is_shredded);
			validity.SetInvalid(result_index);
			continue;
		}
		value_data[result_index] = StringVector::EmptyString(value, blob_length);
		auto &value_blob = value_data[result_index];
		auto value_blob_data = reinterpret_cast<data_ptr_t>(value_blob.GetDataWriteable());

		idx_t offset_index = 0;
		WriteValueData(variant, row, value_index, value_blob_data, offsets, offset_index, shredding_state);
		D_ASSERT(data_ptr_cast(value_blob.GetDataWriteable() + blob_length) == value_blob_data);
		value_blob.SetSizeAndFinalize(blob_length, blob_length);
	}
}

//! fwd-declare static method
static void WriteVariantValues(UnifiedVariantVectorData &variant, Vector &result,
                               optional_ptr<const SelectionVector> sel,
                               optional_ptr<const SelectionVector> value_index_sel,
                               optional_ptr<const SelectionVector> result_sel, idx_t count);

static void WriteTypedObjectValues(UnifiedVariantVectorData &variant, Vector &result, const SelectionVector &sel,
                                   const SelectionVector &value_index_sel, const SelectionVector &result_sel,
                                   idx_t count) {
	auto &type = result.GetType();
	D_ASSERT(type.id() == LogicalTypeId::STRUCT);

	auto &validity = FlatVector::Validity(result);
	(void)validity;

	//! Collect the nested data for the objects
	auto nested_data = make_unsafe_uniq_array_uninitialized<VariantNestedData>(count);
	for (idx_t i = 0; i < count; i++) {
		auto row = sel[i];
		//! When we're shredding an object, the top-level struct of it should always be valid
		D_ASSERT(validity.RowIsValid(result_sel[i]));
		auto value_index = value_index_sel[i];
		D_ASSERT(variant.GetTypeId(row, value_index) == VariantLogicalType::OBJECT);
		nested_data[i] = VariantUtils::DecodeNestedData(variant, row, value_index);
	}

	auto &shredded_types = StructType::GetChildTypes(type);
	auto &shredded_fields = StructVector::GetEntries(result);
	D_ASSERT(shredded_types.size() == shredded_fields.size());

	SelectionVector child_values_indexes;
	SelectionVector child_row_sel;
	SelectionVector child_result_sel;
	child_values_indexes.Initialize(count);
	child_row_sel.Initialize(count);
	child_result_sel.Initialize(count);

	for (idx_t child_idx = 0; child_idx < shredded_types.size(); child_idx++) {
		auto &child_vec = *shredded_fields[child_idx];
		D_ASSERT(child_vec.GetType() == shredded_types[child_idx].second);

		//! Prepare the path component to perform the lookup for
		auto &key = shredded_types[child_idx].first;
		VariantPathComponent path_component;
		path_component.lookup_mode = VariantChildLookupMode::BY_KEY;
		path_component.key = key;

		ValidityMask lookup_validity(count);
		VariantUtils::FindChildValues(variant, path_component, sel, child_values_indexes, lookup_validity,
		                              nested_data.get(), count);

		if (!lookup_validity.AllValid()) {
			auto &child_variant_vectors = StructVector::GetEntries(child_vec);

			//! For some of the rows the field is missing, adjust the selection vector to exclude these rows.
			idx_t child_count = 0;
			for (idx_t i = 0; i < count; i++) {
				if (!lookup_validity.RowIsValid(i)) {
					//! The field is missing, set it to null
					FlatVector::SetNull(*child_variant_vectors[0], result_sel[i], true);
					if (child_variant_vectors.size() >= 2) {
						FlatVector::SetNull(*child_variant_vectors[1], result_sel[i], true);
					}
					continue;
				}

				child_row_sel[child_count] = sel[i];
				child_values_indexes[child_count] = child_values_indexes[i];
				child_result_sel[child_count] = result_sel[i];
				child_count++;
			}

			if (child_count) {
				//! If not all rows are missing this field, write the values for it
				WriteVariantValues(variant, child_vec, child_row_sel, child_values_indexes, child_result_sel,
				                   child_count);
			}
		} else {
			WriteVariantValues(variant, child_vec, &sel, child_values_indexes, result_sel, count);
		}
	}
}

static void WriteTypedArrayValues(UnifiedVariantVectorData &variant, Vector &result, const SelectionVector &sel,
                                  const SelectionVector &value_index_sel, const SelectionVector &result_sel,
                                  idx_t count) {
	auto list_data = FlatVector::GetData<list_entry_t>(result);

	auto nested_data = make_unsafe_uniq_array_uninitialized<VariantNestedData>(count);

	idx_t total_offset = 0;
	for (idx_t i = 0; i < count; i++) {
		auto row = sel[i];
		auto value_index = value_index_sel[i];
		auto result_row = result_sel[i];

		D_ASSERT(variant.GetTypeId(row, value_index) == VariantLogicalType::ARRAY);
		nested_data[i] = VariantUtils::DecodeNestedData(variant, row, value_index);

		list_entry_t list_entry;
		list_entry.length = nested_data[i].child_count;
		list_entry.offset = total_offset;
		list_data[result_row] = list_entry;

		total_offset += nested_data[i].child_count;
	}
	ListVector::Reserve(result, total_offset);
	ListVector::SetListSize(result, total_offset);

	SelectionVector child_sel;
	child_sel.Initialize(total_offset);

	SelectionVector child_value_index_sel;
	child_value_index_sel.Initialize(total_offset);

	SelectionVector child_result_sel;
	child_result_sel.Initialize(total_offset);

	for (idx_t i = 0; i < count; i++) {
		auto row = sel[i];
		auto result_row = result_sel[i];

		auto &array_data = nested_data[i];
		auto &entry = list_data[result_row];
		for (idx_t j = 0; j < entry.length; j++) {
			auto offset = entry.offset + j;
			child_sel[offset] = row;
			child_value_index_sel[offset] = variant.GetValuesIndex(row, array_data.children_idx + j);
			child_result_sel[offset] = offset;
		}
	}

	auto &child_vector = ListVector::GetEntry(result);
	WriteVariantValues(variant, child_vector, child_sel, child_value_index_sel, child_result_sel, total_offset);
}

//! TODO: introduce a third selection vector, because we also need one to map to the result row to write
//! This becomes necessary when we introduce LISTs into the equation because lists are stored on the same VARIANT row,
//! but we're now going to write the flattened child vector
static void WriteShreddedPrimitive(UnifiedVariantVectorData &variant, Vector &result, const SelectionVector &sel,
                                   const SelectionVector &value_index_sel, const SelectionVector &result_sel,
                                   idx_t count, idx_t type_size) {
	auto result_data = FlatVector::GetData(result);
	for (idx_t i = 0; i < count; i++) {
		auto row = sel[i];
		auto result_row = result_sel[i];
		auto value_index = value_index_sel[i];
		D_ASSERT(variant.RowIsValid(row));

		auto byte_offset = variant.GetByteOffset(row, value_index);
		auto &data = variant.GetData(row);
		auto value_ptr = data.GetData();
		auto result_offset = type_size * result_row;
		memcpy(result_data + result_offset, value_ptr + byte_offset, type_size);
	}
}

template <class T>
static void WriteShreddedDecimal(UnifiedVariantVectorData &variant, Vector &result, const SelectionVector &sel,
                                 const SelectionVector &value_index_sel, const SelectionVector &result_sel,
                                 idx_t count) {
	auto result_data = FlatVector::GetData(result);
	for (idx_t i = 0; i < count; i++) {
		auto row = sel[i];
		auto result_row = result_sel[i];
		auto value_index = value_index_sel[i];
		D_ASSERT(variant.RowIsValid(row) && variant.GetTypeId(row, value_index) == VariantLogicalType::DECIMAL);

		auto decimal_data = VariantUtils::DecodeDecimalData(variant, row, value_index);
		D_ASSERT(decimal_data.width <= DecimalWidth<T>::max);
		auto result_offset = sizeof(T) * result_row;
		memcpy(result_data + result_offset, decimal_data.value_ptr, sizeof(T));
	}
}

static void WriteShreddedString(UnifiedVariantVectorData &variant, Vector &result, const SelectionVector &sel,
                                const SelectionVector &value_index_sel, const SelectionVector &result_sel,
                                idx_t count) {
	auto result_data = FlatVector::GetData<string_t>(result);
	for (idx_t i = 0; i < count; i++) {
		auto row = sel[i];
		auto result_row = result_sel[i];
		auto value_index = value_index_sel[i];
		D_ASSERT(variant.RowIsValid(row) && (variant.GetTypeId(row, value_index) == VariantLogicalType::VARCHAR ||
		                                     variant.GetTypeId(row, value_index) == VariantLogicalType::BLOB));

		auto string_data = VariantUtils::DecodeStringData(variant, row, value_index);
		result_data[result_row] = StringVector::AddStringOrBlob(result, string_data);
	}
}

static void WriteShreddedBoolean(UnifiedVariantVectorData &variant, Vector &result, const SelectionVector &sel,
                                 const SelectionVector &value_index_sel, const SelectionVector &result_sel,
                                 idx_t count) {
	auto result_data = FlatVector::GetData<bool>(result);
	for (idx_t i = 0; i < count; i++) {
		auto row = sel[i];
		auto result_row = result_sel[i];
		auto value_index = value_index_sel[i];
		D_ASSERT(variant.RowIsValid(row));
		auto type_id = variant.GetTypeId(row, value_index);
		D_ASSERT(type_id == VariantLogicalType::BOOL_FALSE || type_id == VariantLogicalType::BOOL_TRUE);

		result_data[result_row] = type_id == VariantLogicalType::BOOL_TRUE;
	}
}

static void WriteTypedPrimitiveValues(UnifiedVariantVectorData &variant, Vector &result, const SelectionVector &sel,
                                      const SelectionVector &value_index_sel, const SelectionVector &result_sel,
                                      idx_t count) {
	auto &type = result.GetType();
	D_ASSERT(!type.IsNested());
	switch (type.id()) {
	case LogicalTypeId::TINYINT:
	case LogicalTypeId::SMALLINT:
	case LogicalTypeId::INTEGER:
	case LogicalTypeId::BIGINT:
	case LogicalTypeId::FLOAT:
	case LogicalTypeId::DOUBLE:
	case LogicalTypeId::DATE:
	case LogicalTypeId::TIME:
	case LogicalTypeId::TIMESTAMP_TZ:
	case LogicalTypeId::TIMESTAMP:
	case LogicalTypeId::TIMESTAMP_NS:
	case LogicalTypeId::UUID: {
		const auto physical_type = type.InternalType();
		WriteShreddedPrimitive(variant, result, sel, value_index_sel, result_sel, count, GetTypeIdSize(physical_type));
		break;
	}
	case LogicalTypeId::DECIMAL: {
		const auto physical_type = type.InternalType();
		switch (physical_type) {
		//! DECIMAL4
		case PhysicalType::INT32:
			WriteShreddedDecimal<int32_t>(variant, result, sel, value_index_sel, result_sel, count);
			break;
		//! DECIMAL8
		case PhysicalType::INT64:
			WriteShreddedDecimal<int64_t>(variant, result, sel, value_index_sel, result_sel, count);
			break;
		//! DECIMAL16
		case PhysicalType::INT128:
			WriteShreddedDecimal<hugeint_t>(variant, result, sel, value_index_sel, result_sel, count);
			break;
		default:
			throw InvalidInputException("Can't shred on column of type '%s'", type.ToString());
		}
		break;
	}
	case LogicalTypeId::BLOB:
	case LogicalTypeId::VARCHAR: {
		WriteShreddedString(variant, result, sel, value_index_sel, result_sel, count);
		break;
	}
	case LogicalTypeId::BOOLEAN:
		WriteShreddedBoolean(variant, result, sel, value_index_sel, result_sel, count);
		break;
	default:
		throw InvalidInputException("Can't shred on type: %s", type.ToString());
	}
}

static void WriteTypedValues(UnifiedVariantVectorData &variant, Vector &result, const SelectionVector &sel,
                             const SelectionVector &value_index_sel, const SelectionVector &result_sel, idx_t count) {
	auto &type = result.GetType();

	if (type.id() == LogicalTypeId::STRUCT) {
		//! Shredded OBJECT
		WriteTypedObjectValues(variant, result, sel, value_index_sel, result_sel, count);
	} else if (type.id() == LogicalTypeId::LIST) {
		//! Shredded ARRAY
		WriteTypedArrayValues(variant, result, sel, value_index_sel, result_sel, count);
	} else {
		//! Primitive types
		WriteTypedPrimitiveValues(variant, result, sel, value_index_sel, result_sel, count);
	}
}

static void WriteVariantValues(UnifiedVariantVectorData &variant, Vector &result,
                               optional_ptr<const SelectionVector> sel,
                               optional_ptr<const SelectionVector> value_index_sel,
                               optional_ptr<const SelectionVector> result_sel, idx_t count) {
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

	if (typed_value) {
		ShreddingState shredding_state(typed_value->GetType(), count);
		CreateValues(variant, *value, sel, value_index_sel, result_sel, &shredding_state, count);

		SelectionVector null_values;
		if (shredding_state.count) {
			WriteTypedValues(variant, *typed_value, shredding_state.shredded_sel, shredding_state.values_index_sel,
			                 shredding_state.result_sel, shredding_state.count);
			//! 'shredding_state.result_sel' will always be a subset of 'result_sel', set the rows not in the subset to
			//! NULL
			idx_t sel_idx = 0;
			for (idx_t i = 0; i < count; i++) {
				auto original_index = result_sel ? result_sel->get_index(i) : i;
				if (sel_idx < shredding_state.count && shredding_state.result_sel[sel_idx] == original_index) {
					sel_idx++;
					continue;
				}
				FlatVector::SetNull(*typed_value, original_index, true);
			}
		} else {
			//! Set all rows of the typed_value to NULL, nothing is shredded on
			for (idx_t i = 0; i < count; i++) {
				FlatVector::SetNull(*typed_value, result_sel ? result_sel->get_index(i) : i, true);
			}
		}
	} else {
		CreateValues(variant, *value, sel, value_index_sel, result_sel, nullptr, count);
	}
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
	auto &metadata = *result_vectors[0];
	CreateMetadata(variant, metadata, count);
	WriteVariantValues(variant, result, nullptr, nullptr, nullptr, count);

	if (input.AllConstant()) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
	}
}

LogicalType VariantColumnWriter::TransformTypedValueRecursive(const LogicalType &type) {
	switch (type.id()) {
	case LogicalTypeId::STRUCT: {
		//! Wrap all fields of the struct in a struct with 'value' and 'typed_value' fields
		auto &child_types = StructType::GetChildTypes(type);
		child_list_t<LogicalType> replaced_types;
		for (auto &entry : child_types) {
			child_list_t<LogicalType> child_children;
			child_children.emplace_back("value", LogicalType::BLOB);
			if (entry.second.id() != LogicalTypeId::VARIANT) {
				child_children.emplace_back("typed_value", TransformTypedValueRecursive(entry.second));
			}
			replaced_types.emplace_back(entry.first, LogicalType::STRUCT(child_children));
		}
		return LogicalType::STRUCT(replaced_types);
	}
	case LogicalTypeId::LIST: {
		auto &child_type = ListType::GetChildType(type);
		child_list_t<LogicalType> replaced_types;
		replaced_types.emplace_back("value", LogicalType::BLOB);
		if (child_type.id() != LogicalTypeId::VARIANT) {
			replaced_types.emplace_back("typed_value", TransformTypedValueRecursive(child_type));
		}
		return LogicalType::LIST(LogicalType::STRUCT(replaced_types));
	}
	case LogicalTypeId::UNION:
	case LogicalTypeId::MAP:
	case LogicalTypeId::VARIANT:
	case LogicalTypeId::ARRAY:
		throw BinderException("'%s' can't appear inside the a 'typed_value' shredded type!", type.ToString());
	default:
		return type;
	}
}

static LogicalType GetParquetVariantType(optional_ptr<LogicalType> shredding = nullptr) {
	child_list_t<LogicalType> children;
	children.emplace_back("metadata", LogicalType::BLOB);
	children.emplace_back("value", LogicalType::BLOB);
	if (shredding) {
		children.emplace_back("typed_value", VariantColumnWriter::TransformTypedValueRecursive(*shredding));
	}
	auto res = LogicalType::STRUCT(std::move(children));
	res.SetAlias("PARQUET_VARIANT");
	return res;
}

static unique_ptr<FunctionData> BindTransform(ClientContext &context, ScalarFunction &bound_function,
                                              vector<unique_ptr<Expression>> &arguments) {
	if (arguments.empty()) {
		return nullptr;
	}
	auto type = ExpressionBinder::GetExpressionReturnType(*arguments[0]);

	if (arguments.size() == 2) {
		auto &shredding = *arguments[1];
		auto expr_return_type = ExpressionBinder::GetExpressionReturnType(shredding);
		expr_return_type = LogicalType::NormalizeType(expr_return_type);
		if (expr_return_type.id() != LogicalTypeId::VARCHAR) {
			throw BinderException("Optional second argument 'shredding' has to be of type VARCHAR, i.e: "
			                      "'STRUCT(my_field BOOLEAN)', found type: '%s' instead",
			                      expr_return_type);
		}
		if (!shredding.IsFoldable()) {
			throw BinderException("Optional second argument 'shredding' has to be a constant expression");
		}
		Value type_str = ExpressionExecutor::EvaluateScalar(context, shredding);
		if (type_str.IsNull()) {
			throw BinderException("Optional second argument 'shredding' can not be NULL");
		}
		auto shredded_type = TransformStringToLogicalType(type_str.GetValue<string>());
		bound_function.SetReturnType(GetParquetVariantType(shredded_type));
	} else {
		bound_function.SetReturnType(GetParquetVariantType());
	}

	return nullptr;
}

ScalarFunction VariantColumnWriter::GetTransformFunction() {
	ScalarFunction transform("variant_to_parquet_variant", {LogicalType::VARIANT()}, LogicalType::ANY, ToParquetVariant,
	                         BindTransform);
	transform.SetNullHandling(FunctionNullHandling::SPECIAL_HANDLING);
	return transform;
}

} // namespace duckdb
