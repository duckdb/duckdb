#include "writer/variant_column_writer.hpp"
#include "parquet_writer.hpp"

namespace duckdb {

unique_ptr<ParquetAnalyzeSchemaState> VariantColumnWriter::AnalyzeSchemaInit() {
	return make_uniq<VariantAnalyzeSchemaState>();
}

static void AnalyzeSchemaInternal(VariantAnalyzeData &state, UnifiedVariantVectorData &variant, idx_t row,
                                  uint32_t values_index) {
	if (!variant.RowIsValid(row)) {
		state.type_map[static_cast<uint8_t>(VariantLogicalType::VARIANT_NULL)]++;
		return;
	}

	auto type_id = variant.GetTypeId(row, values_index);
	state.type_map[static_cast<uint8_t>(type_id)]++;

	if (type_id == VariantLogicalType::OBJECT) {
		if (!state.object_data) {
			state.object_data = make_uniq<ObjectAnalyzeData>();
		}
		auto &object_data = *state.object_data;

		auto nested_data = VariantUtils::DecodeNestedData(variant, row, values_index);
		for (idx_t i = 0; i < nested_data.child_count; i++) {
			auto child_values_index = variant.GetValuesIndex(row, i + nested_data.children_idx);
			auto child_key_index = variant.GetKeysIndex(row, i + nested_data.children_idx);

			auto &key = variant.GetKey(row, child_key_index);
			auto &child_state = object_data.fields[key.GetString()];
			AnalyzeSchemaInternal(child_state, variant, row, child_values_index);
		}
	} else if (type_id == VariantLogicalType::ARRAY) {
		if (!state.array_data) {
			state.array_data = make_uniq<ArrayAnalyzeData>();
		}
		auto &array_data = *state.array_data;
		auto nested_data = VariantUtils::DecodeNestedData(variant, row, values_index);
		for (idx_t i = 0; i < nested_data.child_count; i++) {
			auto child_values_index = variant.GetValuesIndex(row, i + nested_data.children_idx);
			auto &child_state = array_data.child;
			AnalyzeSchemaInternal(child_state, variant, row, child_values_index);
		}
	} else if (type_id == VariantLogicalType::DECIMAL) {
		auto decimal_data = VariantUtils::DecodeDecimalData(variant, row, values_index);
		auto physical_type = decimal_data.GetPhysicalType();
		switch (physical_type) {
		case PhysicalType::INT32:
			state.decimal_type_map[0]++;
			break;
		case PhysicalType::INT64:
			state.decimal_type_map[1]++;
			break;
		case PhysicalType::INT128:
			state.decimal_type_map[2]++;
			break;
		default:
			break;
		}
	} else if (type_id == VariantLogicalType::BOOL_FALSE) {
		//! Move it to bool_true to have the counts all in one place
		state.type_map[static_cast<uint8_t>(VariantLogicalType::BOOL_TRUE)]++;
		state.type_map[static_cast<uint8_t>(VariantLogicalType::BOOL_FALSE)]--;
	}
}

void VariantColumnWriter::AnalyzeSchema(ParquetAnalyzeSchemaState &state_p, Vector &input, idx_t count) {
	auto &state = state_p.Cast<VariantAnalyzeSchemaState>();

	RecursiveUnifiedVectorFormat recursive_format;
	Vector::RecursiveToUnifiedFormat(input, count, recursive_format);
	UnifiedVariantVectorData variant(recursive_format);

	for (idx_t i = 0; i < count; i++) {
		AnalyzeSchemaInternal(state.analyze_data, variant, i, 0);
	}
}

namespace {

struct ShredAnalysisState {
	idx_t highest_count = 0;
	LogicalTypeId type_id;
	PhysicalType decimal_type;
};

} // namespace

template <VariantLogicalType VARIANT_TYPE, LogicalTypeId SHREDDED_TYPE>
static void CheckPrimitive(const VariantAnalyzeData &state, ShredAnalysisState &result) {
	auto count = state.type_map[static_cast<uint8_t>(VARIANT_TYPE)];
	if (VARIANT_TYPE == VariantLogicalType::DECIMAL) {
		if (!count) {
			return;
		}
		auto int32_count = state.decimal_type_map[0];
		if (int32_count > result.highest_count) {
			result.type_id = LogicalTypeId::DECIMAL;
			result.decimal_type = PhysicalType::INT32;
		}
		auto int64_count = state.decimal_type_map[1];
		if (int64_count > result.highest_count) {
			result.type_id = LogicalTypeId::DECIMAL;
			result.decimal_type = PhysicalType::INT64;
		}
		auto int128_count = state.decimal_type_map[2];
		if (int128_count > result.highest_count) {
			result.type_id = LogicalTypeId::DECIMAL;
			result.decimal_type = PhysicalType::INT128;
		}
	} else {
		if (count > result.highest_count) {
			result.highest_count = count;
			result.type_id = SHREDDED_TYPE;
		}
	}
}

static LogicalType ConstructShreddedType(const VariantAnalyzeData &state) {
	ShredAnalysisState result;

	CheckPrimitive<VariantLogicalType::BOOL_TRUE, LogicalTypeId::BOOLEAN>(state, result);
	CheckPrimitive<VariantLogicalType::INT8, LogicalTypeId::TINYINT>(state, result);
	CheckPrimitive<VariantLogicalType::INT16, LogicalTypeId::SMALLINT>(state, result);
	CheckPrimitive<VariantLogicalType::INT32, LogicalTypeId::INTEGER>(state, result);
	CheckPrimitive<VariantLogicalType::INT64, LogicalTypeId::BIGINT>(state, result);
	CheckPrimitive<VariantLogicalType::FLOAT, LogicalTypeId::FLOAT>(state, result);
	CheckPrimitive<VariantLogicalType::DOUBLE, LogicalTypeId::DOUBLE>(state, result);
	CheckPrimitive<VariantLogicalType::DECIMAL, LogicalTypeId::DECIMAL>(state, result);
	CheckPrimitive<VariantLogicalType::DATE, LogicalTypeId::DATE>(state, result);
	CheckPrimitive<VariantLogicalType::TIME_MICROS, LogicalTypeId::TIME>(state, result);
	CheckPrimitive<VariantLogicalType::TIMESTAMP_MICROS, LogicalTypeId::TIMESTAMP>(state, result);
	CheckPrimitive<VariantLogicalType::TIMESTAMP_NANOS, LogicalTypeId::TIMESTAMP_NS>(state, result);
	CheckPrimitive<VariantLogicalType::TIMESTAMP_MICROS_TZ, LogicalTypeId::TIMESTAMP_TZ>(state, result);
	CheckPrimitive<VariantLogicalType::BLOB, LogicalTypeId::BLOB>(state, result);
	CheckPrimitive<VariantLogicalType::VARCHAR, LogicalTypeId::VARCHAR>(state, result);
	CheckPrimitive<VariantLogicalType::UUID, LogicalTypeId::UUID>(state, result);

	auto array_count = state.type_map[static_cast<uint8_t>(VariantLogicalType::ARRAY)];
	auto object_count = state.type_map[static_cast<uint8_t>(VariantLogicalType::OBJECT)];
	if (array_count > object_count) {
		if (array_count > result.highest_count) {
			auto &array_data = *state.array_data;
			return LogicalType::LIST(ConstructShreddedType(array_data.child));
		}
	} else {
		if (object_count > result.highest_count) {
			auto &object_data = *state.object_data;

			//! TODO: implement some logic to determine which fields are worth shredding, considering the overhead when
			//! only 10% of rows make use of the field
			child_list_t<LogicalType> field_types;
			for (auto &field : object_data.fields) {
				field_types.emplace_back(field.first, ConstructShreddedType(field.second));
			}
			return LogicalType::STRUCT(field_types);
		}
	}

	if (result.type_id == LogicalTypeId::DECIMAL) {
		//! TODO: what should the scale be???
		if (result.decimal_type == PhysicalType::INT32) {
			return LogicalType::DECIMAL(DecimalWidth<int32_t>::max, 0);
		} else if (result.decimal_type == PhysicalType::INT64) {
			return LogicalType::DECIMAL(DecimalWidth<int64_t>::max, 0);
		} else if (result.decimal_type == PhysicalType::INT128) {
			return LogicalType::DECIMAL(DecimalWidth<hugeint_t>::max, 0);
		}
	}
	return result.type_id;
}

void VariantColumnWriter::AnalyzeSchemaFinalize(const ParquetAnalyzeSchemaState &state_p) {
	auto &state = state_p.Cast<VariantAnalyzeSchemaState>();
	auto shredded_type = ConstructShreddedType(state.analyze_data);

	auto typed_value = TransformTypedValueRecursive(shredded_type);

	auto &schema = Schema();
	auto &context = writer.GetContext();
	D_ASSERT(child_writers.size() == 2);
	child_writers.pop_back();
	//! Recreate the column writer for 'value' because this is now "optional"
	child_writers.push_back(ColumnWriter::CreateWriterRecursive(context, writer, schema_path, LogicalType::BLOB,
	                                                            "value", nullptr, nullptr, schema.max_repeat,
	                                                            schema.max_define + 1, true));
	child_writers.push_back(ColumnWriter::CreateWriterRecursive(context, writer, schema_path, typed_value,
	                                                            "typed_value", nullptr, nullptr, schema.max_repeat,
	                                                            schema.max_define + 1, true));
}

} // namespace duckdb
