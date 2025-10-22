#include "duckdb/storage/table/variant_column_data.hpp"
#include "duckdb/common/types/variant.hpp"
#include "duckdb/common/types/variant_visitor.hpp"
#include "duckdb/function/variant/variant_shredding.hpp"

namespace duckdb {

namespace {

struct VariantShreddedData {
public:
	VariantShreddedData(Vector &unshredded, Vector &shredded)
	    : unshredded(unshredded), shredded(shredded), untyped_value_index(*StructVector::GetEntries(shredded)[0]),
	      typed_value(*StructVector::GetEntries(shredded)[1]) {

		if (typed_value.GetType().id() == LogicalTypeId::STRUCT) {
			auto &child_types = StructType::GetChildTypes(typed_value.GetType());
			auto &typed_value_children = StructVector::GetEntries(typed_value);
			for (uint32_t i = 0; i < static_cast<uint32_t>(child_types.size()); i++) {
				auto &field_name = child_types[i].first;
				field_map.emplace(string_t(field_name.c_str(), field_name.size()), *typed_value_children[i]);
			}
		}
		is_list = typed_value.GetType().id() == LogicalTypeId::LIST;
	}

public:
	optional_ptr<Vector> GetVectorForField(const string_t &field_name) {
		auto it = field_map.find(field_name);
		if (it == field_map.end()) {
			return nullptr;
		}
		return it->second.get();
	}
	optional_ptr<Vector> GetVectorForElement() {
		if (!is_list) {
			return nullptr;
		}
		return ListVector::GetEntry(typed_value);
	}

public:
	Vector &unshredded;
	Vector &shredded;
	Vector &untyped_value_index;
	Vector &typed_value;

	case_insensitive_string_map_t<reference<Vector>> field_map;
	bool is_list;
};

struct VariantShreddingVisitor {
	using result_type = void;

	static void VisitNull(VariantShreddedData &field_stats) {
		return;
	}
	static void VisitBoolean(bool val, VariantShreddedData &field_stats) {
		return;
	}

	static void VisitMetadata(VariantLogicalType type_id, VariantShreddedData &field_stats) {
		// field_stats.SetType(type_id);
	}

	template <typename T>
	static void VisitInteger(T val, VariantShreddedData &field_stats) {
	}
	static void VisitFloat(float val, VariantShreddedData &field_stats) {
	}
	static void VisitDouble(double val, VariantShreddedData &field_stats) {
	}
	static void VisitUUID(hugeint_t val, VariantShreddedData &field_stats) {
	}
	static void VisitDate(date_t val, VariantShreddedData &field_stats) {
	}
	static void VisitInterval(interval_t val, VariantShreddedData &field_stats) {
	}
	static void VisitTime(dtime_t val, VariantShreddedData &field_stats) {
	}
	static void VisitTimeNanos(dtime_ns_t val, VariantShreddedData &field_stats) {
	}
	static void VisitTimeTZ(dtime_tz_t val, VariantShreddedData &field_stats) {
	}
	static void VisitTimestampSec(timestamp_sec_t val, VariantShreddedData &field_stats) {
	}
	static void VisitTimestampMs(timestamp_ms_t val, VariantShreddedData &field_stats) {
	}
	static void VisitTimestamp(timestamp_t val, VariantShreddedData &field_stats) {
	}
	static void VisitTimestampNanos(timestamp_ns_t val, VariantShreddedData &field_stats) {
	}
	static void VisitTimestampTZ(timestamp_tz_t val, VariantShreddedData &field_stats) {
	}
	static void WriteStringInternal(const string_t &str, VariantShreddedData &field_stats) {
	}
	static void VisitString(const string_t &str, VariantShreddedData &field_stats) {
	}
	static void VisitBlob(const string_t &blob, VariantShreddedData &field_stats) {
	}
	static void VisitBignum(const string_t &bignum, VariantShreddedData &field_stats) {
	}
	static void VisitGeometry(const string_t &geom, VariantShreddedData &field_stats) {
	}
	static void VisitBitstring(const string_t &bits, VariantShreddedData &field_stats) {
	}

	template <typename T>
	static void VisitDecimal(T val, uint32_t width, uint32_t scale, VariantShreddedData &field_stats) {
		//! FIXME: need to visit to be able to shred on DECIMAL values
	}

	static void VisitArray(const UnifiedVariantVectorData &variant, idx_t row, const VariantNestedData &nested_data,
	                       VariantShreddedData &field_stats) {
		VariantVisitor<VariantShreddingVisitor>::VisitArrayItems(variant, row, nested_data, field_stats);
	}

	static void VisitObject(const UnifiedVariantVectorData &variant, idx_t row, const VariantNestedData &nested_data,
	                        VariantShreddedData &field_stats) {
		//! Then visit the fields in sorted order
		for (idx_t i = 0; i < nested_data.child_count; i++) {
			auto source_children_idx = nested_data.children_idx + i;

			//! Add the key of the field to the result
			auto keys_index = variant.GetKeysIndex(row, source_children_idx);
			auto &key = variant.GetKey(row, keys_index);

			// auto &child_stats = field_stats.GetOrCreateField(stats, key.GetString());

			////! Visit the child value
			// auto values_index = variant.GetValuesIndex(row, source_children_idx);
			// VariantVisitor<VariantShreddingVisitor>::Visit(variant, row, values_index, stats, child_stats);
		}
	}

	static void VisitDefault(VariantLogicalType type_id, const_data_ptr_t, VariantShreddedData &field_stats) {
		throw InternalException("VariantLogicalType(%s) not handled", EnumUtil::ToString(type_id));
	}
};

struct DuckDBVariantShredding : public VariantShredding {
public:
	DuckDBVariantShredding() : VariantShredding() {
	}
	~DuckDBVariantShredding() override = default;

public:
	void WriteVariantValues(UnifiedVariantVectorData &variant, Vector &result, optional_ptr<const SelectionVector> sel,
	                        optional_ptr<const SelectionVector> value_index_sel,
	                        optional_ptr<const SelectionVector> result_sel, idx_t count) override;
};

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

struct DuckDBVariantShreddingState : public VariantShreddingState {
public:
	DuckDBVariantShreddingState(const LogicalType &type, idx_t total_count)
	    : VariantShreddingState(type, total_count), variant_types(GetVariantType(type)) {
	}

public:
	const unordered_set<VariantLogicalType> &GetVariantTypes() override {
		return variant_types;
	}

private:
	unordered_set<VariantLogicalType> variant_types;
};

} // namespace

void DuckDBVariantShredding::WriteVariantValues(UnifiedVariantVectorData &variant, Vector &result,
                                                optional_ptr<const SelectionVector> sel,
                                                optional_ptr<const SelectionVector> value_index_sel,
                                                optional_ptr<const SelectionVector> result_sel, idx_t count) {
	auto &result_type = result.GetType();
	D_ASSERT(result_type.id() == LogicalTypeId::STRUCT);
	auto &child_types = StructType::GetChildTypes(result_type);
	auto &child_vectors = StructVector::GetEntries(result);
	D_ASSERT(child_types.size() == child_vectors.size());

	auto &untyped_value_index = *child_vectors[0];
	auto &typed_value = *child_vectors[1];

	DuckDBVariantShreddingState shredding_state(typed_value.GetType(), count);
	CreateValues(variant, untyped_value_index, sel, value_index_sel, result_sel, &shredding_state, count);

	SelectionVector null_values;
	if (shredding_state.count) {
		WriteTypedValues(variant, typed_value, shredding_state.shredded_sel, shredding_state.values_index_sel,
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
			FlatVector::SetNull(typed_value, original_index, true);
		}
	} else {
		//! Set all rows of the typed_value to NULL, nothing is shredded on
		for (idx_t i = 0; i < count; i++) {
			FlatVector::SetNull(typed_value, result_sel ? result_sel->get_index(i) : i, true);
		}
	}
}

void VariantColumnData::ShredVariantData(Vector &input, Vector &output, idx_t count, const LogicalType &shredded_type) {
	RecursiveUnifiedVectorFormat recursive_format;
	Vector::RecursiveToUnifiedFormat(input, count, recursive_format);
	UnifiedVariantVectorData variant(recursive_format);

	auto &child_vectors = StructVector::GetEntries(output);
	VariantShreddedData shredded_data(*child_vectors[0], *child_vectors[1]);
	for (idx_t i = 0; i < count; i++) {
		VariantVisitor<VariantShreddingVisitor>::Visit(variant, i, 0, shredded_data);
	}
}

} // namespace duckdb
