#include "duckdb/storage/statistics/numeric_statistics.hpp"

#include "duckdb/common/field_writer.hpp"
#include "duckdb/common/operator/comparison_operators.hpp"
#include "duckdb/common/types/vector.hpp"

namespace duckdb {

template <>
void NumericStatistics::Update<interval_t>(SegmentStatistics &stats, interval_t new_value) {
}

template <>
void NumericStatistics::Update<list_entry_t>(SegmentStatistics &stats, list_entry_t new_value) {
}

NumericStatistics::NumericStatistics(LogicalType type_p, StatisticsType stats_type)
    : BaseStatistics(std::move(type_p), stats_type) {
	InitializeBase();
	min = Value::MaximumValue(type);
	max = Value::MinimumValue(type);
}

NumericStatistics::NumericStatistics(LogicalType type_p, Value min_p, Value max_p, StatisticsType stats_type)
    : BaseStatistics(std::move(type_p), stats_type), min(std::move(min_p)), max(std::move(max_p)) {
	InitializeBase();
}

void NumericStatistics::Merge(const BaseStatistics &other_p) {
	BaseStatistics::Merge(other_p);
	auto &other = (const NumericStatistics &)other_p;
	if (other.min.IsNull() || min.IsNull()) {
		min = Value(type);
	} else if (other.min < min) {
		min = other.min;
	}
	if (other.max.IsNull() || max.IsNull()) {
		max = Value(type);
	} else if (other.max > max) {
		max = other.max;
	}
}

FilterPropagateResult NumericStatistics::CheckZonemap(ExpressionType comparison_type, const Value &constant) const {
	if (constant.IsNull()) {
		return FilterPropagateResult::FILTER_ALWAYS_FALSE;
	}
	if (min.IsNull() || max.IsNull()) {
		return FilterPropagateResult::NO_PRUNING_POSSIBLE;
	}
	switch (comparison_type) {
	case ExpressionType::COMPARE_EQUAL:
		if (constant == min && constant == max) {
			return FilterPropagateResult::FILTER_ALWAYS_TRUE;
		} else if (constant >= min && constant <= max) {
			return FilterPropagateResult::NO_PRUNING_POSSIBLE;
		} else {
			return FilterPropagateResult::FILTER_ALWAYS_FALSE;
		}
	case ExpressionType::COMPARE_NOTEQUAL:
		if (constant < min || constant > max) {
			return FilterPropagateResult::FILTER_ALWAYS_TRUE;
		} else if (min == max && min == constant) {
			// corner case of a cluster with one numeric equal to the target constant
			return FilterPropagateResult::FILTER_ALWAYS_FALSE;
		}
		return FilterPropagateResult::NO_PRUNING_POSSIBLE;
	case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
		// X >= C
		// this can be true only if max(X) >= C
		// if min(X) >= C, then this is always true
		if (min >= constant) {
			return FilterPropagateResult::FILTER_ALWAYS_TRUE;
		} else if (max >= constant) {
			return FilterPropagateResult::NO_PRUNING_POSSIBLE;
		} else {
			return FilterPropagateResult::FILTER_ALWAYS_FALSE;
		}
	case ExpressionType::COMPARE_GREATERTHAN:
		// X > C
		// this can be true only if max(X) > C
		// if min(X) > C, then this is always true
		if (min > constant) {
			return FilterPropagateResult::FILTER_ALWAYS_TRUE;
		} else if (max > constant) {
			return FilterPropagateResult::NO_PRUNING_POSSIBLE;
		} else {
			return FilterPropagateResult::FILTER_ALWAYS_FALSE;
		}
	case ExpressionType::COMPARE_LESSTHANOREQUALTO:
		// X <= C
		// this can be true only if min(X) <= C
		// if max(X) <= C, then this is always true
		if (max <= constant) {
			return FilterPropagateResult::FILTER_ALWAYS_TRUE;
		} else if (min <= constant) {
			return FilterPropagateResult::NO_PRUNING_POSSIBLE;
		} else {
			return FilterPropagateResult::FILTER_ALWAYS_FALSE;
		}
	case ExpressionType::COMPARE_LESSTHAN:
		// X < C
		// this can be true only if min(X) < C
		// if max(X) < C, then this is always true
		if (max < constant) {
			return FilterPropagateResult::FILTER_ALWAYS_TRUE;
		} else if (min < constant) {
			return FilterPropagateResult::NO_PRUNING_POSSIBLE;
		} else {
			return FilterPropagateResult::FILTER_ALWAYS_FALSE;
		}
	default:
		throw InternalException("Expression type in zonemap check not implemented");
	}
}

unique_ptr<BaseStatistics> NumericStatistics::Copy() const {
	auto result = make_unique<NumericStatistics>(type, min, max, stats_type);
	result->CopyBase(*this);
	return std::move(result);
}

bool NumericStatistics::IsConstant() const {
	return max <= min;
}

void SerializeNumericStatsValue(const Value &val, FieldWriter &writer) {
	writer.WriteField<bool>(val.IsNull());
	if (val.IsNull()) {
		return;
	}
	switch (val.type().InternalType()) {
	case PhysicalType::BOOL:
		writer.WriteField<bool>(BooleanValue::Get(val));
		break;
	case PhysicalType::INT8:
		writer.WriteField<int8_t>(TinyIntValue::Get(val));
		break;
	case PhysicalType::INT16:
		writer.WriteField<int16_t>(SmallIntValue::Get(val));
		break;
	case PhysicalType::INT32:
		writer.WriteField<int32_t>(IntegerValue::Get(val));
		break;
	case PhysicalType::INT64:
		writer.WriteField<int64_t>(BigIntValue::Get(val));
		break;
	case PhysicalType::UINT8:
		writer.WriteField<int8_t>(UTinyIntValue::Get(val));
		break;
	case PhysicalType::UINT16:
		writer.WriteField<int16_t>(USmallIntValue::Get(val));
		break;
	case PhysicalType::UINT32:
		writer.WriteField<int32_t>(UIntegerValue::Get(val));
		break;
	case PhysicalType::UINT64:
		writer.WriteField<int64_t>(UBigIntValue::Get(val));
		break;
	case PhysicalType::INT128:
		writer.WriteField<hugeint_t>(HugeIntValue::Get(val));
		break;
	case PhysicalType::FLOAT:
		writer.WriteField<float>(FloatValue::Get(val));
		break;
	case PhysicalType::DOUBLE:
		writer.WriteField<double>(DoubleValue::Get(val));
		break;
	default:
		throw InternalException("Unsupported type for serializing numeric statistics");
	}
}

void NumericStatistics::Serialize(FieldWriter &writer) const {
	SerializeNumericStatsValue(min, writer);
	SerializeNumericStatsValue(max, writer);
}

Value DeserializeNumericStatsValue(const LogicalType &type, FieldReader &reader) {
	auto is_null = reader.ReadRequired<bool>();
	if (is_null) {
		return Value(type);
	}
	Value result;
	switch (type.InternalType()) {
	case PhysicalType::BOOL:
		result = Value::BOOLEAN(reader.ReadRequired<bool>());
		break;
	case PhysicalType::INT8:
		result = Value::TINYINT(reader.ReadRequired<int8_t>());
		break;
	case PhysicalType::INT16:
		result = Value::SMALLINT(reader.ReadRequired<int16_t>());
		break;
	case PhysicalType::INT32:
		result = Value::INTEGER(reader.ReadRequired<int32_t>());
		break;
	case PhysicalType::INT64:
		result = Value::BIGINT(reader.ReadRequired<int64_t>());
		break;
	case PhysicalType::UINT8:
		result = Value::UTINYINT(reader.ReadRequired<uint8_t>());
		break;
	case PhysicalType::UINT16:
		result = Value::USMALLINT(reader.ReadRequired<uint16_t>());
		break;
	case PhysicalType::UINT32:
		result = Value::UINTEGER(reader.ReadRequired<uint32_t>());
		break;
	case PhysicalType::UINT64:
		result = Value::UBIGINT(reader.ReadRequired<uint64_t>());
		break;
	case PhysicalType::INT128:
		result = Value::HUGEINT(reader.ReadRequired<hugeint_t>());
		break;
	case PhysicalType::FLOAT:
		result = Value::FLOAT(reader.ReadRequired<float>());
		break;
	case PhysicalType::DOUBLE:
		result = Value::DOUBLE(reader.ReadRequired<double>());
		break;
	default:
		throw InternalException("Unsupported type for deserializing numeric statistics");
	}
	result.Reinterpret(type);
	return result;
}

unique_ptr<BaseStatistics> NumericStatistics::Deserialize(FieldReader &reader, LogicalType type) {
	auto min = DeserializeNumericStatsValue(type, reader);
	auto max = DeserializeNumericStatsValue(type, reader);
	return make_unique_base<BaseStatistics, NumericStatistics>(std::move(type), std::move(min), std::move(max),
	                                                           StatisticsType::LOCAL_STATS);
}

string NumericStatistics::ToString() const {
	return StringUtil::Format("[Min: %s, Max: %s]%s", min.ToString(), max.ToString(), BaseStatistics::ToString());
}

template <class T>
void NumericStatistics::TemplatedVerify(Vector &vector, const SelectionVector &sel, idx_t count) const {
	UnifiedVectorFormat vdata;
	vector.ToUnifiedFormat(count, vdata);

	auto data = (T *)vdata.data;
	for (idx_t i = 0; i < count; i++) {
		auto idx = sel.get_index(i);
		auto index = vdata.sel->get_index(idx);
		if (!vdata.validity.RowIsValid(index)) {
			continue;
		}
		if (!min.IsNull() && LessThan::Operation(data[index], min.GetValueUnsafe<T>())) { // LCOV_EXCL_START
			throw InternalException("Statistics mismatch: value is smaller than min.\nStatistics: %s\nVector: %s",
			                        ToString(), vector.ToString(count));
		} // LCOV_EXCL_STOP
		if (!max.IsNull() && GreaterThan::Operation(data[index], max.GetValueUnsafe<T>())) {
			throw InternalException("Statistics mismatch: value is bigger than max.\nStatistics: %s\nVector: %s",
			                        ToString(), vector.ToString(count));
		}
	}
}

void NumericStatistics::Verify(Vector &vector, const SelectionVector &sel, idx_t count) const {
	BaseStatistics::Verify(vector, sel, count);

	switch (type.InternalType()) {
	case PhysicalType::BOOL:
		break;
	case PhysicalType::INT8:
		TemplatedVerify<int8_t>(vector, sel, count);
		break;
	case PhysicalType::INT16:
		TemplatedVerify<int16_t>(vector, sel, count);
		break;
	case PhysicalType::INT32:
		TemplatedVerify<int32_t>(vector, sel, count);
		break;
	case PhysicalType::INT64:
		TemplatedVerify<int64_t>(vector, sel, count);
		break;
	case PhysicalType::UINT8:
		TemplatedVerify<uint8_t>(vector, sel, count);
		break;
	case PhysicalType::UINT16:
		TemplatedVerify<uint16_t>(vector, sel, count);
		break;
	case PhysicalType::UINT32:
		TemplatedVerify<uint32_t>(vector, sel, count);
		break;
	case PhysicalType::UINT64:
		TemplatedVerify<uint64_t>(vector, sel, count);
		break;
	case PhysicalType::INT128:
		TemplatedVerify<hugeint_t>(vector, sel, count);
		break;
	case PhysicalType::FLOAT:
		TemplatedVerify<float>(vector, sel, count);
		break;
	case PhysicalType::DOUBLE:
		TemplatedVerify<double>(vector, sel, count);
		break;
	default:
		throw InternalException("Unsupported type %s for numeric statistics verify", type.ToString());
	}
}

} // namespace duckdb
