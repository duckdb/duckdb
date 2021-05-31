#include "cast_sqlite.hpp"

/*** Cast Operations *******************************/

// INT casts
template <>
sqlite3_value CastSQLite::Operation(int8_t input) {
	return OperationInt<int8_t>(input);
}

template <>
sqlite3_value CastSQLite::Operation(int16_t input) {
	return OperationInt<int16_t>(input);
}

template <>
sqlite3_value CastSQLite::Operation(int32_t input) {
	return OperationInt<int32_t>(input);
}

template <>
sqlite3_value CastSQLite::Operation(int64_t input) {
	return OperationInt<int64_t>(input);
}

// FLOAT casts
template <>
sqlite3_value CastSQLite::Operation(float input) {
	return OperationFloat<float>(input);
}

template <>
sqlite3_value CastSQLite::Operation(double input) {
	return OperationFloat<double>(input);
}

/*** Get Values ***********************************/
template <>
int64_t CastSQLite::GetValue(sqlite3_value input) {
	return input.u.i;
}

template <>
double CastSQLite::GetValue(sqlite3_value input) {
	return input.u.r;
}

//*** Cast to vectors ***********************************/

void CastSQLite::ToVectorSQLite(LogicalType type, VectorData &vec_data, vector<sqlite3_value> &result) {
	LogicalTypeId type_id = type.id();
	switch (type_id) {
	case LogicalTypeId::TINYINT: {
		ToVectorSQLiteValue<int8_t>((int8_t *)vec_data.data, (sqlite3_value *)result.data(), result.size());
		break;
	}
	case LogicalTypeId::SMALLINT: {
		ToVectorSQLiteValue<int16_t>((int16_t *)vec_data.data, (sqlite3_value *)result.data(), result.size());
		break;
	}
	case LogicalTypeId::INTEGER: {
		ToVectorSQLiteValue<int32_t>((int32_t *)vec_data.data, (sqlite3_value *)result.data(), result.size());
		break;
	}
	case LogicalTypeId::BIGINT: {
		ToVectorSQLiteValue<int64_t>((int64_t *)vec_data.data, (sqlite3_value *)result.data(), result.size());
		break;
	}
	case LogicalTypeId::FLOAT: {
		ToVectorSQLiteValue<float>((float *)vec_data.data, (sqlite3_value *)result.data(), result.size());
		break;
	}
	case LogicalTypeId::DOUBLE: {
		ToVectorSQLiteValue<double>((double *)vec_data.data, (sqlite3_value *)result.data(), result.size());
		break;
	}
	default:
		break;
	}
}

void CastSQLite::ToVectorString(SQLiteTypeValue type, vector<sqlite3_value> &vec_sqlite, Vector &result) {
	string_t *result_data;
	if(result.GetVectorType() == VectorType::CONSTANT_VECTOR) {
		result_data = ConstantVector::GetData<string_t>(result);
	} else {
		result_data = FlatVector::GetData<string_t>(result);
	}

	switch (type) {
	case SQLiteTypeValue::INTEGER: {
		ToVectorStringValue<int64_t>((sqlite3_value *)vec_sqlite.data(), vec_sqlite.size(), result_data, result);
		break;
	}
	case SQLiteTypeValue::FLOAT: {
		ToVectorStringValue<double>((sqlite3_value *)vec_sqlite.data(), vec_sqlite.size(), result_data, result);
		break;
	}
	default:
		break;
	}
}
