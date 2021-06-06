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

// STRING cast
template <>
sqlite3_value CastSQLite::Operation(string_t input) {
	sqlite3_value sqlite_str;
	sqlite_str.type = SQLiteTypeValue::TEXT;
	sqlite_str.n = input.GetSize();
	sqlite_str.str_t = input;
	return sqlite_str;
}

sqlite3_value CastSQLite::OperationBlob(string_t input) {
	sqlite3_value sqlite_blob;
	sqlite_blob.type = SQLiteTypeValue::BLOB;
	sqlite_blob.n = input.GetSize();
	sqlite_blob.str_t = input;
	return sqlite_blob;
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

template <>
string_t CastSQLite::GetValue(sqlite3_value input) {
	return input.str_t;
}

//*** Cast to vectors ***********************************/
unique_ptr<vector<sqlite3_value>> CastSQLite::ToVectorSQLite(LogicalType type, VectorData &vec_data, idx_t size) {
	LogicalTypeId type_id = type.id();
	switch (type_id) {
	case LogicalTypeId::TINYINT: {
		return ToVectorSQLiteValue<int8_t>((int8_t *)vec_data.data, size);
	}
	case LogicalTypeId::SMALLINT: {
		return ToVectorSQLiteValue<int16_t>((int16_t *)vec_data.data, size);
	}
	case LogicalTypeId::INTEGER: {
		return ToVectorSQLiteValue<int32_t>((int32_t *)vec_data.data, size);
	}
	case LogicalTypeId::BIGINT: {
		return ToVectorSQLiteValue<int64_t>((int64_t *)vec_data.data, size);
	}
	case LogicalTypeId::FLOAT: {
		return ToVectorSQLiteValue<float>((float *)vec_data.data, size);
	}
	case LogicalTypeId::DOUBLE: {
		return ToVectorSQLiteValue<double>((double *)vec_data.data, size);
	}
	case LogicalTypeId::BLOB: {
		return ToVectorSQLiteValueBlob((string_t *)vec_data.data, size);
	}
	case LogicalTypeId::VARCHAR: {
		return ToVectorSQLiteValue<string_t>((string_t *)vec_data.data, size);
	}
	default:
		throw std::runtime_error("SQLite UDF API: type is not supported!");
	}
}

void CastSQLite::ToVectorString(SQLiteTypeValue type, vector<sqlite3_value> &vec_sqlite, Vector &result) {
	string_t *result_data;
	if (result.GetVectorType() == VectorType::CONSTANT_VECTOR) {
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
	case SQLiteTypeValue::BLOB:
	case SQLiteTypeValue::TEXT: {
		ToVectorStringValue<string_t>((sqlite3_value *)vec_sqlite.data(), vec_sqlite.size(), result_data, result);
		break;
	}
	default:
		break;
	}
}

template <>
void CastSQLite::ToVectorStringValue<string_t>(sqlite3_value *__restrict data, idx_t count,
                                               string_t *__restrict result_data, Vector &result) {
	for (idx_t i = 0; i < count; ++i) {
		string_t value = GetValue<string_t>(data[i]);
		result_data[i] = value;
	}
}
