#include "cast_sqlite.hpp"

#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/common/types/validity_mask.hpp"

namespace duckdb {

bool CastSQLite::RequiresCastToVarchar(LogicalType type) {
	LogicalTypeId type_id = type.id();
	switch (type_id) {
	case LogicalTypeId::TINYINT:
	case LogicalTypeId::SMALLINT:
	case LogicalTypeId::INTEGER:
	case LogicalTypeId::BIGINT:
	case LogicalTypeId::FLOAT:
	case LogicalTypeId::DOUBLE:
	case LogicalTypeId::CHAR:
	case LogicalTypeId::VARCHAR:
	case LogicalTypeId::BLOB:
	case LogicalTypeId::SQLNULL:
		return false; // supported types
	default:
		return true; // types need casting to varchar
	}
}

void CastSQLite::InputVectorsToVarchar(DataChunk &data_chunk, DataChunk &new_chunk) {
	new_chunk.SetCardinality(data_chunk.size());
	if (data_chunk.ColumnCount() == 0) {
		return;
	}
	auto new_types = data_chunk.GetTypes();
	for (auto &type : new_types) {
		if (CastSQLite::RequiresCastToVarchar(type)) {
			type = LogicalType::VARCHAR;
		}
	}
	new_chunk.Initialize(Allocator::DefaultAllocator(), new_types);

	for (idx_t i = 0; i < data_chunk.ColumnCount(); ++i) {
		if (CastSQLite::RequiresCastToVarchar(data_chunk.data[i].GetType())) {
			VectorOperations::Cast(data_chunk.data[i], new_chunk.data[i], data_chunk.size(), true);
		} else {
			new_chunk.data[i].Reference(data_chunk.data[i]);
		}
	}
}

VectorType CastSQLite::ToVectorsSQLiteValue(DataChunk &data_chunk, Vector &result,
                                            vector<unique_ptr<vector<sqlite3_value>>> &vec_sqlite_values,
                                            unique_ptr<UnifiedVectorFormat[]> vec_data) {
	VectorType result_vec_type = VectorType::CONSTANT_VECTOR;

	// Casting input data to sqlite_value
	for (idx_t i = 0; i < data_chunk.ColumnCount(); ++i) {
		auto input_data = vec_data[i];
		auto sqlite_values = CastSQLite::ToVector(data_chunk.data[i].GetType(), input_data, data_chunk.size(), result);
		vec_sqlite_values[i] = move(sqlite_values);

		// case there is a non-constant input vector, the result must be a FLAT vector
		if (data_chunk.data[i].GetVectorType() != VectorType::CONSTANT_VECTOR) {
			result_vec_type = VectorType::FLAT_VECTOR;
		}
	}
	return result_vec_type;
}

//*** Cast to vectors ***********************************/
unique_ptr<vector<sqlite3_value>> CastSQLite::ToVector(LogicalType type, UnifiedVectorFormat &vec_data, idx_t size,
                                                       Vector &result) {
	LogicalTypeId type_id = type.id();
	switch (type_id) {
	case LogicalTypeId::TINYINT: {
		return CastToVectorSQLiteValue::Operation<int8_t, CastToSQLiteValue>(vec_data, size);
	}
	case LogicalTypeId::SMALLINT: {
		return CastToVectorSQLiteValue::Operation<int16_t, CastToSQLiteValue>(vec_data, size);
	}
	case LogicalTypeId::INTEGER: {
		return CastToVectorSQLiteValue::Operation<int32_t, CastToSQLiteValue>(vec_data, size);
	}
	case LogicalTypeId::BIGINT: {
		return CastToVectorSQLiteValue::Operation<int64_t, CastToSQLiteValue>(vec_data, size);
	}
	case LogicalTypeId::FLOAT: {
		return CastToVectorSQLiteValue::Operation<float, CastToSQLiteValue>(vec_data, size);
	}
	case LogicalTypeId::DOUBLE: {
		return CastToVectorSQLiteValue::Operation<double, CastToSQLiteValue>(vec_data, size);
	}
	case LogicalTypeId::BLOB: {
		return CastToVectorSQLiteValue::Operation<string_t, CastToSQLiteValue::Blob>(vec_data, size);
	}
	case LogicalTypeId::SQLNULL: {
		return CastToVectorSQLiteValue::FromNull(size);
	}
	case LogicalTypeId::CHAR:
	case LogicalTypeId::VARCHAR:
	default:
		return CastToVectorSQLiteValue::Operation<string_t, CastToSQLiteValue>(vec_data, size);
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
		if (result.GetVectorType() == VectorType::CONSTANT_VECTOR) {
			ConstantVector::SetNull(result, true);
		}
		break;
	}
}

template <>
void CastSQLite::ToVectorStringValue<string_t>(sqlite3_value *__restrict data, idx_t count,
                                               string_t *__restrict result_data, Vector &result) {
	for (idx_t i = 0; i < count; ++i) {
		string_t str_value = CastFromSQLiteValue::GetValue<string_t>(data[i]);
		result_data[i] = StringVector::AddString(result, str_value);
	}
}

/*** Cast Single Value Operations *****************************/

// INT casts
template <>
sqlite3_value CastToSQLiteValue::Operation(int8_t input) {
	return OperationInt<int8_t>(input);
}

template <>
sqlite3_value CastToSQLiteValue::Operation(int16_t input) {
	return OperationInt<int16_t>(input);
}

template <>
sqlite3_value CastToSQLiteValue::Operation(int32_t input) {
	return OperationInt<int32_t>(input);
}

template <>
sqlite3_value CastToSQLiteValue::Operation(int64_t input) {
	return OperationInt<int64_t>(input);
}

// FLOAT casts
template <>
sqlite3_value CastToSQLiteValue::Operation(float input) {
	return OperationFloat<float>(input);
}

template <>
sqlite3_value CastToSQLiteValue::Operation(double input) {
	return OperationFloat<double>(input);
}

// STRING cast
template <>
sqlite3_value CastToSQLiteValue::Operation(string_t input) {
	sqlite3_value sqlite_str;
	sqlite_str.type = SQLiteTypeValue::TEXT;
	sqlite_str.str = input.GetString();
	return sqlite_str;
}

sqlite3_value CastToSQLiteValue::OperationNull() {
	sqlite3_value sqlite_null;
	sqlite_null.type = SQLiteTypeValue::NULL_VALUE;
	sqlite_null.u.i = 0;
	sqlite_null.u.r = 0.0;
	return sqlite_null;
}

/*** Get Values *******************************************/
template <>
int64_t CastFromSQLiteValue::GetValue(sqlite3_value input) {
	return input.u.i;
}

template <>
double CastFromSQLiteValue::GetValue(sqlite3_value input) {
	return input.u.r;
}

template <>
string_t CastFromSQLiteValue::GetValue(sqlite3_value input) {
	return string_t(input.str);
}

} // namespace duckdb
