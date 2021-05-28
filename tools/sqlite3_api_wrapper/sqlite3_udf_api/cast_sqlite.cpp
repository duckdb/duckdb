#include "cast_sqlite.hpp"

template <>
sqlite3_value CastSQLite::Operation(int32_t input) {
    sqlite3_value sqlite_int;
    sqlite_int.u.i = input;
	// sqlite_int.flags = MEM_Int;
	sqlite_int.type = PhysicalType::INT32;
    return sqlite_int;
}

template <>
sqlite3_value CastSQLite::Operation(double input) {
    sqlite3_value sqlite_double;
    sqlite_double.u.r = input;
    // sqlite_double.flags = MEM_Real;
	sqlite_double.type = PhysicalType::DOUBLE;
    return sqlite_double;
}

void CastSQLite::ToVector(LogicalType type, VectorData &vec_data, vector<sqlite3_value> &result) {
	LogicalTypeId type_id = type.id();
	switch (type_id) {
	case LogicalTypeId::INTEGER: {
		ToVectorSQLiteValue<int32_t>((int32_t *)vec_data.data, (sqlite3_value *)result.data(), result.size());
		break;
	}
	default:
		break;
	}
}
