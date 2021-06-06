#include "udf_struct_sqlite3.h"
#include "sqlite3_value_type.hpp"

#include "duckdb/common/types/vector.hpp"
#include "duckdb/common/constants.hpp"
#include "duckdb/common/operator/cast_operators.hpp"

using namespace duckdb;
using namespace std;

struct CastSQLite {
    template <class SRC>
    static inline sqlite3_value Operation(SRC input) {
        return (sqlite3_value)input;
    }

	template <class SRC>
    static inline sqlite3_value OperationInt(SRC input) {
        sqlite3_value sqlite_int;
		sqlite_int.u.i = input;
		sqlite_int.type = SQLiteTypeValue::INTEGER;
		return sqlite_int;
    }


	template <class SRC>
    static inline sqlite3_value OperationFloat(SRC input) {
        sqlite3_value sqlite_float;
		sqlite_float.u.r = input;
		sqlite_float.type = SQLiteTypeValue::FLOAT;
		return sqlite_float;
    }

	template<class T>
	static inline unique_ptr<vector<sqlite3_value>> ToVectorSQLiteValue(T *__restrict data, idx_t count) {
		unique_ptr<vector<sqlite3_value>> result = make_unique<vector<sqlite3_value>>(count);
		auto res_data = (*result).data();
		for(idx_t i=0; i < count; ++i) {
			res_data[i] = CastSQLite::Operation(data[i]);
		}
		return result;
	}

	static sqlite3_value OperationBlob(string_t blob);

	static inline unique_ptr<vector<sqlite3_value>> ToVectorSQLiteValueBlob(string_t *__restrict data, idx_t count) {
		unique_ptr<vector<sqlite3_value>> result = make_unique<vector<sqlite3_value>>(count);
		auto res_data = (*result).data();
		for(idx_t i=0; i < count; ++i) {
			res_data[i] = CastSQLite::OperationBlob(data[i]);
		}
		return result;
	}

	static unique_ptr<vector<sqlite3_value>> ToVectorSQLite(LogicalType type, VectorData &vec_data, idx_t size);

	/**
	 * Noop GetValue()
	 * Specialized GetValue() must be implemented for every needed type
	 */
    template <class DST>
    static inline DST GetValue(sqlite3_value input) {
		DST value;
        return value;
    }

	template<class T>
	static void ToVectorStringValue(sqlite3_value *__restrict data,  idx_t count, string_t *__restrict result_data, Vector &result) {
		for(idx_t i=0; i < count; ++i) {
		    T value = GetValue<T>(data[i]);
			result_data[i] = StringCast::Operation(value, result);
		}
	}
	static void ToVectorString(SQLiteTypeValue type, vector<sqlite3_value> &vec_sqlite, Vector &result);
};

template<>
void CastSQLite::ToVectorStringValue<string_t>(sqlite3_value *__restrict data,  idx_t count, string_t *__restrict result_data, Vector &result);


// CAST to sqlite int ****************************/
template <>
sqlite3_value CastSQLite::Operation(int8_t input);

template <>
sqlite3_value CastSQLite::Operation(int16_t input);

template <>
sqlite3_value CastSQLite::Operation(int32_t input);

template <>
sqlite3_value CastSQLite::Operation(int64_t input);

// CAST to sqlite float **************************/
template <>
sqlite3_value CastSQLite::Operation(float input);

template <>
sqlite3_value CastSQLite::Operation(double input);

// CAST string **********************************/
template <>
sqlite3_value CastSQLite::Operation(char *input);

template <>
sqlite3_value CastSQLite::Operation(string_t input);


//GET value from sqlite int (sqlite.u.i) ********/
template <>
int64_t CastSQLite::GetValue(sqlite3_value input);

//GET value from sqlite float (sqlite.u.r) ******/
template <>
double CastSQLite::GetValue(sqlite3_value input);

template <>
string_t CastSQLite::GetValue(sqlite3_value input);
