#include "udf_struct_sqlite3.h"
#include "sqlite3_value_type.hpp"

#include "duckdb/common/types/vector.hpp"
#include "duckdb/common/constants.hpp"
#include "duckdb/common/operator/cast_operators.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/operator/string_cast.hpp"

namespace duckdb {

struct CastSQLite {
	static void InputVectorsToVarchar(DataChunk &data_chunk, DataChunk &new_chunk);
	static bool RequiresCastToVarchar(LogicalType type);

	static VectorType ToVectorsSQLiteValue(DataChunk &data_chunk, Vector &result,
	                                       vector<unique_ptr<vector<sqlite3_value>>> &vec_sqlites,
	                                       duckdb::unsafe_unique_array<UnifiedVectorFormat> vec_data);

	static unique_ptr<vector<sqlite3_value>> ToVector(LogicalType type, UnifiedVectorFormat &vec_data, idx_t size,
	                                                  Vector &result);
};

struct CastToSQLiteValue {
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

	struct Blob {
		template <class SRC = string_t>
		static sqlite3_value Operation(SRC blob) {
			sqlite3_value sqlite_blob;
			sqlite_blob.type = SQLiteTypeValue::BLOB;
			sqlite_blob.str = blob.GetString();
			return sqlite_blob;
		}
	};

	static sqlite3_value OperationNull();
};

struct CastToVectorSQLiteValue {

	template <class INPUT_TYPE, class OPCAST>
	static inline unique_ptr<vector<sqlite3_value>> Operation(UnifiedVectorFormat &vec_data, idx_t count) {
		unique_ptr<vector<sqlite3_value>> result = make_uniq<vector<sqlite3_value>>(count);
		auto res_data = (*result).data();

		auto input_data = UnifiedVectorFormat::GetData<INPUT_TYPE>(vec_data);

		if (vec_data.validity.AllValid()) {
			for (idx_t i = 0; i < count; ++i) {
				auto idx = vec_data.sel->get_index(i);
				res_data[i] = OPCAST::template Operation<INPUT_TYPE>(input_data[idx]);
			}
			return result;
		}

		for (idx_t i = 0; i < count; ++i) {
			auto idx = vec_data.sel->get_index(i);
			if (vec_data.validity.RowIsValidUnsafe(idx)) {
				res_data[i] = OPCAST::template Operation<INPUT_TYPE>(input_data[idx]);
			} else {
				res_data[i] = CastToSQLiteValue::OperationNull();
			}
		}
		return result;
	}

	static inline unique_ptr<vector<sqlite3_value>> FromNull(idx_t count) {
		unique_ptr<vector<sqlite3_value>> result = make_uniq<vector<sqlite3_value>>(count);
		auto res_data = (*result).data();
		for (idx_t i = 0; i < count; ++i) {
			res_data[i] = CastToSQLiteValue::OperationNull();
		}
		return result;
	}
};

// CAST to sqlite int ****************************/
template <>
sqlite3_value CastToSQLiteValue::Operation(int8_t input);

template <>
sqlite3_value CastToSQLiteValue::Operation(int16_t input);

template <>
sqlite3_value CastToSQLiteValue::Operation(int32_t input);

template <>
sqlite3_value CastToSQLiteValue::Operation(int64_t input);

// CAST to sqlite float **************************/
template <>
sqlite3_value CastToSQLiteValue::Operation(float input);

template <>
sqlite3_value CastToSQLiteValue::Operation(double input);

// CAST string **********************************/
template <>
sqlite3_value CastToSQLiteValue::Operation(char *input);

template <>
sqlite3_value CastToSQLiteValue::Operation(string_t input);

} // namespace duckdb
