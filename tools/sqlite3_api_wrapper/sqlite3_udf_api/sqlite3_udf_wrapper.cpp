#include "sqlite3_udf_wrapper.hpp"
#include "cast_sqlite.hpp"
#include <cassert>

#include "sqlite3.h"
#include "duckdb/common/types/blob.hpp"
#include "duckdb/common/operator/string_cast.hpp"

duckdb::scalar_function_t duckdb::SQLiteUDFWrapper::CreateSQLiteScalarFunction(duckdb::scalar_sqlite_udf_t sqlite_udf,
                                                                               sqlite3 *db_sqlite3, void *pApp) {
	duckdb::scalar_function_t udf_function = [=](DataChunk &args, ExpressionState &state, Vector &result) -> void {
		DataChunk cast_chunk;
		CastSQLite::InputVectorsToVarchar(args, cast_chunk);

		// ToUnifiedFormat all input colunms
		auto vec_data = cast_chunk.ToUnifiedFormat();

		// Vector of sqlite3_value for all input columns
		vector<unique_ptr<vector<sqlite3_value>>> vec_sqlite(cast_chunk.ColumnCount());
		// Casting input data to vectors of sqlite_value
		VectorType result_vec_type =
		    CastSQLite::ToVectorsSQLiteValue(cast_chunk, result, vec_sqlite, std::move(vec_data));
		sqlite3_context context;
		context.pFunc.pUserData = pApp; // set the function data

		size_t argc = cast_chunk.ColumnCount(); // num of args for the UDF

		duckdb::unique_ptr<sqlite3_value *[]> argv = duckdb::unique_ptr<sqlite3_value *[]>(new sqlite3_value *[argc]);

		// traversing the vector of sqlite values
		auto result_data = FlatVector::GetData<string_t>(result);
		for (idx_t row_idx = 0; row_idx < cast_chunk.size(); ++row_idx) {
			// create a tuple from sqlite_values
			for (idx_t col_idx = 0; col_idx < argc; ++col_idx) {
				argv[col_idx] = &(*(vec_sqlite[col_idx]))[row_idx];
				argv[col_idx]->db = db_sqlite3;
				db_sqlite3->errCode = SQLITE_OK;
			}
			// call the UDF on that tuple
			context.isError = SQLITE_OK;
			context.result.type = SQLiteTypeValue::NULL_VALUE;
			sqlite_udf(&context, argc, argv.get());

			// check memory allocatated by the sqlite_values
			// error set by the UDF
			if (context.isError != SQLITE_OK) {
				string str_msg;
				if (context.result.type == SQLiteTypeValue::TEXT) {
					str_msg = context.result.str;
				} else {
					str_msg = "Error in SQLite UDF, but no error message was provided";
				}
				throw std::runtime_error(str_msg.c_str());
			}

			// getting the result type set by the UDF
			switch (context.result.type) {
			case SQLiteTypeValue::INTEGER:
				result_data[row_idx] = StringCast::Operation(context.result.u.i, result);
				break;
			case SQLiteTypeValue::FLOAT:
				result_data[row_idx] = StringCast::Operation(context.result.u.r, result);
				break;
			case SQLiteTypeValue::TEXT:
				result_data[row_idx] = StringVector::AddString(result, context.result.str);
				break;
			case SQLiteTypeValue::BLOB:
				result_data[row_idx] = StringVector::AddString(result, Blob::ToString(context.result.str));
				break;
			case SQLiteTypeValue::NULL_VALUE:
				// NULL value set by the UDF by calling sqlite3_result_null()
				FlatVector::SetNull(result, row_idx, true);
				break;
			default:
				throw InternalException("Unrecognized SQLiteTypeValue in type conversion");
			}
		}
		result.SetVectorType(result_vec_type);
	};
	return udf_function;
}
