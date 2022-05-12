#include "sqlite3_udf_wrapper.hpp"
#include "cast_sqlite.hpp"
#include <cassert>

#include "sqlite3.h"

duckdb::scalar_function_t duckdb::SQLiteUDFWrapper::CreateSQLiteScalarFunction(duckdb::scalar_sqlite_udf_t sqlite_udf,
                                                                               sqlite3 *db_sqlite3, void *pApp) {
	duckdb::scalar_function_t udf_function = [=](DataChunk &args, ExpressionState &state, Vector &result) -> void {
		DataChunk cast_chunk;
		CastSQLite::InputVectorsToVarchar(args, cast_chunk);

		// Orrify all input colunms
		unique_ptr<VectorData[]> vec_data = cast_chunk.Orrify();

		// Vector of sqlite3_value for all input columns
		vector<unique_ptr<vector<sqlite3_value>>> vec_sqlite(cast_chunk.ColumnCount());
		// Casting input data to vectors of sqlite_value
		VectorType result_vec_type = CastSQLite::ToVectorsSQLiteValue(cast_chunk, result, vec_sqlite, move(vec_data));
		result.SetVectorType(result_vec_type);

		SQLiteTypeValue res_sqlite_value_type =
		    SQLiteTypeValue::NULL_VALUE; // to hold the last sqlite value type set by UDF

		sqlite3_context context;
		context.pFunc.pUserData = pApp; // set the function data

		size_t argc = cast_chunk.ColumnCount();                     // num of args for the UDF
		vector<sqlite3_value> res_sqlite_values(cast_chunk.size()); // to store the results from the UDF calls
		vector<sqlite3_value> vec_values_to_free; // sqlite_values to free because some memory allocation has happened

		unique_ptr<sqlite3_value *[]> argv = unique_ptr<sqlite3_value *[]>(new sqlite3_value *[argc]);

		// traversing the vector of sqlite values
		for (idx_t row_idx = 0; row_idx < res_sqlite_values.size(); ++row_idx) {
			// create a tuple from sqlite_values
			for (idx_t col_idx = 0; col_idx < argc; ++col_idx) {
				argv[col_idx] = &(*(vec_sqlite[col_idx]))[row_idx];
				argv[col_idx]->db = db_sqlite3;
				db_sqlite3->errCode = SQLITE_OK;
			}
			// call the UDF on that tuple
			context.isError = SQLITE_OK;
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
			if (context.result.type != SQLiteTypeValue::NULL_VALUE) {
				res_sqlite_value_type = context.result.type;
			} else {
				// //NULL value set by the UDF by calling sqlite3_result_null()
				if (result_vec_type == VectorType::CONSTANT_VECTOR) {
					ConstantVector::SetNull(result, true);
				} else {
					FlatVector::SetNull(result, row_idx, true);
				}
			}
			// getting result
			res_sqlite_values[row_idx] = context.result;
		}

		CastSQLite::ToVectorString(res_sqlite_value_type, res_sqlite_values, result);
	};
	return udf_function;
}
