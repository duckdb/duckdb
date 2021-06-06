#include "sqlite3_udf_wrapper.hpp"
#include "cast_sqlite.hpp"
#include <cassert>

#include "sqlite3.h"

#include "duckdb/common/operator/cast_operators.hpp"

scalar_function_t SQLiteUDFWrapper::CreateSQLiteScalarFunction(scalar_sqlite_udf_t sqlite_udf, void *pApp) {
	scalar_function_t udf_function = [=](DataChunk &args, ExpressionState &state, Vector &result) -> void {
		//Orrify all input colunms
		unique_ptr<VectorData[]> vec_data = args.Orrify();

		//Vector of sqlite3_value for all input columns
		vector<unique_ptr<vector<sqlite3_value>>> vec_sqlite(args.ColumnCount());

		VectorType result_vec_type = VectorType::CONSTANT_VECTOR;

		//Casting input data to sqlite_value
		for(idx_t i=0; i < args.ColumnCount(); ++i) {
			auto input_data = vec_data[i];
			auto sqlite_values = CastSQLite::ToVectorSQLite(args.data[i].GetType(), input_data, args.size());
			vec_sqlite[i] = move(sqlite_values);

			//case there is a non-constant input vector, the result must be a FLAT vector
			if(args.data[i].GetVectorType() != VectorType::CONSTANT_VECTOR) {
				result_vec_type = VectorType::FLAT_VECTOR;
			}
		}
		//set the resut vector type
		result.SetVectorType(result_vec_type);

		//to hold the last sqlite value type set by UDF
		SQLiteTypeValue res_sqlite_value_type = SQLiteTypeValue::NULL_VALUE;

		sqlite3_context context;
		context.pFunc.pUserData = pApp; //set the function data

		size_t argc = args.ColumnCount(); //num of args for the UDF
		vector<sqlite3_value> res_sqlite_values(args.size()); //to store the results from the UDF calls
		//traversing the vector of sqlite values
		for(idx_t row_idx=0; row_idx < res_sqlite_values.size(); ++row_idx) {
			sqlite3_value **argv = new sqlite3_value*[argc];

			//create a tuple from sqlite_values
			for(idx_t col_idx=0; col_idx < argc; ++col_idx) {
				argv[col_idx] = &(*(vec_sqlite[col_idx]))[row_idx];
			}
			//call the UDF on the tuple
			sqlite_udf(&context, argc, argv);

			//error set by the UDF
			if(context.isError == SQLITE_ERROR) {
				char *error_msg = context.result.str_t.GetDataWriteable();
				string str_msg(error_msg, context.result.n);
				throw std::runtime_error(str_msg.c_str());
			}

			//getting the result type set by the UDF
			if(context.result.type != SQLiteTypeValue::NULL_VALUE) {
				res_sqlite_value_type = context.result.type;
			} else {
				//NULL value set by the UDF calling sqlite3_result_null()
				if(result_vec_type == VectorType::CONSTANT_VECTOR) {
					ConstantVector::SetNull(result, true);
				} else {
					FlatVector::SetNull(result, row_idx, true);
				}
			}
			//getting result
			res_sqlite_values[row_idx] = context.result;

			if(argc > 0) {
				delete [] argv;
			}
		}

		CastSQLite::ToVectorString(res_sqlite_value_type, res_sqlite_values, result);
	};
	return udf_function;
}
