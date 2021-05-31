#include "sqlite3_udf_wrapper.hpp"
#include "cast_sqlite.hpp"
#include <cassert>

#include "duckdb/common/operator/cast_operators.hpp"

scalar_function_t SQLiteUDFWrapper::CreateSQLiteScalarFunction(scalar_sqlite_udf_t sqlite_udf) {
	scalar_function_t udf_function = [=](DataChunk &args, ExpressionState &state, Vector &result) -> void {
		//Orrify all input colunms
		unique_ptr<VectorData[]> vec_data = args.Orrify();

		//Vector of sqlite3_value for all input columns
		vector<unique_ptr<vector<sqlite3_value>>> vec_sqlite(args.ColumnCount());

		//Casting input data to sqlite_value
		for(idx_t i=0; i < args.ColumnCount(); ++i) {
			auto input_data = vec_data[i];
			auto sqlite_values = CastSQLite::ToVectorSQLite(args.data[i].GetType(), input_data, args.size());
			vec_sqlite[i] = move(sqlite_values);
		}

		sqlite3_context context;
		size_t argc = args.ColumnCount();
		vector<sqlite3_value> res_sqlite(args.size());
		//Call
		for(idx_t row_idx=0; row_idx < res_sqlite.size(); ++row_idx) {
			sqlite3_value **argv = new sqlite3_value*[argc];
			for(idx_t col_idx=0; col_idx < argc; ++col_idx) {
				argv[col_idx] = &(*(vec_sqlite[col_idx]))[row_idx];
			}
			//calling the SQLite3 UDF
			sqlite_udf(&context, argc, argv);

			//getting result
			res_sqlite[row_idx] = context.result;

			if(argc > 0) {
				delete [] argv;
			}
		}

		CastSQLite::ToVectorString(context.result.type, res_sqlite, result);
	};
	return udf_function;
}