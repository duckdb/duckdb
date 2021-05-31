#include "sqlite3_udf_wrapper.hpp"
#include "cast_sqlite.hpp"
#include <cassert>

#include "duckdb/common/operator/cast_operators.hpp"

scalar_function_t SQLiteUDFWrapper::CreateBinarySQLiteFunction(scalar_sqlite_udf_t sqlite_udf) {
	scalar_function_t udf_function = [=](DataChunk &args, ExpressionState &state, Vector &result) -> void {
		assert(args.ColumnCount() == 1);

		VectorData vec_data;
		args.data[0].Orrify(args.size(), vec_data);

		vector<sqlite3_value> vec_sqlite(args.size());
		CastSQLite::ToVectorSQLite(args.data[0].GetType(), vec_data, vec_sqlite);

		sqlite3_context context;
		int argc = 1;
		vector<sqlite3_value> res_sqlite(args.size());
		for(idx_t i=0; i < vec_sqlite.size(); ++i) {
			sqlite3_value *value = &vec_sqlite[i];

			//calling the SQLite3 UDF
			sqlite_udf(&context, argc, &value);

			//getting result
			res_sqlite[i] = context.result;
		}

		CastSQLite::ToVectorString(context.result.type, res_sqlite, result);
	};
	return udf_function;
}
