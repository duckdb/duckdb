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
		CastSQLite::ToVector(args.data[0].GetType(), vec_data, vec_sqlite);

		string_t *result_data;
		if(result.GetVectorType() == VectorType::CONSTANT_VECTOR) {
			result_data = ConstantVector::GetData<string_t>(result);
		} else {
			result_data = FlatVector::GetData<string_t>(result);
		}

		sqlite3_context context;
		int argc = 1;
		for(idx_t i=0; i < vec_sqlite.size(); ++i) {
			sqlite3_value *value = &vec_sqlite[i];

			//calling the SQLite3 UDF
			sqlite_udf(&context, argc, &value);

			//casting the result sqlite value to string_t
			if(TypeIsInteger(context.result.type)) {
     		    auto value = context.result.u.i;
				result_data[i] = StringCast::Operation(value, result);
    		} else {
				//TODO implement other types
			}
		}
	};
	return udf_function;
}
