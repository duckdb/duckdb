#include "duckdb/common/file_system.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/function/scalar/string_functions.hpp"

namespace duckdb {

namespace {

void PathJoinFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto count = args.size();
	auto col_count = args.ColumnCount();
	auto &context = state.GetContext();
	auto &fs = FileSystem::GetFileSystem(context);

	result.SetVectorType(VectorType::FLAT_VECTOR);
	auto &validity = FlatVector::Validity(result);
	auto result_data = FlatVector::GetData<string_t>(result);

	vector<UnifiedVectorFormat> inputs(col_count);
	for (idx_t col_idx = 0; col_idx < col_count; col_idx++) {
		args.data[col_idx].ToUnifiedFormat(count, inputs[col_idx]);
	}

	for (idx_t row_idx = 0; row_idx < count; row_idx++) {
		bool has_null = false;
		string current;

		for (idx_t col_idx = 0; col_idx < col_count; col_idx++) {
			auto &vdata = inputs[col_idx];
			auto idx = vdata.sel->get_index(row_idx);
			if (!vdata.validity.RowIsValid(idx)) {
				validity.SetInvalid(row_idx);
				has_null = true;
				break;
			}
			auto input_value = UnifiedVectorFormat::GetData<string_t>(vdata)[idx].GetString();
			if (col_idx == 0) {
				current = input_value;
			} else {
				current = fs.JoinPath(current, input_value);
			}
		}

		if (has_null) {
			continue;
		}
		result_data[row_idx] = StringVector::AddString(result, current);
	}
}

} // namespace

ScalarFunction PathJoinFun::GetFunction() {
	ScalarFunction path_join(PathJoinFun::Name, {LogicalType::VARCHAR}, LogicalType::VARCHAR, PathJoinFunction);
	path_join.varargs = LogicalType::VARCHAR;
	path_join.null_handling = FunctionNullHandling::DEFAULT_NULL_HANDLING;
	return path_join;
}

} // namespace duckdb
