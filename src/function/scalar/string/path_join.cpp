#include "duckdb/common/exception.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/function/scalar/string_functions.hpp"

namespace duckdb {

namespace {

struct PathJoinBindData : public FunctionData {
	explicit PathJoinBindData(idx_t path_count_p) : path_count(path_count_p) {
	}

	idx_t path_count;

public:
	unique_ptr<FunctionData> Copy() const override {
		return make_uniq<PathJoinBindData>(path_count);
	}

	bool Equals(const FunctionData &other) const override {
		auto &o = other.Cast<PathJoinBindData>();
		return path_count == o.path_count;
	}
};

// Process one output row; returns false if any input is NULL.
static bool ProcessRow(idx_t row_idx, const vector<UnifiedVectorFormat> &inputs, idx_t col_count, FileSystem &fs,
                       string &out_result) {
	Path out_path;
	for (idx_t col_idx = 0; col_idx < col_count; col_idx++) {
		auto &vdata = inputs[col_idx];
		auto idx = vdata.sel->get_index(row_idx);
		if (!vdata.validity.RowIsValid(idx)) {
			return false;
		}
		auto input_value = UnifiedVectorFormat::GetData<string_t>(vdata)[idx].GetString();
		if (col_idx == 0) {
			out_path = Path::FromString(input_value);
		} else {
			out_path = out_path.Join(input_value);
		}
	}
	out_result = out_path.ToString();
	return true;
}

void PathJoinFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto count = args.size();
	auto col_count = args.ColumnCount();
	auto &context = state.GetContext();
	auto &fs = FileSystem::GetFileSystem(context);

	vector<UnifiedVectorFormat> inputs(args.ColumnCount());
	for (idx_t col_idx = 0; col_idx < args.ColumnCount(); col_idx++) {
		args.data[col_idx].ToUnifiedFormat(count, inputs[col_idx]);
	}

	// constant fast path
	if (args.AllConstant() && count > 0) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
		auto &validity = ConstantVector::Validity(result);
		auto result_data = ConstantVector::GetData<string_t>(result);
		string joined;
		if (!ProcessRow(0, inputs, col_count, fs, joined)) {
			validity.SetInvalid(0);
		} else {
			result_data[0] = StringVector::AddString(result, joined);
		}
		return;
	}

	auto result_data = FlatVector::Writer<string_t>(result, count);
	for (idx_t row_idx = 0; row_idx < count; row_idx++) {
		string joined;
		if (!ProcessRow(row_idx, inputs, col_count, fs, joined)) {
			result_data.WriteNull();
			continue;
		}
		result_data.WriteValue(string_t(joined));
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
