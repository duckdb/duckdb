#include "duckdb/common/exception.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/planner/expression/bound_argument_pack.hpp"
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
bool ProcessRow(idx_t row_idx, const vector<VectorIterator<string_t>> &inputs, idx_t col_count, string &out_result) {
	Path out_path;
	for (idx_t col_idx = 0; col_idx < col_count; col_idx++) {
		auto &vdata = inputs[col_idx];
		auto input_val = vdata[row_idx];
		if (!input_val.IsValid()) {
			return false;
		}
		auto input_value = input_val.GetValue().GetString();
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
	vector<VectorIterator<string_t>> inputs;
	inputs.emplace_back(args.data[0].Values<string_t>());
	for (const auto &path : ArgumentPack::GetInput(args.data[1])) {
		inputs.emplace_back(path.Values<string_t>());
	}
	const auto col_count = inputs.size();

	auto result_data = FlatVector::Writer<string_t>(result, count);
	for (idx_t row_idx = 0; row_idx < count; row_idx++) {
		string joined;
		if (!ProcessRow(row_idx, inputs, col_count, joined)) {
			result_data.WriteNull();
			continue;
		}
		result_data.WriteValue(string_t(joined));
	}
}

} // namespace

ScalarFunction PathJoinFun::GetFunction() {
	auto sig = FunctionSignature()
	               .AddParameter("path", LogicalType::VARCHAR)
	               .AddVarPositionalParameter("paths", LogicalType::VARCHAR)
	               .SetReturnType(LogicalType::VARCHAR);

	auto fun = ScalarFunction(PathJoinFun::Name, std::move(sig))
	               .SetFunctionCallback(PathJoinFunction)
	               .SetNullHandling(FunctionNullHandling::DEFAULT_NULL_HANDLING)
	               .SetFallible(); // throws if the paths that are joined are incompatible

	return fun;
}

} // namespace duckdb
