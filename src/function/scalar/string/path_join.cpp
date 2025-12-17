#include "duckdb/common/exception.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/function/scalar/string_functions.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/execution/expression_executor.hpp"

namespace duckdb {

namespace {

struct PathJoinBindData : public FunctionData {
	explicit PathJoinBindData(bool strict_p, idx_t path_count_p) : strict(strict_p), path_count(path_count_p) {
	}

	bool strict;
	idx_t path_count;

public:
	unique_ptr<FunctionData> Copy() const override {
		return make_uniq<PathJoinBindData>(strict, path_count);
	}

	bool Equals(const FunctionData &other) const override {
		auto &o = other.Cast<PathJoinBindData>();
		return strict == o.strict && path_count == o.path_count;
	}
};

// Process one output row; returns true if row is NULL (in non-strict mode) or fills out_result otherwise.
static bool ProcessRow(idx_t row_idx, ValidityMask &validity, const vector<UnifiedVectorFormat> &inputs,
                       idx_t col_count, FileSystem &fs, bool strict, string &out_result) {
	for (idx_t col_idx = 0; col_idx < col_count; col_idx++) {
		auto &vdata = inputs[col_idx];
		auto idx = vdata.sel->get_index(row_idx);
		if (!vdata.validity.RowIsValid(idx)) {
			if (strict) {
				throw InvalidInputException("path_join: NULL argument in strict mode");
			}
			validity.SetInvalid(row_idx);
			return true;
		}
		auto input_value = UnifiedVectorFormat::GetData<string_t>(vdata)[idx].GetString();
		try {
			if (col_idx == 0) {
				out_result = input_value;
			} else {
				out_result = fs.JoinPath(out_result, input_value);
			}
		} catch (...) {
			if (strict) {
				throw;
			}
			validity.SetInvalid(row_idx);
			return true;
		}
	}
	return false;
}

void PathJoinFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto count = args.size();
	auto &func_expr = state.expr.Cast<BoundFunctionExpression>();
	auto &bind = func_expr.bind_info->Cast<PathJoinBindData>();
	auto col_count = bind.path_count;
	auto &context = state.GetContext();
	auto &fs = FileSystem::GetFileSystem(context);

	vector<UnifiedVectorFormat> inputs(args.ColumnCount());
	for (idx_t col_idx = 0; col_idx < args.ColumnCount(); col_idx++) {
		args.data[col_idx].ToUnifiedFormat(count, inputs[col_idx]);
	}

	// constant fast path
	bool all_constant = true;
	for (idx_t col_idx = 0; col_idx < col_count; col_idx++) {
		if (args.data[col_idx].GetVectorType() != VectorType::CONSTANT_VECTOR) {
			all_constant = false;
			break;
		}
	}
	if (all_constant && count > 0) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
		auto &validity = ConstantVector::Validity(result);
		auto result_data = ConstantVector::GetData<string_t>(result);
		string joined;
		bool is_null = ProcessRow(0, validity, inputs, col_count, fs, bind.strict, joined);
		if (!is_null) {
			result_data[0] = StringVector::AddString(result, joined);
		}
		return;
	}

	result.SetVectorType(VectorType::FLAT_VECTOR);
	auto &validity = FlatVector::Validity(result);
	auto result_data = FlatVector::GetData<string_t>(result);

	for (idx_t row_idx = 0; row_idx < count; row_idx++) {
		string joined;
		bool is_null = ProcessRow(row_idx, validity, inputs, col_count, fs, bind.strict, joined);
		if (is_null) {
			continue;
		}
		result_data[row_idx] = StringVector::AddString(result, joined);
	}
}

} // namespace

unique_ptr<FunctionData> PathJoinBind(ClientContext &context, ScalarFunction &bound_function,
                                      vector<unique_ptr<Expression>> &arguments) {
	if (arguments.empty()) {
		throw BinderException("path_join requires at least one argument");
	}
	bool strict = true;
	idx_t path_count = arguments.size();
	vector<LogicalType> arg_types(arguments.size(), LogicalType::VARCHAR);
	// optional strict flag if the last argument is boolean
	if (arguments.back()->return_type == LogicalType::BOOLEAN) {
		if (!arguments.back()->IsFoldable()) {
			throw BinderException("path_join strict flag must be a constant boolean");
		}
		auto strict_val = ExpressionExecutor::EvaluateScalar(context, *arguments.back());
		if (!strict_val.IsNull()) {
			strict = BooleanValue::Get(strict_val);
		}
		path_count--;
		if (path_count == 0) {
			throw BinderException("path_join requires at least one path argument");
		}
		arg_types.back() = LogicalType::BOOLEAN;
	}
	bound_function.arguments = std::move(arg_types);
	bound_function.null_handling = FunctionNullHandling::SPECIAL_HANDLING;
	return make_uniq<PathJoinBindData>(strict, path_count);
}

ScalarFunction PathJoinFun::GetFunction() {
	ScalarFunction path_join(PathJoinFun::Name, {LogicalType::VARCHAR}, LogicalType::VARCHAR, PathJoinFunction);
	path_join.bind = PathJoinBind;
	path_join.varargs = LogicalType::ANY;
	path_join.null_handling = FunctionNullHandling::SPECIAL_HANDLING;
	return path_join;
}

} // namespace duckdb
