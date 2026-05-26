#include "duckdb/function/scalar/variant_utils.hpp"
#include "duckdb/execution/expression_executor.hpp"

namespace duckdb {

VariantPathBindData::VariantPathBindData() : FunctionData() {
}
VariantPathBindData::VariantPathBindData(const string &input_path) : FunctionData() {
	if (input_path.empty()) {
		paths.emplace_back();
	} else {
		paths.push_back({VariantPathComponent(input_path)});
	}
}
VariantPathBindData::VariantPathBindData(const vector<string> &input_paths) : FunctionData() {
	for (const auto &path : input_paths) {
		if (path.empty()) {
			paths.emplace_back();
		} else {
			paths.push_back({VariantPathComponent(path)});
		}
	}
}

unique_ptr<FunctionData> VariantPathBindData::Copy() const {
	return make_uniq<VariantPathBindData>(*this);
}

bool VariantPathBindData::Equals(const FunctionData &other) const {
	auto &bind_data = other.Cast<VariantPathBindData>();
	if (paths.size() != bind_data.paths.size()) {
		return false;
	}

	for (idx_t i = 0; i < paths.size(); i++) {
		if (paths[i].size() != bind_data.paths[i].size()) {
			return false;
		}
		for (idx_t j = 0; j < paths[i].size(); j++) {
			if (paths[i][j] != bind_data.paths[i][j]) {
				return false;
			}
		}
	}
	return true;
}

bool VariantBindUtils::GetConstantArgument(ClientContext &context, const Expression &expr, Value &constant_arg) {
	if (!expr.IsFoldable()) {
		return false;
	}
	constant_arg = ExpressionExecutor::EvaluateScalar(context, expr);
	if (!constant_arg.IsNull()) {
		return true;
	}
	return false;
}

} // namespace duckdb
