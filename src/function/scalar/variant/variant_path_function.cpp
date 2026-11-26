#include "duckdb/function/scalar/variant_path_function.hpp"
#include "duckdb/common/numeric_utils.hpp"

namespace duckdb {

optional<VariantNode> VariantPathFunction::ResolveIndex(const VariantNode &node, int64_t index) {
	if (node.IsNull() || node.IsMissing() || node.GetTypeId() != LogicalTypeId::LIST) {
		return {};
	}
	const auto elements = node.GetArrayChildren();
	const auto length = elements.size();
	if (index >= 0) {
		if (static_cast<idx_t>(index) >= length) {
			return {};
		}
		return elements[static_cast<idx_t>(index)];
	}
	// -(index + 1) cannot overflow for the minimum int64_t
	const auto from_end = static_cast<idx_t>(-(index + 1));
	if (from_end >= length) {
		return {};
	}
	return elements[length - 1 - from_end];
}

vector<optional<string_t>> VariantPathFunction::GetSingleKeys(const vector<vector<VariantPathComponent>> &paths) {
	vector<optional<string_t>> keys(paths.size());
	for (idx_t path_idx = 0; path_idx < paths.size(); path_idx++) {
		const auto &path = paths[path_idx];
		if (path.size() == 1 && path[0].lookup_mode == VariantChildLookupMode::BY_KEY) {
			keys[path_idx] = string_t(path[0].key.c_str(), NumericCast<uint32_t>(path[0].key.size()));
		}
	}
	return keys;
}

void VariantPathFunction::ResolvePaths(const VariantNode &root, const vector<vector<VariantPathComponent>> &paths,
                                       const vector<optional<string_t>> &keys,
                                       vector<optional<VariantNode>> &selected) {
	//! Copied into locals so the matching loop keeps them in registers
	const auto path_count = paths.size();
	const auto key_data = keys.data();
	const auto selected_data = selected.data();

	idx_t pending = 0;
	for (idx_t path_idx = 0; path_idx < path_count; path_idx++) {
		if (key_data[path_idx]) {
			selected_data[path_idx].reset();
			pending++;
		} else {
			selected_data[path_idx] = ResolvePath(root, paths[path_idx]);
		}
	}
	if (pending == 0 || root.IsNull() || root.IsMissing() || root.GetTypeId() != LogicalTypeId::STRUCT) {
		return;
	}
	for (const auto &entry : root.GetObjectChildren()) {
		for (idx_t path_idx = 0; path_idx < path_count; path_idx++) {
			auto &value = selected_data[path_idx];
			const auto &key = key_data[path_idx];
			if (key && !value && *key == entry.key) {
				value = entry.value;
				pending--;
			}
		}
		if (pending == 0) {
			break;
		}
	}
}

ScalarFunctionSet VariantPathFunction::CreateFunctionSet(const Identifier &name, const scalar_function_t &function,
                                                         const LogicalType &return_type, const bool path_optional,
                                                         const init_local_state_t init_state) {
	ScalarFunctionSet fun_set(name);

	if (path_optional) {
		ScalarFunction no_path_fun({}, return_type, function, VariantBindUtils::VariantPathBind, nullptr);
		no_path_fun.GetSignature().AddParameter("input_variant", LogicalType::VARIANT());
		fun_set.AddFunction(std::move(no_path_fun));
	}

	ScalarFunction path_fun({}, return_type, function, VariantBindUtils::VariantPathBind, nullptr, init_state);
	path_fun.GetSignature()
	    .AddParameter("input_variant", LogicalType::VARIANT())
	    .AddParameter("path", LogicalType::VARCHAR);
	fun_set.AddFunction(std::move(path_fun));

	ScalarFunction path_list_fun({}, LogicalType::LIST(return_type), function, VariantBindUtils::VariantPathBind,
	                             nullptr, init_state);
	path_list_fun.GetSignature()
	    .AddParameter("input_variant", LogicalType::VARIANT())
	    .AddParameter("path", LogicalType::LIST(LogicalType::VARCHAR));
	fun_set.AddFunction(std::move(path_list_fun));

	return fun_set;
}

} // namespace duckdb
