#include "duckdb/function/scalar/variant_utils.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/common/string_map_set.hpp"

namespace duckdb {

VariantPathKeyIndex::VariantPathKeyIndex(const vector<optional<string_t>> &keys) {
	string_map_t<vector<idx_t>> positions_by_key;
	for (idx_t path_idx = 0; path_idx < keys.size(); path_idx++) {
		if (keys[path_idx]) {
			positions_by_key[*keys[path_idx]].push_back(path_idx);
		}
	}
	//! at most half of the slots are used, so a lookup ends at an unused slot
	const auto slot_count = NextPowerOfTwo(positions_by_key.size() * 2);
	mask = slot_count - 1;
	slots.resize(slot_count, Slot {string_t(), list_entry_t(0, 0)});
	for (auto &entry : positions_by_key) {
		auto slot_idx = Hash(entry.first) & mask;
		while (slots[slot_idx].positions.length != 0) {
			slot_idx = (slot_idx + 1) & mask;
		}
		slots[slot_idx] = Slot {entry.first, list_entry_t(positions.size(), entry.second.size())};
		positions.insert(positions.end(), entry.second.begin(), entry.second.end());
	}
}

VariantPathBindData::VariantPathBindData() : FunctionData() {
	IndexPaths();
}
VariantPathBindData::VariantPathBindData(const string &input_path) : FunctionData() {
	if (input_path.empty()) {
		paths.emplace_back();
	} else {
		paths.push_back({VariantPathComponent(input_path)});
	}
	IndexPaths();
}
VariantPathBindData::VariantPathBindData(const vector<string> &input_paths) : FunctionData() {
	for (const auto &path : input_paths) {
		if (path.empty()) {
			paths.emplace_back();
		} else {
			paths.push_back({VariantPathComponent(path)});
		}
	}
	IndexPaths();
}
VariantPathBindData::VariantPathBindData(int64_t index) : FunctionData() {
	paths.push_back({VariantPathComponent(index)});
	IndexPaths();
}
VariantPathBindData::VariantPathBindData(const VariantPathBindData &other) : FunctionData(other), paths(other.paths) {
	//! the keys point into the paths, so they are taken from the copied paths
	IndexPaths();
}

void VariantPathBindData::IndexPaths() {
	single_keys.resize(paths.size());
	idx_t single_key_count = 0;
	for (idx_t path_idx = 0; path_idx < paths.size(); path_idx++) {
		const auto &path = paths[path_idx];
		if (path.size() == 1 && path[0].lookup_mode == VariantChildLookupMode::BY_KEY) {
			single_keys[path_idx] = string_t(path[0].key.c_str(), NumericCast<uint32_t>(path[0].key.size()));
			single_key_count++;
		}
	}
	if (single_key_count >= KEY_INDEX_THRESHOLD) {
		key_index = make_uniq<VariantPathKeyIndex>(single_keys);
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

static vector<string> CollectPaths(const Value &constant_arg, const string &function_name) {
	vector<string> paths;
	const auto &children = ListValue::GetChildren(constant_arg);
	for (const auto &child : children) {
		if (child.IsNull()) {
			throw BinderException("'%s' does not accept NULL paths", function_name);
		}
		paths.push_back(child.GetValue<string>());
	}

	return paths;
}

unique_ptr<FunctionData> VariantBindUtils::VariantPathBind(BindScalarFunctionInput &input) {
	auto &arguments = input.GetArguments();
	const auto &function_name = input.GetBoundFunction().GetName();

	if (arguments.size() != 1 && arguments.size() != 2) {
		throw BinderException("%s expects either one VARIANT column argument, or two VARIANT column and "
		                      "VARCHAR path arguments",
		                      function_name);
	}

	if (arguments.size() == 1) {
		// No path supplied, execute function over the root of the variant
		return make_uniq<VariantPathBindData>();
	}

	const auto &path_expr = *arguments[1];
	const auto &return_type = path_expr.GetReturnType();
	if (return_type.id() != LogicalTypeId::VARCHAR && return_type.id() != LogicalTypeId::LIST &&
	    return_type.id() != LogicalTypeId::BIGINT) {
		throw BinderException("%s expects the second argument to be of type VARCHAR, VARCHAR[] or BIGINT, not %s",
		                      function_name, return_type.ToString());
	}
	if (return_type.id() == LogicalTypeId::LIST) {
		const auto child_type_id = ListType::GetChildType(return_type).id();
		if (child_type_id != LogicalTypeId::VARCHAR && child_type_id != LogicalTypeId::SQLNULL) {
			throw BinderException("%s expects the second argument to be of type VARCHAR, VARCHAR[] or BIGINT, not %s",
			                      function_name, return_type.ToString());
		}
	}

	auto constant_arg = input.GetNonNullConstant(1);

	const auto path_type_id = constant_arg.type().id();
	if (path_type_id == LogicalTypeId::VARCHAR) {
		return make_uniq<VariantPathBindData>(constant_arg.GetValue<string>());
	}
	if (path_type_id == LogicalTypeId::LIST) {
		return make_uniq<VariantPathBindData>(CollectPaths(constant_arg, function_name.GetIdentifierName()));
	}
	if (path_type_id == LogicalTypeId::BIGINT) {
		return make_uniq<VariantPathBindData>(constant_arg.GetValue<int64_t>());
	}

	throw BinderException("%s received an unexpected type for the second argument", function_name);
}

} // namespace duckdb
