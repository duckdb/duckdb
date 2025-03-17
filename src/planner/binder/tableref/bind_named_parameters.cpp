#include "duckdb/planner/binder.hpp"

namespace duckdb {

template <typename T>
map<string, T> order_case_insensitive_map(const case_insensitive_map_t<T> &input_map) {
	return map<string, T>(input_map.begin(), input_map.end());
}

void Binder::BindNamedParameters(named_parameter_type_map_t &types, named_parameter_map_t &values,
                                 QueryErrorContext &error_context, string &func_name) {
	for (auto &kv : values) {
		auto entry = types.find(kv.first);
		if (entry == types.end()) {
			auto ordered_params = order_case_insensitive_map(types);
			// create a list of named parameters for the error
			string named_params;
			for (auto &kv_ordered_params : ordered_params) {
				named_params += "    ";
				named_params += kv_ordered_params.first;
				named_params += " ";
				named_params += kv_ordered_params.second.ToString();
				named_params += "\n";
			}
			string error_msg;
			if (named_params.empty()) {
				error_msg = "Function does not accept any named parameters.";
			} else {
				error_msg = "Candidates:\n" + named_params;
			}
			throw BinderException(error_context, "Invalid named parameter \"%s\" for function %s\n%s", kv.first,
			                      func_name, error_msg);
		}
		if (entry->second.id() != LogicalTypeId::ANY) {
			kv.second = kv.second.DefaultCastAs(entry->second);
		}
	}
}

} // namespace duckdb
