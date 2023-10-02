#include "duckdb/planner/binder.hpp"

namespace duckdb {

void Binder::BindNamedParameters(named_parameter_type_map_t &types, named_parameter_map_t &values,
                                 QueryErrorContext &error_context, string &func_name) {
	for (auto &kv : values) {
		auto entry = types.find(kv.first);
		if (entry == types.end()) {
			// create a list of named parameters for the error
			string named_params;
			for (auto &kv : types) {
				named_params += "    ";
				named_params += kv.first;
				named_params += " ";
				named_params += kv.second.ToString();
				named_params += "\n";
			}
			string error_msg;
			if (named_params.empty()) {
				error_msg = "Function does not accept any named parameters.";
			} else {
				error_msg = "Candidates:\n" + named_params;
			}
			throw BinderException(error_context.FormatError("Invalid named parameter \"%s\" for function %s\n%s",
			                                                kv.first, func_name, error_msg));
		}
		if (entry->second.id() != LogicalTypeId::ANY) {
			kv.second = kv.second.DefaultCastAs(entry->second);
		}
	}
}

} // namespace duckdb
