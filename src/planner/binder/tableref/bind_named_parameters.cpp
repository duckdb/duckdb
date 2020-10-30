#include "duckdb/planner/binder.hpp"

namespace duckdb {

void Binder::BindNamedParameters(unordered_map<string, LogicalType> types, unordered_map<string, Value> values,
                                 QueryErrorContext error_context, string func_name) {
	for (auto &kv : values) {
		auto entry = types.find(kv.first);
		if (entry == types.end()) {
			// create a list of named parameters for the error
			string named_params;
			for (auto &kv : types) {
				named_params += "    " + kv.first + " " + kv.second.ToString() + "\n";
			}
			if (named_params.empty()) {
				named_params = "Function does not accept any named parameters.";
			} else {
				named_params = "Candidates: " + named_params;
			}
			throw BinderException(error_context.FormatError("Invalid named parameter \"%s\" for function %s\n%s",
			                                                kv.first, func_name, named_params));
		}
		kv.second = kv.second.CastAs(entry->second);
	}
}

} // namespace duckdb
