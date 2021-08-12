#include "parameter_wrapper.hpp"

using duckdb::ParameterWrapper;
using duckdb::idx_t;

ParameterWrapper::~ParameterWrapper() {
}

void ParameterWrapper::GetValues(std::vector<Value> &values, idx_t paramset_idx) {
	values.clear();
	for (idx_t desc_idx = 0; desc_idx < param_descriptors.size(); ++desc_idx) {
		values.emplace_back(param_descriptors[desc_idx].values[paramset_idx]);
	}
}

void ParameterWrapper::SetValue(Value &value, idx_t param_idx) {
	if (paramset_size == 1) {
		if (param_descriptors[param_idx].values.empty()) {
			param_descriptors[param_idx].values.emplace_back(value);
		} else {
			// replacing value, i.e., reusing the prepared statement
			param_descriptors[param_idx].values[0] = value;
		}
	} else if (param_descriptors[param_idx].values.size() < paramset_size) {
		param_descriptors[param_idx].values.emplace_back(value);
	}
	// maybe throw an exception here
}
