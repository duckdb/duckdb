//===----------------------------------------------------------------------===//
//                         DuckDB
//
// function/aggregate_function/covar.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "function/aggregate_function/distributive.hpp"

namespace duckdb {

void covar_update(Vector inputs[], index_t input_count, Vector &result);
void covarpop_finalize(Vector &payloads, Vector &result);
void covarsamp_finalize(Vector &payloads, Vector &result);
SQLType covar_get_return_type(vector<SQLType> &arguments);

static index_t covar_state_size(TypeId return_type) {
	// count meanx meany co-moment
	return sizeof(uint64_t) + 3 * sizeof(double);
}

static void covar_initialize(data_ptr_t payload, TypeId return_type) {
	memset(payload, 0, covar_state_size(return_type));
}

class CovarSampFunction {
public:
	static const char *GetName() {
		return "covar_samp";
	}

	static aggregate_size_t GetStateSizeFunction() {
		return covar_state_size;
	}

	static aggregate_initialize_t GetInitalizeFunction() {
		return covar_initialize;
	}

	static aggregate_update_t GetUpdateFunction() {
		return covar_update;
	}

	static aggregate_finalize_t GetFinalizeFunction() {
		return covarsamp_finalize;
	}

	static aggregate_simple_initialize_t GetSimpleInitializeFunction() {
		return nullptr;
	}

	static aggregate_simple_update_t GetSimpleUpdateFunction() {
		return nullptr;
	}

	static get_return_type_function_t GetReturnTypeFunction() {
		return covar_get_return_type;
	}

	static matches_argument_function_t GetCastArgumentsFunction() {
		return cast_arguments;
	}
};

class CovarPopFunction {
public:
	static const char *GetName() {
		return "covar_pop";
	}

	static aggregate_size_t GetStateSizeFunction() {
		return covar_state_size;
	}

	static aggregate_initialize_t GetInitalizeFunction() {
		return covar_initialize;
	}

	static aggregate_update_t GetUpdateFunction() {
		return covar_update;
	}

	static aggregate_finalize_t GetFinalizeFunction() {
		return covarpop_finalize;
	}

	static aggregate_simple_initialize_t GetSimpleInitializeFunction() {
		return nullptr;
	}

	static aggregate_simple_update_t GetSimpleUpdateFunction() {
		return nullptr;
	}

	static get_return_type_function_t GetReturnTypeFunction() {
		return covar_get_return_type;
	}

	static matches_argument_function_t GetCastArgumentsFunction() {
		return cast_arguments;
	}
};

} // namespace duckdb
