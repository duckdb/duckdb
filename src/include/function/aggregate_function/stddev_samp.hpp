//===----------------------------------------------------------------------===//
//                         DuckDB
//
// function/aggregate_function/stddev_samp.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "function/aggregate_function/distributive.hpp"

namespace duckdb {

void stddevsamp_update(Vector inputs[], index_t input_count, Vector &result);
void stddevsamp_finalize(Vector &payloads, Vector &result);
void stddevpop_finalize(Vector &payloads, Vector &result);
void varsamp_finalize(Vector &payloads, Vector &result);
void varpop_finalize(Vector &payloads, Vector &result);

SQLType stddev_get_return_type(vector<SQLType> &arguments);

struct stddev_state_t {
    uint64_t    count;
    double      mean;
    double      dsquared;
};

static index_t stddev_state_size(TypeId return_type) {
	return sizeof(stddev_state_t);
}

static void stddevsamp_initialize(data_ptr_t payload, TypeId return_type) {
	memset(payload, 0, stddev_state_size(return_type));
}

class StdDevSampFunction {
public:
	static const char *GetName() {
		return "stddev_samp";
	}

	static aggregate_size_t GetStateSizeFunction() {
		return stddev_state_size;
	}

	static aggregate_initialize_t GetInitalizeFunction() {
		return stddevsamp_initialize;
	}

	static aggregate_update_t GetUpdateFunction() {
		return stddevsamp_update;
	}

	static aggregate_finalize_t GetFinalizeFunction() {
		return stddevsamp_finalize;
	}

	static aggregate_simple_initialize_t GetSimpleInitializeFunction() {
		return nullptr;
	}

	static aggregate_simple_update_t GetSimpleUpdateFunction() {
		return nullptr;
	}

	static get_return_type_function_t GetReturnTypeFunction() {
		return stddev_get_return_type;
	}

	static matches_argument_function_t GetCastArgumentsFunction() {
		return cast_arguments;
	}
};

class StdDevPopFunction {
public:
	static const char *GetName() {
		return "stddev_pop";
	}

	static aggregate_size_t GetStateSizeFunction() {
		return stddev_state_size;
	}

	static aggregate_initialize_t GetInitalizeFunction() {
		return stddevsamp_initialize;
	}

	static aggregate_update_t GetUpdateFunction() {
		return stddevsamp_update;
	}

	static aggregate_finalize_t GetFinalizeFunction() {
		return stddevpop_finalize;
	}

	static aggregate_simple_initialize_t GetSimpleInitializeFunction() {
		return nullptr;
	}

	static aggregate_simple_update_t GetSimpleUpdateFunction() {
		return nullptr;
	}

	static get_return_type_function_t GetReturnTypeFunction() {
		return stddev_get_return_type;
	}

	static matches_argument_function_t GetCastArgumentsFunction() {
		return cast_arguments;
	}
};
class VarSampFunction {
public:
	static const char *GetName() {
		return "var_samp";
	}

	static aggregate_size_t GetStateSizeFunction() {
		return stddev_state_size;
	}

	static aggregate_initialize_t GetInitalizeFunction() {
		return stddevsamp_initialize;
	}

	static aggregate_update_t GetUpdateFunction() {
		return stddevsamp_update;
	}

	static aggregate_finalize_t GetFinalizeFunction() {
		return varsamp_finalize;
	}

	static aggregate_simple_initialize_t GetSimpleInitializeFunction() {
		return nullptr;
	}

	static aggregate_simple_update_t GetSimpleUpdateFunction() {
		return nullptr;
	}

	static get_return_type_function_t GetReturnTypeFunction() {
		return stddev_get_return_type;
	}

	static matches_argument_function_t GetCastArgumentsFunction() {
		return cast_arguments;
	}
};

class VarPopFunction {
public:
	static const char *GetName() {
		return "var_pop";
	}

	static aggregate_size_t GetStateSizeFunction() {
		return stddev_state_size;
	}

	static aggregate_initialize_t GetInitalizeFunction() {
		return stddevsamp_initialize;
	}

	static aggregate_update_t GetUpdateFunction() {
		return stddevsamp_update;
	}

	static aggregate_finalize_t GetFinalizeFunction() {
		return varpop_finalize;
	}

	static aggregate_simple_initialize_t GetSimpleInitializeFunction() {
		return nullptr;
	}

	static aggregate_simple_update_t GetSimpleUpdateFunction() {
		return nullptr;
	}

	static get_return_type_function_t GetReturnTypeFunction() {
		return stddev_get_return_type;
	}

	static matches_argument_function_t GetCastArgumentsFunction() {
		return cast_arguments;
	}
};

} // namespace duckdb
