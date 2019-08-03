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
SQLType stddev_get_return_type(vector<SQLType> &arguments);

static index_t stddev_payload_size(TypeId return_type) {
	// count running_mean running_dsquared
	return sizeof(uint64_t) + sizeof(double) + sizeof(double);
}

static void stddevsamp_initialize(data_ptr_t payload, TypeId return_type) {
	memset(payload, 0, stddev_payload_size(return_type));
}

class StdDevSampFunction {
public:
	static const char *GetName() {
		return "stddev_samp";
	}

	static aggregate_size_t GetPayloadSizeFunction() {
		return stddev_payload_size;
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

} // namespace duckdb
