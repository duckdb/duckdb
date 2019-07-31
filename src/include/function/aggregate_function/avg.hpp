//===----------------------------------------------------------------------===//
//                         DuckDB
//
// function/aggregate_function/avg.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "function/aggregate_function/distributive.hpp"

namespace duckdb {

static index_t avg_payload_size(TypeId return_type) {
	// count running_sum
	return sizeof(uint64_t) + sizeof(double);
}

static void avg_initialize(data_ptr_t payload, TypeId return_type) {
	memset(payload, 0, avg_payload_size(return_type));
}

void avg_update(Vector inputs[], index_t input_count, Vector &result);
void avg_finalize(Vector& payloads, Vector &result);
SQLType avg_get_return_type(vector<SQLType> &arguments);

static Value avg_simple_initialize() {
	return Value(TypeId::DOUBLE);
}

class AvgFunction {
public:
	static const char*GetName() {
		return "avg";
	}

	static aggregate_size_t GetPayloadSizeFunction() {
		return avg_payload_size;
	}

	static aggregate_initialize_t GetInitalizeFunction() {
		return avg_initialize;
	}

	static aggregate_update_t GetUpdateFunction() {
		return avg_update;
	}

	static aggregate_finalize_t GetFinalizeFunction() {
		return avg_finalize;
	}

	static aggregate_simple_initialize_t GetSimpleInitializeFunction() {
		return avg_simple_initialize;
	}

	static aggregate_simple_update_t GetSimpleUpdateFunction() {
		return nullptr;
	}

	static get_return_type_function_t GetReturnTypeFunction() {
		return avg_get_return_type;
	}

	static matches_argument_function_t GetCastArgumentsFunction() {
		return cast_arguments;
	}
};


} // namespace duckdb
