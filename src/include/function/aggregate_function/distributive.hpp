//===----------------------------------------------------------------------===//
//                         DuckDB
//
// function/aggregate_function/distributive.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/types/data_chunk.hpp"
#include "function/function.hpp"

namespace duckdb {

void gather_finalize(Vector& payloads, Vector &result);

class AggregateInPlaceFunction {
public:
	static aggregate_finalize_t GetFinalizeFunction() {
		return gather_finalize;
	}
};

static index_t get_bigint_type_size(TypeId return_type) {
	return GetTypeIdSize(TypeId::BIGINT);
}

static void bigint_payload_initialize(data_ptr_t payload, TypeId return_type) {
	memset(payload, 0, get_bigint_type_size(return_type));
}

static Value bigint_simple_initialize() {
	return Value::BIGINT(0);
}

static SQLType get_bigint_return_type(vector<SQLType> &arguments) {
	if (arguments.size() > 1)
		return SQLTypeId::INVALID;
	return SQLTypeId::BIGINT;
}

static bool dont_cast_arguments(vector<SQLType> &arguments) {
	return false;
}

class AggregateBigintReturnFunction : public AggregateInPlaceFunction {
public:
	static aggregate_size_t GetStateSizeFunction() {
		return get_bigint_type_size;
	}

	static aggregate_initialize_t GetInitalizeFunction() {
		return bigint_payload_initialize;
	}

	static aggregate_simple_initialize_t GetSimpleInitializeFunction() {
		return bigint_simple_initialize;
	}

	static get_return_type_function_t GetReturnTypeFunction() {
		return get_bigint_return_type;
	}

	static matches_argument_function_t GetCastArgumentsFunction() {
		return dont_cast_arguments;
	}
};

static index_t get_return_type_size(TypeId return_type) {
	return GetTypeIdSize(return_type);
}

void null_payload_initialize(data_ptr_t payload, TypeId return_type);

static Value null_simple_initialize() {
	return Value();
}

static SQLType get_same_return_type(vector<SQLType> &arguments) {
	if (arguments.size() != 1)
		return SQLTypeId::INVALID;
	return arguments[0];
}

static bool cast_arguments(vector<SQLType> &arguments) {
	return true;
}

class AggregateSameReturnFunction : public AggregateInPlaceFunction {
public:
	static aggregate_size_t GetStateSizeFunction() {
		return get_return_type_size;
	}

	static aggregate_initialize_t GetInitalizeFunction() {
		return null_payload_initialize;
	}

	static aggregate_simple_initialize_t GetSimpleInitializeFunction() {
		return null_simple_initialize;
	}

	static get_return_type_function_t GetReturnTypeFunction() {
		return get_same_return_type;
	}

	static matches_argument_function_t GetCastArgumentsFunction() {
		return cast_arguments;
	}
};

} // namespace duckdb
