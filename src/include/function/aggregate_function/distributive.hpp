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
	return SQLTypeId::BIGINT;
}

static bool dont_cast_arguments(vector<SQLType> &arguments) {
	return false;
}

class AggregateBigintReturnFunction : public AggregateInPlaceFunction {
public:
	static aggregate_size_t GetPayloadSizeFunction() {
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
	assert(arguments.size() == 1);
	return arguments[0];
}

static bool cast_arguments(vector<SQLType> &arguments) {
	return true;
}

class AggregateSameReturnFunction : public AggregateInPlaceFunction {
public:
	static aggregate_size_t GetPayloadSizeFunction() {
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


void count_update( Vector inputs[], index_t input_count, Vector &result );
void count_simple_update( Vector inputs[], index_t input_count, Value &result );

class CountFunction : public AggregateBigintReturnFunction {
public:
	static const char*GetName() {
		return "count";
	}

	static aggregate_update_t GetUpdateFunction() {
		return count_update;
	}

	static aggregate_simple_update_t GetSimpleUpdateFunction() {
		return count_simple_update;
	}
};

void countstar_update( Vector inputs[], index_t input_count, Vector &result );
void countstar_simple_update( Vector inputs[], index_t input_count, Value &result );

class CountStarFunction : public AggregateBigintReturnFunction {
public:
	static const char*GetName() {
		return "count_star";
	}

	static aggregate_update_t GetUpdateFunction() {
		return countstar_update;
	}

	static aggregate_simple_update_t GetSimpleUpdateFunction() {
		return countstar_simple_update;
	}
};

void first_update( Vector inputs[], index_t input_count, Vector &result );

class FirstFunction : public AggregateSameReturnFunction {
public:
	static const char*GetName() {
		return "first";
	}

	static aggregate_update_t GetUpdateFunction() {
		return first_update;
	}

	static aggregate_simple_update_t GetSimpleUpdateFunction() {
		return nullptr;
	}
};

void max_update( Vector inputs[], index_t input_count, Vector &result );
void max_simple_update( Vector inputs[], index_t input_count, Value &result );

class MaxFunction : public AggregateSameReturnFunction{
public:
	static const char*GetName() {
		return "max";
	}

	static aggregate_update_t GetUpdateFunction() {
		return max_update;
	}

	static aggregate_simple_update_t GetSimpleUpdateFunction() {
		return max_simple_update;
	}
};

void min_update( Vector inputs[], index_t input_count, Vector &result );
void min_simple_update( Vector inputs[], index_t input_count, Value &result );

class MinFunction : public AggregateSameReturnFunction {
public:
	static const char*GetName() {
		return "min";
	}

	static aggregate_update_t GetUpdateFunction() {
		return min_update;
	}

	static aggregate_simple_update_t GetSimpleUpdateFunction() {
		return min_simple_update;
	}
};

void stddevsamp_update(Vector inputs[], index_t input_count, Vector &result);
void stddevsamp_finalize(Vector& payloads, Vector &result);
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
	static const char*GetName() {
		return "aggregate_stddev_samp";
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

void sum_update(Vector inputs[], index_t input_count, Vector &result);
void sum_simple_update(Vector inputs[], index_t input_count, Value &result);
SQLType sum_get_return_type(vector<SQLType> &arguments);

class SumFunction : public AggregateInPlaceFunction {
public:
	static const char*GetName() {
		return "sum";
	}

	static aggregate_size_t GetPayloadSizeFunction() {
		return get_return_type_size;
	}

	static aggregate_initialize_t GetInitalizeFunction() {
		return null_payload_initialize;
	}

	static aggregate_update_t GetUpdateFunction() {
		return sum_update;
	}

	static aggregate_simple_initialize_t GetSimpleInitializeFunction() {
		return null_simple_initialize;
	}

	static aggregate_simple_update_t GetSimpleUpdateFunction() {
		return sum_simple_update;
	}

	static get_return_type_function_t GetReturnTypeFunction() {
		return sum_get_return_type;
	}

	static matches_argument_function_t GetCastArgumentsFunction() {
		return cast_arguments;
	}
};

} // namespace duckdb
