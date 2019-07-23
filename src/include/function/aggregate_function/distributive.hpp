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

//! The type used for aggregate functions
typedef void (*aggregate_function_t)(Vector inputs[], index_t input_count, Vector &result);
typedef void (*aggregate_simple_function_t)(Vector inputs[], index_t input_count, Value &result);
typedef void (*aggregate_finalize_t)(Vector& payloads, Vector &result);

static SQLType get_same_return_type(vector<SQLType> &arguments) {
	assert(arguments.size() == 1);
	return arguments[0];
}

static SQLType get_bigint_return_type(vector<SQLType> &arguments) {
	return SQLTypeId::BIGINT;
}

void gather_finalize(Vector& payloads, Vector &result);

void count_function( Vector inputs[], index_t input_count, Vector &result );
void count_simple_function( Vector inputs[], index_t input_count, Value &result );

class CountFunction {
public:
	static const char*GetName() {
		return "count";
	}

	static aggregate_function_t GetFunction() {
		return count_function;
	}

	static aggregate_simple_function_t GetSimpleFunction() {
		return count_simple_function;
	}

	static aggregate_finalize_t GetFinalizeFunction() {
		return gather_finalize;
	}

	static get_return_type_function_t GetReturnTypeFunction() {
		return get_bigint_return_type;
	}
};

void countstar_function( Vector inputs[], index_t input_count, Vector &result );
void countstar_simple_function( Vector inputs[], index_t input_count, Value &result );

class CountStarFunction {
public:
	static const char*GetName() {
		return "countstar";
	}

	static aggregate_function_t GetFunction() {
		return countstar_function;
	}

	static aggregate_simple_function_t GetSimpleFunction() {
		return countstar_simple_function;
	}

	static aggregate_finalize_t GetFinalizeFunction() {
		return gather_finalize;
	}

	static get_return_type_function_t GetReturnTypeFunction() {
		return get_bigint_return_type;
	}
};

void first_function( Vector inputs[], index_t input_count, Vector &result );

class FirstFunction {
public:
	static const char*GetName() {
		return "first";
	}

	static aggregate_function_t GetFunction() {
		return first_function;
	}

	static aggregate_simple_function_t GetSimpleFunction() {
		return nullptr;
	}

	static aggregate_finalize_t GetFinalizeFunction() {
		return gather_finalize;
	}

	static get_return_type_function_t GetReturnTypeFunction() {
		return get_same_return_type;
	}
};

void max_function( Vector inputs[], index_t input_count, Vector &result );
void max_simple_function( Vector inputs[], index_t input_count, Value &result );

class MaxFunction {
public:
	static const char*GetName() {
		return "max";
	}

	static aggregate_function_t GetFunction() {
		return max_function;
	}

	static aggregate_simple_function_t GetSimpleFunction() {
		return max_simple_function;
	}

	static aggregate_finalize_t GetFinalizeFunction() {
		return gather_finalize;
	}

	static get_return_type_function_t GetReturnTypeFunction() {
		return get_same_return_type;
	}
};

void min_function( Vector inputs[], index_t input_count, Vector &result );
void min_simple_function( Vector inputs[], index_t input_count, Value &result );

class MinFunction {
public:
	static const char*GetName() {
		return "min";
	}

	static aggregate_function_t GetFunction() {
		return min_function;
	}

	static aggregate_simple_function_t GetSimpleFunction() {
		return min_simple_function;
	}

	static aggregate_finalize_t GetFinalizeFunction() {
		return gather_finalize;
	}

	static get_return_type_function_t GetReturnTypeFunction() {
		return get_same_return_type;
	}
};

void stddevsamp_function(Vector inputs[], index_t input_count, Vector &result);
void stddevsamp_finalize(Vector& payloads, Vector &result);
SQLType stddev_get_return_type(vector<SQLType> &arguments);

class StdDevSampFunction {
public:
	static const char*GetName() {
		return "stddev_samp";
	}

	static aggregate_function_t GetFunction() {
		return stddevsamp_function;
	}

	static aggregate_simple_function_t GetSimpleFunction() {
		return nullptr;
	}

	static aggregate_finalize_t GetFinalizeFunction() {
		return stddevsamp_finalize;
	}

	static get_return_type_function_t GetReturnTypeFunction() {
		return stddev_get_return_type;
	}
};

void sum_function(Vector inputs[], index_t input_count, Vector &result);
void sum_simple_function(Vector inputs[], index_t input_count, Value &result);
SQLType sum_get_return_type(vector<SQLType> &arguments);

class SumFunction {
public:
	static const char*GetName() {
		return "sum";
	}

	static aggregate_function_t GetFunction() {
		return sum_function;
	}

	static aggregate_simple_function_t GetSimpleFunction() {
		return sum_simple_function;
	}

	static aggregate_finalize_t GetFinalizeFunction() {
		return gather_finalize;
	}

	static get_return_type_function_t GetReturnTypeFunction() {
		return sum_get_return_type;
	}
};

} // namespace duckdb
