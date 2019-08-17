//===----------------------------------------------------------------------===//
//                         DuckDB
//
// function/aggregate_function.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "function/function.hpp"

namespace duckdb {

//! The type used for sizing hashed aggregate function states
typedef index_t (*aggregate_size_t)(TypeId return_type);
//! The type used for initializing hashed aggregate function states
typedef void (*aggregate_initialize_t)(data_ptr_t payload, TypeId return_type);
//! The type used for updating hashed aggregate functions
typedef void (*aggregate_update_t)(Vector inputs[], index_t input_count, Vector &result);
//! The type used for finalizing hashed aggregate function payloads
typedef void (*aggregate_finalize_t)(Vector &payloads, Vector &result);

//! The type used for initializing simple aggregate function
typedef Value (*aggregate_simple_initialize_t)();
//! The type used for updating simple aggregate functions
typedef void (*aggregate_simple_update_t)(Vector inputs[], index_t input_count, Value &result);

//! Gets the return type of the function given the types of the input argument
typedef SQLType (*get_return_type_function_t)(vector<SQLType> &arguments);

class AggregateFunction : public Function {
public:
	AggregateFunction(string name, get_return_type_function_t return_type, aggregate_size_t state_size, aggregate_initialize_t initialize, aggregate_update_t update, aggregate_finalize_t finalize, aggregate_simple_initialize_t simple_initialize = nullptr, aggregate_simple_update_t simple_update = nullptr, bool cast_arguments = true) :
		Function(name), return_type(return_type), state_size(state_size), initialize(initialize), update(update), finalize(finalize), simple_initialize(simple_initialize), simple_update(simple_update), cast_arguments(cast_arguments) {}

	get_return_type_function_t return_type;

	//! The hashed aggregate state sizing function
	aggregate_size_t state_size;
	//! The hashed aggregate initialization function
	aggregate_initialize_t initialize;
	//! The hashed aggregate update function
	aggregate_update_t update;
	//! The hashed aggregate finalization function
	aggregate_finalize_t finalize;

	//! The simple aggregate initialization function (may be null)
	aggregate_simple_initialize_t simple_initialize;
	//! The simple aggregate update function (may be null)
	aggregate_simple_update_t simple_update;
	//! Whether or not to cast the arguments
	bool cast_arguments;

	bool operator==(const AggregateFunction &rhs) const {
		return state_size == rhs.state_size && initialize == rhs.initialize && update == rhs.update && finalize == rhs.finalize;
	}
	bool operator!=(const AggregateFunction &rhs) const {
		return !(*this == rhs);
	}
};

} // namespace duckdb
