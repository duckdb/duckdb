//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/function/aggregate_function.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/function/function.hpp"

namespace duckdb {

//! The type used for sizing hashed aggregate function states
typedef index_t (*aggregate_size_t)(TypeId return_type);
//! The type used for initializing hashed aggregate function states
typedef void (*aggregate_initialize_t)(data_ptr_t state, TypeId return_type);
//! The type used for updating hashed aggregate functions
typedef void (*aggregate_update_t)(Vector inputs[], index_t input_count, Vector &state);
//! The type used for combining hashed aggregate states (optional)
typedef void (*aggregate_combine_t)(Vector &state, Vector &combined);
//! The type used for finalizing hashed aggregate function payloads
typedef void (*aggregate_finalize_t)(Vector &state, Vector &result);
//! The type used for the aggregate destructor method. NOTE: this method is used in destructors and MAY NOT throw.
typedef void (*aggregate_destructor_t)(Vector &state);

//! The type used for updating simple (non-grouped) aggregate functions
typedef void (*aggregate_simple_update_t)(Vector inputs[], index_t input_count, data_ptr_t state);

class AggregateFunction : public SimpleFunction {
public:
	AggregateFunction(string name, vector<SQLType> arguments, SQLType return_type, aggregate_size_t state_size,
	                  aggregate_initialize_t initialize, aggregate_update_t update, aggregate_combine_t combine,
	                  aggregate_finalize_t finalize, aggregate_simple_update_t simple_update = nullptr,
	                  aggregate_destructor_t destructor = nullptr)
	    : SimpleFunction(name, arguments, return_type, false), state_size(state_size), initialize(initialize),
	      update(update), combine(combine), finalize(finalize), simple_update(simple_update), destructor(destructor) {
	}

	AggregateFunction(vector<SQLType> arguments, SQLType return_type, aggregate_size_t state_size,
	                  aggregate_initialize_t initialize, aggregate_update_t update, aggregate_combine_t combine,
	                  aggregate_finalize_t finalize, aggregate_simple_update_t simple_update = nullptr,
	                  aggregate_destructor_t destructor = nullptr)
	    : AggregateFunction(string(), arguments, return_type, state_size, initialize, update, combine, finalize,
	                        simple_update, destructor) {
	}

	//! The hashed aggregate state sizing function
	aggregate_size_t state_size;
	//! The hashed aggregate state initialization function
	aggregate_initialize_t initialize;
	//! The hashed aggregate update state function
	aggregate_update_t update;
	//! The hashed aggregate combine states function
	aggregate_combine_t combine;
	//! The hashed aggregate finalization function
	aggregate_finalize_t finalize;
	//! The simple aggregate update function (may be null)
	aggregate_simple_update_t simple_update;
	//! The destructor method (may be null)
	aggregate_destructor_t destructor;

	bool operator==(const AggregateFunction &rhs) const {
		return state_size == rhs.state_size && initialize == rhs.initialize && update == rhs.update &&
		       combine == rhs.combine && finalize == rhs.finalize;
	}
	bool operator!=(const AggregateFunction &rhs) const {
		return !(*this == rhs);
	}
};

} // namespace duckdb
