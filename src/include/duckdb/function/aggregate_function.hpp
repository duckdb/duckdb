//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/function/aggregate_function.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/function/function.hpp"
#include "duckdb/common/vector_operations/aggregate_executor.hpp"

namespace duckdb {

//! The type used for sizing hashed aggregate function states
typedef idx_t (*aggregate_size_t)();
//! The type used for initializing hashed aggregate function states
typedef void (*aggregate_initialize_t)(data_ptr_t state);
//! The type used for updating hashed aggregate functions
typedef void (*aggregate_update_t)(Vector inputs[], idx_t input_count, Vector &state);
//! The type used for combining hashed aggregate states (optional)
typedef void (*aggregate_combine_t)(Vector &state, Vector &combined);
//! The type used for finalizing hashed aggregate function payloads
typedef void (*aggregate_finalize_t)(Vector &state, Vector &result);
//! The type used for the aggregate destructor method. NOTE: this method is used in destructors and MAY NOT throw.
typedef void (*aggregate_destructor_t)(Vector &state);

//! The type used for updating simple (non-grouped) aggregate functions
typedef void (*aggregate_simple_update_t)(Vector inputs[], idx_t input_count, data_ptr_t state);

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

public:
	template <class STATE, class INPUT_TYPE, class RESULT_TYPE, class OP>
	static AggregateFunction UnaryAggregate(SQLType input_type, SQLType return_type) {
		return AggregateFunction(
		    {input_type}, return_type, AggregateFunction::StateSize<STATE>,
		    AggregateFunction::StateInitialize<STATE, OP>, AggregateFunction::UnaryScatterUpdate<STATE, INPUT_TYPE, OP>,
		    AggregateFunction::StateCombine<STATE, OP>, AggregateFunction::StateFinalize<STATE, RESULT_TYPE, OP>,
		    AggregateFunction::UnaryUpdate<STATE, INPUT_TYPE, OP>);
	};
	template <class STATE, class INPUT_TYPE, class RESULT_TYPE, class OP>
	static AggregateFunction UnaryAggregateDestructor(SQLType input_type, SQLType return_type) {
		auto aggregate = UnaryAggregate<STATE, INPUT_TYPE, RESULT_TYPE, OP>(input_type, return_type);
		aggregate.destructor = AggregateFunction::StateDestroy<STATE, OP>;
		return aggregate;
	};
	template <class STATE, class A_TYPE, class B_TYPE, class RESULT_TYPE, class OP>
	static AggregateFunction BinaryAggregate(SQLType a_type, SQLType b_type, SQLType return_type) {
		return AggregateFunction({a_type, b_type}, return_type, AggregateFunction::StateSize<STATE>,
		                         AggregateFunction::StateInitialize<STATE, OP>,
		                         AggregateFunction::BinaryScatterUpdate<STATE, A_TYPE, B_TYPE, OP>,
		                         AggregateFunction::StateCombine<STATE, OP>,
		                         AggregateFunction::StateFinalize<STATE, RESULT_TYPE, OP>,
		                         AggregateFunction::BinaryUpdate<STATE, A_TYPE, B_TYPE, OP>);
	};
	template <class STATE, class A_TYPE, class B_TYPE, class RESULT_TYPE, class OP>
	static AggregateFunction BinaryAggregateDestructor(SQLType a_type, SQLType b_type, SQLType return_type) {
		auto aggregate = BinaryAggregate<STATE, A_TYPE, B_TYPE, RESULT_TYPE, OP>(a_type, b_type, return_type);
		aggregate.destructor = AggregateFunction::StateDestroy<STATE, OP>;
		return aggregate;
	};

public:
	template <class OP> static AggregateFunction GetNumericUnaryAggregate(SQLType type) {
		switch (type.id) {
		case SQLTypeId::TINYINT:
			return UnaryAggregate<int8_t, int8_t, int8_t, OP>(type, type);
		case SQLTypeId::SMALLINT:
			return UnaryAggregate<int16_t, int16_t, int16_t, OP>(type, type);
		case SQLTypeId::INTEGER:
			return UnaryAggregate<int32_t, int32_t, int32_t, OP>(type, type);
		case SQLTypeId::BIGINT:
			return UnaryAggregate<int64_t, int64_t, int64_t, OP>(type, type);
		case SQLTypeId::FLOAT:
			return UnaryAggregate<float, float, float, OP>(type, type);
		case SQLTypeId::DOUBLE:
			return UnaryAggregate<double, double, double, OP>(type, type);
		case SQLTypeId::DECIMAL:
			return UnaryAggregate<double, double, double, OP>(type, type);
		default:
			throw NotImplementedException("Unimplemented numeric type");
		}
	}

	template <class OP> static AggregateFunction GetUnaryAggregate(SQLType type) {
		switch (type.id) {
		case SQLTypeId::BOOLEAN:
			return UnaryAggregate<int8_t, int8_t, int8_t, OP>(type, type);
		case SQLTypeId::TINYINT:
		case SQLTypeId::SMALLINT:
		case SQLTypeId::INTEGER:
		case SQLTypeId::BIGINT:
		case SQLTypeId::FLOAT:
		case SQLTypeId::DOUBLE:
		case SQLTypeId::DECIMAL:
			return GetNumericUnaryAggregate<OP>(type);
		case SQLTypeId::DATE:
			return UnaryAggregate<date_t, date_t, date_t, OP>(type, type);
		case SQLTypeId::TIMESTAMP:
			return UnaryAggregate<timestamp_t, timestamp_t, timestamp_t, OP>(type, type);
		case SQLTypeId::VARCHAR:
			return UnaryAggregate<string_t, string_t, string_t, OP>(type, type);
		default:
			throw NotImplementedException("Unimplemented type for unary aggregate");
		}
	}

public:
	template <class STATE> static idx_t StateSize() {
		return sizeof(STATE);
	}

	template <class STATE, class OP> static void StateInitialize(data_ptr_t state) {
		OP::Initialize((STATE *)state);
	}

	template <class STATE, class T, class OP>
	static void UnaryScatterUpdate(Vector inputs[], idx_t input_count, Vector &states) {
		assert(input_count == 1);
		AggregateExecutor::UnaryScatter<STATE, T, OP>(inputs[0], states);
	}

	template <class STATE, class INPUT_TYPE, class OP>
	static void UnaryUpdate(Vector inputs[], idx_t input_count, data_ptr_t state) {
		assert(input_count == 1);
		AggregateExecutor::UnaryUpdate<STATE, INPUT_TYPE, OP>(inputs[0], state);
	}

	template <class STATE, class A_TYPE, class B_TYPE, class OP>
	static void BinaryScatterUpdate(Vector inputs[], idx_t input_count, Vector &states) {
		assert(input_count == 2);
		AggregateExecutor::BinaryScatter<STATE, A_TYPE, B_TYPE, OP>(inputs[0], inputs[1], states);
	}

	template <class STATE, class A_TYPE, class B_TYPE, class OP>
	static void BinaryUpdate(Vector inputs[], idx_t input_count, data_ptr_t state) {
		assert(input_count == 2);
		AggregateExecutor::BinaryUpdate<STATE, A_TYPE, B_TYPE, OP>(inputs[0], inputs[1], state);
	}

	template <class STATE, class OP> static void StateCombine(Vector &source, Vector &target) {
		AggregateExecutor::Combine<STATE, OP>(source, target);
	}

	template <class STATE, class RESULT_TYPE, class OP> static void StateFinalize(Vector &states, Vector &result) {
		AggregateExecutor::Finalize<STATE, RESULT_TYPE, OP>(states, result);
	}

	template <class STATE, class OP> static void StateDestroy(Vector &states) {
		AggregateExecutor::Destroy<STATE, OP>(states);
	}
};

} // namespace duckdb
