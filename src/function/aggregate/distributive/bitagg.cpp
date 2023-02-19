#include "duckdb/function/aggregate/distributive_functions.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/types/null_value.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/common/vector_operations/aggregate_executor.hpp"
#include "duckdb/common/types/bit.hpp"
#include "duckdb/storage/statistics/numeric_statistics.hpp"

namespace duckdb {

template <class T>
struct BitState {
	bool is_set;
	T value;
};

template <class OP>
static AggregateFunction GetBitfieldUnaryAggregate(LogicalType type) {
	switch (type.id()) {
	case LogicalTypeId::TINYINT:
		return AggregateFunction::UnaryAggregate<BitState<uint8_t>, int8_t, int8_t, OP>(type, type);
	case LogicalTypeId::SMALLINT:
		return AggregateFunction::UnaryAggregate<BitState<uint16_t>, int16_t, int16_t, OP>(type, type);
	case LogicalTypeId::INTEGER:
		return AggregateFunction::UnaryAggregate<BitState<uint32_t>, int32_t, int32_t, OP>(type, type);
	case LogicalTypeId::BIGINT:
		return AggregateFunction::UnaryAggregate<BitState<uint64_t>, int64_t, int64_t, OP>(type, type);
	case LogicalTypeId::HUGEINT:
		return AggregateFunction::UnaryAggregate<BitState<hugeint_t>, hugeint_t, hugeint_t, OP>(type, type);
	case LogicalTypeId::UTINYINT:
		return AggregateFunction::UnaryAggregate<BitState<uint8_t>, uint8_t, uint8_t, OP>(type, type);
	case LogicalTypeId::USMALLINT:
		return AggregateFunction::UnaryAggregate<BitState<uint16_t>, uint16_t, uint16_t, OP>(type, type);
	case LogicalTypeId::UINTEGER:
		return AggregateFunction::UnaryAggregate<BitState<uint32_t>, uint32_t, uint32_t, OP>(type, type);
	case LogicalTypeId::UBIGINT:
		return AggregateFunction::UnaryAggregate<BitState<uint64_t>, uint64_t, uint64_t, OP>(type, type);
	default:
		throw InternalException("Unimplemented bitfield type for unary aggregate");
	}
}

struct BitwiseOperation {
	template <class STATE>
	static void Initialize(STATE *state) {
		//  If there are no matching rows, returns a null value.
		state->is_set = false;
	}

	template <class INPUT_TYPE, class STATE, class OP>
	static void Operation(STATE *state, AggregateInputData &, INPUT_TYPE *input, ValidityMask &mask, idx_t idx) {
		if (!state->is_set) {
			OP::template Assign(state, input[idx]);
			state->is_set = true;
		} else {
			OP::template Execute(state, input[idx]);
		}
	}

	template <class INPUT_TYPE, class STATE, class OP>
	static void ConstantOperation(STATE *state, AggregateInputData &aggr_input_data, INPUT_TYPE *input,
	                              ValidityMask &mask, idx_t count) {
		//  count is not relevant
		OP::template Operation<INPUT_TYPE, STATE, OP>(state, aggr_input_data, input, mask, 0);
	}

	template <class INPUT_TYPE, class STATE>
	static void Assign(STATE *state, INPUT_TYPE input) {
		state->value = input;
	}

	template <class STATE, class OP>
	static void Combine(const STATE &source, STATE *target, AggregateInputData &) {
		if (!source.is_set) {
			// source is NULL, nothing to do.
			return;
		}
		if (!target->is_set) {
			// target is NULL, use source value directly.
			OP::template Assign(target, source.value);
			target->is_set = true;
		} else {
			OP::template Execute(target, source.value);
		}
	}

	template <class T, class STATE>
	static void Finalize(Vector &result, AggregateInputData &, STATE *state, T *target, ValidityMask &mask, idx_t idx) {
		if (!state->is_set) {
			mask.SetInvalid(idx);
		} else {
			target[idx] = state->value;
		}
	}

	static bool IgnoreNull() {
		return true;
	}
};

struct BitAndOperation : public BitwiseOperation {
    template <class INPUT_TYPE, class STATE>
	static void Execute(STATE *state, INPUT_TYPE input) {
		state->value &= input;
	}
};

struct BitOrOperation : public BitwiseOperation{
    template <class INPUT_TYPE, class STATE>
	static void Execute(STATE *state, INPUT_TYPE input) {
		state->value |= input;
	}
};

struct BitXorOperation : public BitwiseOperation{
    template <class INPUT_TYPE, class STATE>
	static void Execute(STATE *state, INPUT_TYPE input) {
		state->value ^= input;
	}

    template <class INPUT_TYPE, class STATE, class OP>
    static void ConstantOperation(STATE *state, AggregateInputData &aggr_input_data, INPUT_TYPE *input,
                                  ValidityMask &mask, idx_t count) {
        for (idx_t i = 0; i < count; i++) {
            Operation<INPUT_TYPE, STATE, OP>(state, aggr_input_data, input, mask, 0);
        }
    }
};

struct BitStringBitwiseOperation : public BitwiseOperation {
	template <class STATE>
	static void Destroy(STATE *state) {
		if (state->is_set && !state->value.IsInlined()) {
			delete[] state->value.GetDataUnsafe();
		}
	}

	template <class INPUT_TYPE, class STATE>
	static void Assign(STATE *state, INPUT_TYPE input) {
		D_ASSERT(state->is_set == false);
		if (input.IsInlined()) {
			state->value = input;
		} else {
			// non-inlined string, need to allocate space for it
			auto len = input.GetSize();
			auto ptr = new char[len];
			memcpy(ptr, input.GetDataUnsafe(), len);

			state->value = string_t(ptr, len);
		}
	}

	template <class T, class STATE>
	static void Finalize(Vector &result, AggregateInputData &, STATE *state, T *target, ValidityMask &mask, idx_t idx) {
		if (!state->is_set) {
			mask.SetInvalid(idx);
		} else {
			target[idx] = StringVector::AddStringOrBlob(result, state->value);
		}
	}
};

struct BitStringAndOperation : public BitStringBitwiseOperation {

	template <class INPUT_TYPE, class STATE>
	static void Execute(STATE *state, INPUT_TYPE input) {
		Bit::BitwiseAnd(input, state->value, state->value);
	}
};

struct BitStringOrOperation : public BitStringBitwiseOperation{

	template <class INPUT_TYPE, class STATE>
	static void Execute(STATE *state, INPUT_TYPE input) {
		Bit::BitwiseOr(input, state->value, state->value);
	}
};

struct BitStringXorOperation : public BitStringBitwiseOperation{
	template <class INPUT_TYPE, class STATE>
	static void Execute(STATE *state, INPUT_TYPE input) {
		Bit::BitwiseXor(input, state->value, state->value);
	}

    template <class INPUT_TYPE, class STATE, class OP>
    static void ConstantOperation(STATE *state, AggregateInputData &aggr_input_data, INPUT_TYPE *input,
                                  ValidityMask &mask, idx_t count) {
        for (idx_t i = 0; i < count; i++) {
            Operation<INPUT_TYPE, STATE, OP>(state, aggr_input_data, input, mask, 0);
        }
    }
};

void BitAndFun::RegisterFunction(BuiltinFunctions &set) {
	AggregateFunctionSet bit_and("bit_and");
	for (auto &type : LogicalType::Integral()) {
		bit_and.AddFunction(GetBitfieldUnaryAggregate<BitAndOperation>(type));
	}

	bit_and.AddFunction(
	    AggregateFunction::UnaryAggregateDestructor<BitState<string_t>, string_t, string_t, BitStringAndOperation>(
	        LogicalType::BIT, LogicalType::BIT));
	set.AddFunction(bit_and);
}

void BitOrFun::RegisterFunction(BuiltinFunctions &set) {
	AggregateFunctionSet bit_or("bit_or");
	for (auto &type : LogicalType::Integral()) {
		bit_or.AddFunction(GetBitfieldUnaryAggregate<BitOrOperation>(type));
	}
	bit_or.AddFunction(
	    AggregateFunction::UnaryAggregateDestructor<BitState<string_t>, string_t, string_t, BitStringOrOperation>(
	        LogicalType::BIT, LogicalType::BIT));
	set.AddFunction(bit_or);
}

void BitXorFun::RegisterFunction(BuiltinFunctions &set) {
	AggregateFunctionSet bit_xor("bit_xor");
	for (auto &type : LogicalType::Integral()) {
		bit_xor.AddFunction(GetBitfieldUnaryAggregate<BitXorOperation>(type));
	}
	bit_xor.AddFunction(
	    AggregateFunction::UnaryAggregateDestructor<BitState<string_t>, string_t, string_t, BitStringXorOperation>(
	        LogicalType::BIT, LogicalType::BIT));
	set.AddFunction(bit_xor);
}





template <class T>
struct BitAggState {
    bool is_set;
    T value;
};

struct BitstringAggBindData : public FunctionData {
    Value min;
    Value max;

    unique_ptr<FunctionData> Copy() const override {
        return make_unique<BitstringAggBindData>(*this);
    }

    bool Equals(const FunctionData &other_p) const override {
        auto &other = (BitstringAggBindData &)other_p;
        if (min.IsNull() && other.min.IsNull() && max.IsNull() && other.max.IsNull()) {
            return true;
        }
        if (Value::NotDistinctFrom(min, other.min) && Value::NotDistinctFrom(max, other.max)) {
            return true;
        }

        return false;
    }
};

struct BitStringAggOperation {

    template <class STATE>
    static void Initialize(STATE *state) {
        //  If there are no matching rows, returns a null value.
        state->is_set = false;
    }

    template <class INPUT_TYPE, class STATE, class OP>
    static void Operation(STATE *state, AggregateInputData &data, INPUT_TYPE *input, ValidityMask &mask, idx_t idx) {
        if (!state->is_set) {
            auto bind_agg_data = (BitstringAggBindData *)data.bind_data;
            printf("Creating new state from %s and %s\n", bind_agg_data->min.ToString().c_str(), bind_agg_data->max.ToString().c_str());

            auto internal_type = bind_agg_data->min.type().InternalType();
            idx_t len;
            switch (internal_type) {
            case PhysicalType::INT32:
                len = bind_agg_data->max.GetValueUnsafe<int32_t>() - bind_agg_data->min.GetValueUnsafe<int32_t>();
            default:
                throw InternalException("Unsupported type for bitstring aggregation");
            }
            len = len % 8 ? (len / 8) + 1 : len / 8;
            len++;

            auto ptr = new char[len];
            auto target = string_t(ptr, len);

        } else {
            Bit::SetBit(state->value, *input, 1);
        }
    }

    template <class INPUT_TYPE, class STATE, class OP>
    static void ConstantOperation(STATE *state, AggregateInputData &aggr_input_data, INPUT_TYPE *input,
                                  ValidityMask &mask, idx_t count) {
        //  count is not relevant
        OP::template Operation<INPUT_TYPE, STATE, OP>(state, aggr_input_data, input, mask, 0);
    }


    template <class STATE, class OP>
    static void Combine(const STATE &source, STATE *target, AggregateInputData &) {
        if (!source.is_set) {
            // source is NULL, nothing to do.
            return;
        }
        if (!target->is_set) {
            // target is NULL, use source value directly.
            *target = source;
            target->is_set = true;
        } else {
            Bit::BitwiseOr(source.value, target->value, target->value);
        }
    }

    template <class T, class STATE>
    static void Finalize(Vector &result, AggregateInputData &, STATE *state, T *target, ValidityMask &mask, idx_t idx) {
        if (!state->is_set) {
            mask.SetInvalid(idx);
        } else {
            target[idx] = state->value;
        }
    }

    template <class STATE>
    static void Destroy(STATE *state) {
        if (state->is_set && !state->value.IsInlined()) {
            delete[] state->value.GetDataUnsafe();
        }
    }

    static bool IgnoreNull() {
        return true;
    }
};

unique_ptr<BaseStatistics> BitstringPropagateStats(ClientContext &context, BoundAggregateExpression &expr,
                                             FunctionData *bind_data, vector<unique_ptr<BaseStatistics>> &child_stats,
                                             NodeStatistics *node_stats) {
    printf("\nMaking Stats\n");

    if (child_stats[0] && node_stats && node_stats->has_max_cardinality) {
        auto &numeric_stats = (NumericStatistics &) *child_stats[0];
        if (numeric_stats.min.IsNull() || numeric_stats.max.IsNull()) {
            return nullptr;
        }
        printf("\nStats: %s\n", numeric_stats.ToString().c_str());
        auto bind_agg_data = (BitstringAggBindData *)bind_data;
        bind_agg_data->min = numeric_stats.min;
        bind_agg_data->max = numeric_stats.max;
    }
    return nullptr;
}




unique_ptr<FunctionData> BindBitstringAgg(ClientContext &context, AggregateFunction &function,
                                      vector<unique_ptr<Expression>> &arguments) {
    printf("\nMaking a new BindData struct\n");
    return make_unique<BitstringAggBindData>();
}

AggregateFunction BitStringAggFun::GetBitStringAggregate(PhysicalType type) {
  switch (type) {
//  case PhysicalType::INT16: {
//      auto function = AggregateFunction::UnaryAggregate<BitState<string_t>, int16_t, string_t, IntegerSumOperation>(
//          LogicalType::SMALLINT, LogicalType::BIT);
//      return function;
//  }

  case PhysicalType::INT32: {
      auto function =
          AggregateFunction::UnaryAggregateDestructor<BitAggState<string_t>, int32_t, string_t, BitStringAggOperation>(
              LogicalType::INTEGER, LogicalType::BIT);
      function.bind = BindBitstringAgg; // create new a 'BistringAggBindData'
      function.statistics = BitstringPropagateStats;  // stores min and max from column stats in BistringAggBindData
      return function; // uses the BistringAggBindData to access statistics for creating bitstring
  }
//  case PhysicalType::INT64: {
//      auto function =
//          AggregateFunction::UnaryAggregate<BitState<string_t>, int64_t, string_t, SumToHugeintOperation>(
//              LogicalType::BIGINT, LogicalType::BIT);
//      return function;
//  }
//  case PhysicalType::INT128: {
//      auto function =
//          AggregateFunction::UnaryAggregate<BitState<string_t>, hugeint_t, string_t, HugeintSumOperation>(
//              LogicalType::HUGEINT, LogicalType::BIT);
//      return function;
//  }
  default:
      throw InternalException("Unimplemented bistring aggregate" + TypeIdToString(type));
  }
}


void BitStringAggFun::RegisterFunction(BuiltinFunctions &set) {
 AggregateFunctionSet bitstring_agg("bitstring_agg");

//  bitstring_agg.AddFunction(GetBitStringAggregate(PhysicalType::INT16));
  bitstring_agg.AddFunction(GetBitStringAggregate(PhysicalType::INT32));
//  bitstring_agg.AddFunction(GetBitStringAggregate(PhysicalType::INT64));
//  bitstring_agg.AddFunction(GetBitStringAggregate(PhysicalType::INT128));

  // for (auto &type : LogicalType::Integral()) {
  //     bitstring_agg.AddFunction(GetBitfieldUnaryAggregate<BitStringAggOperation>(type));
  // }
  // bitstring_agg.AddFunction(
  //         AggregateFunction::UnaryAggregateDestructor<BitState<string_t>, string_t, string_t, BitStringXorOperation>(
  //                 LogicalType::BIT, LogicalType::BIT));
  set.AddFunction(bitstring_agg);
}

} // namespace duckdb
