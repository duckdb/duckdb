#include "include/sketch-sum.hpp"

#include "duckdb/function/aggregate/distributive_functions.hpp"
#include "duckdb/function/aggregate/sum_helpers.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/types/decimal.hpp"
#include "duckdb/storage/statistics/numeric_statistics.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/function/aggregate/algebraic_functions.hpp"
#include "duckdb/parser/parsed_data/create_aggregate_function_info.hpp"

#include "third_party/apache-datasketches/hll/include/hll.hpp"

#include <iostream>

namespace duckdb {

class HllStateBase {
    static const uint8_t lg_sketch_rows=12;
  public:       
    bool isset;
	void Initialize() {
		this->isset = false;
        std::cout << "Init" << std::endl;
        this->sketch = new datasketches::hll_sketch(lg_sketch_rows);
        this->union_sketch = new datasketches::hll_union(lg_sketch_rows);
	}

	void Combine(const HllStateBase &other) {
       std::cout << "Combine" << std::endl;

		this->isset = other.isset || this->isset;
        if(!this->sketch->is_empty()) {
            this->union_sketch->update(*this->sketch);
            this->sketch->reset();
        }
		this->union_sketch->update(*other.sketch);
	}

    string_t Serialize() {
        std::cout << "Serialize" << std::endl;
        vector<uint8_t> serialized; 
        if (!union_sketch->is_empty()) {
            auto merged = union_sketch->get_result();
            serialized = merged.serialize_compact();    
            std::cout << "Merged: " << merged.get_estimate()  << std::endl;

        } else {
            serialized = this->sketch->serialize_compact();
            std::cout << "Raw: " << this->sketch->get_estimate()  << std::endl;

        }

        // I'm pretty sure that if the result is too long then this is going to crash. Need to figure out
        // how to safely allocate data.
        return string_t((char *) serialized.data(), serialized.size());
    }

    void Finalize() {
        std::cout << "Finalize" << std::endl;
        delete(sketch);
        sketch = nullptr;
        delete(union_sketch);
        union_sketch = nullptr;
    }

  protected:
    datasketches::hll_sketch* sketch;
    datasketches::hll_union* union_sketch;
};


template <class T>
class HllState : public HllStateBase {
  public:
    void Update(T value) {
        this->isset = true;
        this->sketch->update(value);
    }
};

template <> class HllState<hugeint_t> : public HllStateBase {
  public:
    void Update(hugeint_t value) {
        this->isset = true;
        this->sketch->update(&value, sizeof(value));
    }
};

struct HllOperation {
	template <class STATE>
    static void Initialize(STATE *state) {
 		state->Initialize();
	}

    template <class STATE, class OP>
	static void Combine(const STATE &source, STATE *target, AggregateInputData &aggr_input_data) {
		target->Combine(source);
    }

    template <class T, class STATE>
    static void Finalize(Vector &result, AggregateInputData &, STATE *state, T *target, ValidityMask &mask, idx_t idx) {
        if (!state->isset) {
            mask.SetInvalid(idx);
        } else {
            target[idx] = state->Serialize();
        }
        state->Finalize();
    }

	template <class INPUT_TYPE, class STATE, class OP>
    static void Operation(STATE *state, AggregateInputData &, INPUT_TYPE *input, ValidityMask &mask, idx_t idx) {
		state->Update(input[idx]);
	}

    template <class INPUT_TYPE, class STATE, class OP>
	static void ConstantOperation(STATE *state, AggregateInputData &, INPUT_TYPE *input, ValidityMask &mask,
	                              idx_t count) {
        state->Update(*input);
	}

	static bool IgnoreNull() {
		return true;
	}
};

AggregateFunction GetInitAggregate(PhysicalType type) {
    switch (type) {
    case PhysicalType::INT128: {
        auto function =
            AggregateFunction::UnaryAggregate<HllState<hugeint_t>, hugeint_t, string_t, HllOperation>(
                LogicalType::HUGEINT, LogicalType::BLOB);
        return function;
    }
    case PhysicalType::INT64: {
        auto function =
            AggregateFunction::UnaryAggregate<HllState<int64_t>, int64_t, string_t, HllOperation>(
                LogicalType::BIGINT, LogicalType::BLOB);
        return function;
    }
    case PhysicalType::INT32: {
        auto function =
            AggregateFunction::UnaryAggregate<HllState<int32_t>, int32_t, string_t, HllOperation>(
                LogicalType::INTEGER, LogicalType::BLOB);
        return function;
    }
    case PhysicalType::INT16: {
        auto function =
            AggregateFunction::UnaryAggregate<HllState<int16_t>, int16_t, string_t, HllOperation>(
                LogicalType::SMALLINT, LogicalType::BLOB);
        return function;
    }
    default:
        throw InternalException("Unimplemented sum aggregate");
    }
}

void SketchSum::RegisterFunction(ClientContext &context) {
    AggregateFunctionSet init("hll_count_init");
    
    init.AddFunction(GetInitAggregate(PhysicalType::INT128));
    init.AddFunction(GetInitAggregate(PhysicalType::INT64));
    init.AddFunction(GetInitAggregate(PhysicalType::INT32));
    init.AddFunction(GetInitAggregate(PhysicalType::INT16));

    auto &catalog = Catalog::GetCatalog(context);
    CreateAggregateFunctionInfo func_info(move(init));
    catalog.AddFunction(context, &func_info);
}

} // namespace duckdb
