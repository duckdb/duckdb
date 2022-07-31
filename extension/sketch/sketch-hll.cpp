#include "include/sketch-sum.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/types/decimal.hpp"
#include "duckdb/function/aggregate/algebraic_functions.hpp"
#include "duckdb/function/aggregate/distributive_functions.hpp"
#include "duckdb/function/aggregate/sum_helpers.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/parser/parsed_data/create_aggregate_function_info.hpp"
#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"
#include "duckdb/storage/statistics/numeric_statistics.hpp"

#include "third_party/apache-datasketches/hll/include/hll.hpp"

#include <iostream>

namespace duckdb {

class HllStateBase {
    static const uint8_t lg_sketch_rows=12;
  public:       
    bool isset;
	void Initialize() {
		this->isset = false;
        this->value_sketch = nullptr;
        this->union_sketch = nullptr;
        this->serialized = nullptr;
	}

	void Combine(const HllStateBase &other) {
		this->isset = other.isset || this->isset;
        GetUnionSketch().update(other.GetSketch());
	}

    string_t Serialize() {
        auto result = GetUnionSketch().get_result();
        serialized = new vector<uint8_t>(result.serialize_compact());

        // I'm pretty sure that if the result is too long then this is going to crash.
        // Need to figure out how to safely allocate data.
        return string_t((char *) serialized->data(), serialized->size());
    }

    void Destroy() {
        
        if (this->value_sketch != nullptr) {
            delete(this->value_sketch);
            this->value_sketch = nullptr;
        } 
        if (this->union_sketch != nullptr) {
            delete(this->union_sketch);
            this->union_sketch = nullptr;
        }
        if (this->serialized != nullptr) {
            delete(this->serialized);
            this->serialized = nullptr;
        }
        
    }

  protected:
    datasketches::hll_sketch& GetSketch() const {
        // No one should try to get the value sketch after starting to combine.
        D_ASSERT(this->union_sketch == nullptr);
        if (this->value_sketch == nullptr) {
            this->value_sketch = new datasketches::hll_sketch(lg_sketch_rows);
        }
        return *this->value_sketch;
    }

  private:

    datasketches::hll_union& GetUnionSketch() {
        if (this->union_sketch == nullptr) {
            this->union_sketch = new datasketches::hll_union(lg_sketch_rows);
        
            // Once we get the union sketch, load it with the data in the value sketch, and destroy
            // the value sketch.
            if (this->value_sketch != nullptr) {
                this->union_sketch->update(*this->value_sketch);
            }
        }
        return *this->union_sketch;
    }
    
  private:
    mutable datasketches::hll_sketch* value_sketch;
    mutable datasketches::hll_union* union_sketch;
    vector<uint8_t>* serialized;
};

template <class T>
class HllState : public HllStateBase {
  public:
    void Update(T value) {
        this->isset = true;
        this->GetSketch().update(value);
    }
};

template <> class HllState<hugeint_t> : public HllStateBase {
  public:
    void Update(hugeint_t value) {
        this->isset = true;
        this->GetSketch().update(&value, sizeof(value));
    }
};

template <> class HllState<string_t> : public HllStateBase {
  public:
    void Update(string_t value) {
        this->isset = true;
        this->GetSketch().update(value.GetDataUnsafe(), value.GetSize());
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

    template <class STATE>
	static void Destroy(STATE *state) {
		state->Destroy();
    }

	static bool IgnoreNull() {
		return true;
	}
};

AggregateFunction GetInitAggregate(PhysicalType type) {
    switch (type) {
    case PhysicalType::INT128: {
        auto function =
            AggregateFunction::UnaryAggregateDestructor<HllState<hugeint_t>, hugeint_t, string_t, HllOperation>(
                LogicalType::HUGEINT, LogicalType::BLOB);
        return function;
    }
    case PhysicalType::INT64: {
        auto function =
            AggregateFunction::UnaryAggregateDestructor<HllState<int64_t>, int64_t, string_t, HllOperation>(
                LogicalType::BIGINT, LogicalType::BLOB);
        return function;
    }
    case PhysicalType::INT32: {
        auto function =
            AggregateFunction::UnaryAggregateDestructor<HllState<int32_t>, int32_t, string_t, HllOperation>(
                LogicalType::INTEGER, LogicalType::BLOB);
        return function;
    }
    case PhysicalType::INT16: {
        auto function =
            AggregateFunction::UnaryAggregateDestructor<HllState<int16_t>, int16_t, string_t, HllOperation>(
                LogicalType::SMALLINT, LogicalType::BLOB);
        return function;
    }
    case PhysicalType::DOUBLE: {
        auto function =
            AggregateFunction::UnaryAggregateDestructor<HllState<double>, double, string_t, HllOperation>(
                LogicalType::DOUBLE, LogicalType::BLOB);
        return function;
    }
    case PhysicalType::VARCHAR: {
        auto function =
            AggregateFunction::UnaryAggregateDestructor<HllState<string_t>, string_t, string_t, HllOperation>(
                LogicalType::VARCHAR, LogicalType::BLOB);
        return function;
    }
    default:
        throw InternalException("Unimplemented sum aggregate");
    }
}

unique_ptr<FunctionData> BindDecimalHllInit(ClientContext &context, AggregateFunction &function,
                                        vector<unique_ptr<Expression>> &arguments) {
    auto decimal_type = arguments[0]->return_type;
    function = GetInitAggregate(decimal_type.InternalType());
    function.name = "hll_count_init";
    function.arguments[0] = decimal_type;
    function.return_type = LogicalType::DECIMAL(Decimal::MAX_WIDTH_DECIMAL, DecimalType::GetScale(decimal_type));
    return nullptr;
}

struct ExtractOperator {
    template <class TA, class TR>
    static TR Operation(TA input) {	
        auto sketch = datasketches::hll_sketch::deserialize(input.GetDataUnsafe(),
                                                            input.GetSize());
        // TODO: Check for error.
        return sketch.get_estimate();
    }
};


void SketchSum::RegisterFunction(ClientContext &context) {
    AggregateFunctionSet init("hll_count_init");
    init.AddFunction(GetInitAggregate(PhysicalType::INT128));
    init.AddFunction(GetInitAggregate(PhysicalType::INT64));
    init.AddFunction(GetInitAggregate(PhysicalType::INT32));
    init.AddFunction(GetInitAggregate(PhysicalType::INT16));
    init.AddFunction(GetInitAggregate(PhysicalType::DOUBLE));
    init.AddFunction(GetInitAggregate(PhysicalType::VARCHAR));
    init.AddFunction(AggregateFunction({LogicalTypeId::DECIMAL}, LogicalTypeId::DECIMAL, nullptr, nullptr, nullptr,
                                      nullptr, nullptr, FunctionNullHandling::DEFAULT_NULL_HANDLING, nullptr,
                                      BindDecimalHllInit));

    ScalarFunctionSet extract("hll_count_extract");
    extract.AddFunction(ScalarFunction({LogicalType::BLOB}, LogicalType::BIGINT, 
                                        ScalarFunction::UnaryFunction<string_t, int64_t, ExtractOperator>));

    auto &catalog = Catalog::GetCatalog(context);
    CreateAggregateFunctionInfo init_info(move(init));
    catalog.AddFunction(context, &init_info);
    CreateScalarFunctionInfo extract_info(move(extract));
    catalog.AddFunction(context, &extract_info);
}

} // namespace duckdb
