#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/function/aggregate/holistic_functions.hpp"
#include "duckdb/planner/expression.hpp"
#include "pcg_random.hpp"

#include <algorithm>
#include <cmath>
#include <queue>
#include <random>
#include <stdlib.h>
#include <utility>

namespace duckdb {

struct reservoir_quantile_state_t {
	data_ptr_t v;

	idx_t len;
	idx_t pos;
	//! These are only for the Reservoir Sampling
	pcg32 *rng;
	std::uniform_real_distribution<double> *uniform_dist;
	//! The next element to sample
	idx_t next_index;
	//! The reservoir threshold of the current min entry
	double min_threshold;
	//! The reservoir index of the current min entry
	idx_t min_entry;
	//! The current count towards next index (i.e. we will replace an entry in next_index - current_count tuples)
	idx_t current_count;
	idx_t input_pos;
	//! Priority queue of [random element, index] for each of the elements in the sample
	std::priority_queue<std::pair<double, idx_t>> *reservoir_weights;
};

template <class STATE> void SetNextEntry(STATE *state) {
	//! 4. Let r = random(0, 1) and Xw = log(r) / log(T_w)
	auto &min_key = state->reservoir_weights->top();
	double T_w = -min_key.first;
	double r = (*state->uniform_dist)(*state->rng);
	double X_w = log(r) / log(T_w);
	//! 5. From the current item vc skip items until item vi , such that:
	//! 6. wc +wc+1 +···+wi−1 < Xw <= wc +wc+1 +···+wi−1 +wi
	//! since all our weights are 1 (uniform sampling), we can just determine the amount of elements to skip
	state->min_threshold = T_w;
	state->min_entry = min_key.second;
	state->next_index = MaxValue<idx_t>(1, idx_t(round(X_w)));
	state->current_count = 0;
}

template <class STATE, class T> void ReplaceElement(T &input, STATE *state) {
	//! replace the entry in the reservoir
	//! 7. The item in R with the minimum key is replaced by item vi
	((T *)state->v)[state->min_entry] = input;
	//! pop the minimum entry
	state->reservoir_weights->pop();
	//! now update the reservoir
	//! 8. Let tw = Tw i , r2 = random(tw,1) and vi’s key: ki = (r2)1/wi
	//! 9. The new threshold Tw is the new minimum key of R
	//! we generate a random number between (min_threshold, 1)
	std::uniform_real_distribution<double> dist(state->min_threshold, 1);
	double r2 = dist(*state->rng);
	//! now we insert the new weight into the reservoir
	state->reservoir_weights->push(std::make_pair(-r2, state->min_entry));
	//! we update the min entry with the new min entry in the reservoir
	SetNextEntry<STATE>(state);
}

struct ReservoirQuantileBindData : public FunctionData {
	ReservoirQuantileBindData(float quantile_, int32_t sample_size_) : quantile(quantile_), sample_size(sample_size_) {
	}

	unique_ptr<FunctionData> Copy() override {
		return make_unique<ReservoirQuantileBindData>(quantile, sample_size);
	}

	bool Equals(FunctionData &other_p) override {
		auto &other = (ReservoirQuantileBindData &)other_p;
		return quantile == other.quantile;
	}

	float quantile;
	int32_t sample_size;
};

template <class T> struct ReservoirQuantileOperation {
	template <class STATE> static void Initialize(STATE *state) {
		state->v = nullptr;
		state->len = 0;
		state->pos = 0;
		state->input_pos = 0;
		state->next_index = 0;
		state->min_threshold = 0;
		state->min_entry = 0;
		state->current_count = 0;
		//! Seed with a real random value, if available
		pcg_extras::seed_seq_from<std::random_device> seed_source;
		//! Make a random number engine
		state->rng = new pcg32(seed_source);
		state->uniform_dist = new std::uniform_real_distribution<double>(0, 1);
		state->reservoir_weights = new std::priority_queue<std::pair<double, idx_t>>();
	}

	static void resize_state(reservoir_quantile_state_t *state, idx_t new_len) {
		if (new_len <= state->len) {
			return;
		}
		state->v = (data_ptr_t)realloc(state->v, new_len * sizeof(T));
		if (!state->v) {
			throw InternalException("Memory allocation failure");
		}
		state->len = new_len;
	}

	template <class INPUT_TYPE, class STATE, class OP>
	static void ConstantOperation(STATE *state, FunctionData *bind_data, INPUT_TYPE *input, nullmask_t &nullmask,
	                              idx_t count) {
		for (idx_t i = 0; i < count; i++) {
			Operation<INPUT_TYPE, STATE, OP>(state, bind_data, input, nullmask, 0);
		}
	}

	template <class INPUT_TYPE, class STATE, class OP>
	static void Operation(STATE *state, FunctionData *bind_data_, INPUT_TYPE *data, nullmask_t &nullmask, idx_t idx) {
		auto bind_data = (ReservoirQuantileBindData *)bind_data_;
		D_ASSERT(bind_data);
		if (nullmask[idx]) {
			return;
		}
		if (state->pos == 0) {
			resize_state(state, bind_data->sample_size);
		}
		D_ASSERT(state->v);
		if (state->pos < (idx_t)bind_data->sample_size) {
			//! 1: The first m items of V are inserted into R
			//! first we need to check if the reservoir already has "m" elements
			((T *)state->v)[state->pos++] = data[idx];
			if (state->pos == (idx_t) bind_data->sample_size) {
				//! 2. For each item vi ∈ R: Calculate a key ki = random(0, 1)
				//! we then define the threshold to enter the reservoir T_w as the minimum key of R
				//! we use a priority queue to extract the minimum key in O(1) time
				for (idx_t i = 0; i < (idx_t)bind_data->sample_size; i++) {
					double k_i = (*state->uniform_dist)(*state->rng);
					state->reservoir_weights->push(std::make_pair(-k_i, i));
				}
				SetNextEntry<STATE>(state);
			}
		} else {
			//! now that we have the sample, we start our replacement strategy
			//! 3. Repeat Steps 5–10 until the population is exhausted
			D_ASSERT(state->next_index >= state->current_count);
			if (state->next_index == state->current_count) {
				ReplaceElement<STATE, T>(data[idx], state);
			}
		}
		state->current_count++;
	}

	template <class STATE, class OP> static void Combine(STATE source, STATE *target) {
		if (source.pos == 0) {
			return;
		}
		for (idx_t src_idx = 0; src_idx < source.pos; src_idx++) {
			if (target->pos < target->len) {
				//! 1: The first m items of V are inserted into R
				//! first we need to check if the reservoir already has "m" elements
				((T *)target->v)[target->pos++] = ((T *)source.v)[src_idx];
				if (target->pos == target->len) {
					//! 2. For each item vi ∈ R: Calculate a key ki = random(0, 1)
					//! we then define the threshold to enter the reservoir T_w as the minimum key of R
					//! we use a priority queue to extract the minimum key in O(1) time
					for (idx_t i = 0; i < (idx_t)target->len; i++) {
						double k_i = (*target->uniform_dist)(*target->rng);
						target->reservoir_weights->push(std::make_pair(-k_i, i));
					}
					SetNextEntry<STATE>(target);
				} else {
					//! now that we have the sample, we start our replacement strategy
					//! 3. Repeat Steps 5–10 until the population is exhausted
					if (target->next_index == target->current_count) {
						ReplaceElement<STATE, T>(((T *)source.v)[src_idx], target);
					}
				}
				target->current_count++;
			}
		}
	}

	template <class TARGET_TYPE, class STATE>
	static void Finalize(Vector &result, FunctionData *bind_data_, STATE *state, TARGET_TYPE *target,
	                     nullmask_t &nullmask, idx_t idx) {
		if (state->pos == 0) {
			nullmask[idx] = true;
			return;
		}
		D_ASSERT(state->v);
		D_ASSERT(bind_data_);
		auto bind_data = (ReservoirQuantileBindData *)bind_data_;
		auto v_t = (T *)state->v;
		auto offset = (idx_t)((double)(state->pos - 1) * bind_data->quantile);
		std::nth_element(v_t, v_t + offset, v_t + state->pos);
		target[idx] = v_t[offset];
	}

	template <class STATE> static void Destroy(STATE *state) {
		if (state->v) {
			free(state->v);
			state->v = nullptr;
		}
		if (state->rng) {
			delete state->rng;
			state->rng = nullptr;
		}
		if (state->uniform_dist) {
			delete state->uniform_dist;
			state->uniform_dist = nullptr;
		}
		if (state->reservoir_weights) {
			delete state->reservoir_weights;
			state->reservoir_weights = nullptr;
		}
	}

	static bool IgnoreNull() {
		return true;
	}
};

AggregateFunction GetReservoirQuantileAggregateFunction(PhysicalType type) {
	switch (type) {
	case PhysicalType::INT16:
		return AggregateFunction::UnaryAggregateDestructor<reservoir_quantile_state_t, int16_t, int16_t,
		                                                   ReservoirQuantileOperation<int16_t>>(LogicalType::SMALLINT,
		                                                                                        LogicalType::SMALLINT);

	case PhysicalType::INT32:
		return AggregateFunction::UnaryAggregateDestructor<reservoir_quantile_state_t, int32_t, int32_t,
		                                                   ReservoirQuantileOperation<int32_t>>(LogicalType::INTEGER,
		                                                                                        LogicalType::INTEGER);

	case PhysicalType::INT64:
		return AggregateFunction::UnaryAggregateDestructor<reservoir_quantile_state_t, int64_t, int64_t,
		                                                   ReservoirQuantileOperation<int64_t>>(LogicalType::BIGINT,
		                                                                                        LogicalType::BIGINT);

	case PhysicalType::INT128:
		return AggregateFunction::UnaryAggregateDestructor<reservoir_quantile_state_t, hugeint_t, hugeint_t,
		                                                   ReservoirQuantileOperation<hugeint_t>>(LogicalType::HUGEINT,
		                                                                                          LogicalType::HUGEINT);
	case PhysicalType::FLOAT:
		return AggregateFunction::UnaryAggregateDestructor<reservoir_quantile_state_t, float, float,
		                                                   ReservoirQuantileOperation<float>>(LogicalType::FLOAT,
		                                                                                      LogicalType::FLOAT);

	case PhysicalType::DOUBLE:
		return AggregateFunction::UnaryAggregateDestructor<reservoir_quantile_state_t, double, double,
		                                                   ReservoirQuantileOperation<double>>(LogicalType::DOUBLE,
		                                                                                       LogicalType::DOUBLE);

	default:
		throw NotImplementedException("Unimplemented quantile aggregate");
	}
}

unique_ptr<FunctionData> bind_reservoir_quantile(ClientContext &context, AggregateFunction &function,
                                                 vector<unique_ptr<Expression>> &arguments) {
	if (!arguments[1]->IsScalar()) {
		throw BinderException("QUANTILE can only take constant quantile parameters");
	}
	Value quantile_val = ExpressionExecutor::EvaluateScalar(*arguments[1]);
	auto quantile = quantile_val.GetValue<float>();

	if (quantile_val.is_null || quantile < 0 || quantile > 1) {
		throw BinderException("QUANTILE can only take parameters in range [0, 1]");
	}
	if (arguments.size() <= 2) {
		arguments.pop_back();
		return make_unique<ReservoirQuantileBindData>(quantile, 8192);
	}
	if (!arguments[2]->IsScalar()) {
		throw BinderException("QUANTILE can only take constant quantile parameters");
	}
	Value sample_size_val = ExpressionExecutor::EvaluateScalar(*arguments[2]);
	auto sample_size = sample_size_val.GetValue<int32_t>();

	if (sample_size_val.is_null || sample_size <= 0) {
		throw BinderException("Percentage of the sample must be bigger than 0");
	}

	// remove the quantile argument so we can use the unary aggregate
	arguments.pop_back();
	arguments.pop_back();
	return make_unique<ReservoirQuantileBindData>(quantile, sample_size);
}

unique_ptr<FunctionData> bind_reservoir_quantile_decimal(ClientContext &context, AggregateFunction &function,
                                                         vector<unique_ptr<Expression>> &arguments) {
	auto bind_data = bind_reservoir_quantile(context, function, arguments);
	function = GetReservoirQuantileAggregateFunction(arguments[0]->return_type.InternalType());
	function.name = "reservoir_quantile";
	return bind_data;
}

AggregateFunction GetReservoirQuantileAggregate(PhysicalType type) {
	auto fun = GetReservoirQuantileAggregateFunction(type);
	fun.bind = bind_reservoir_quantile;
	// temporarily push an argument so we can bind the actual quantile
	fun.arguments.push_back(LogicalType::FLOAT);
	return fun;
}

void ReservoirQuantileFun::RegisterFunction(BuiltinFunctions &set) {
	AggregateFunctionSet reservoir_quantile("reservoir_quantile");
	reservoir_quantile.AddFunction(AggregateFunction({LogicalType::DECIMAL, LogicalType::FLOAT, LogicalType::INTEGER},
	                                                 LogicalType::DECIMAL, nullptr, nullptr, nullptr, nullptr, nullptr,
	                                                 nullptr, bind_reservoir_quantile_decimal));
	reservoir_quantile.AddFunction(AggregateFunction({LogicalType::DECIMAL, LogicalType::FLOAT}, LogicalType::DECIMAL,
	                                                 nullptr, nullptr, nullptr, nullptr, nullptr, nullptr,
	                                                 bind_reservoir_quantile_decimal));
	reservoir_quantile.AddFunction(GetReservoirQuantileAggregate(PhysicalType::INT16));
	reservoir_quantile.AddFunction(GetReservoirQuantileAggregate(PhysicalType::INT32));
	reservoir_quantile.AddFunction(GetReservoirQuantileAggregate(PhysicalType::INT64));
	reservoir_quantile.AddFunction(GetReservoirQuantileAggregate(PhysicalType::INT128));
	reservoir_quantile.AddFunction(GetReservoirQuantileAggregate(PhysicalType::DOUBLE));

	set.AddFunction(reservoir_quantile);
}

} // namespace duckdb
