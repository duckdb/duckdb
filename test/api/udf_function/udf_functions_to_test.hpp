/*HEADER file with all UDF Functions to test*/
#pragma once

namespace duckdb {

//UDF Functions to test
inline bool udf_bool(bool a) {return a;}
inline bool udf_bool(bool a, bool b) {return a & b;}
inline bool udf_bool(bool a, bool b, bool c) {return a & b & c;}

inline int8_t udf_int8(int8_t a) {return a;}
inline int8_t udf_int8(int8_t a, int8_t b) {return a * b;}
inline int8_t udf_int8(int8_t a, int8_t b, int8_t c) {return a + b + c;}

inline int16_t udf_int16(int16_t a) {return a;}
inline int16_t udf_int16(int16_t a, int16_t b) {return a * b;}
inline int16_t udf_int16(int16_t a, int16_t b, int16_t c) {return a + b + c;}

inline date_t udf_date(date_t a) {return a;}
inline date_t udf_date(date_t a, date_t b) {return b;}
inline date_t udf_date(date_t a, date_t b, date_t c) {return c;}

inline dtime_t udf_time(dtime_t a) {return a;}
inline dtime_t udf_time(dtime_t a, dtime_t b) {return b;}
inline dtime_t udf_time(dtime_t a, dtime_t b, dtime_t c) {return c;}

inline int udf_int(int a) {return a;}
inline int udf_int(int a, int b) {return a * b;}
inline int udf_int(int a, int b, int c) {return a + b + c;}

inline int64_t udf_int64(int64_t a) {return a;}
inline int64_t udf_int64(int64_t a, int64_t b) {return a * b;}
inline int64_t udf_int64(int64_t a, int64_t b, int64_t c) {return a + b + c;}

inline timestamp_t udf_timestamp(timestamp_t a) {return a;}
inline timestamp_t udf_timestamp(timestamp_t a, timestamp_t b) {return b;}
inline timestamp_t udf_timestamp(timestamp_t a, timestamp_t b, timestamp_t c) {return c;}

inline float udf_float(float a) {return a;}
inline float udf_float(float a, float b) {return a * b;}
inline float udf_float(float a, float b, float c) {return a + b + c;}

inline double udf_double(double a) {return a;}
inline double udf_double(double a, double b) {return a * b;}
inline double udf_double(double a, double b, double c) {return a + b + c;}

inline double udf_decimal(double a) {return a;}
inline double udf_decimal(double a, double b) {return a * b;}
inline double udf_decimal(double a, double b, double c) {return a + b + c;}

inline string_t udf_varchar(string_t a) {return a;}
inline string_t udf_varchar(string_t a, string_t b) {return b;}
inline string_t udf_varchar(string_t a, string_t b, string_t c) {return c;}

// Vectorized UDF Functions -------------------------------------------------------------------

/*
 * This vectorized function is an unary one that copies input values to the result vector
 */
template<typename TYPE>
static void udf_unary_function(DataChunk &input, ExpressionState &state, Vector &result) {
	assert(input.column_count() == 1);
	assert((GetTypeId<TYPE>()) == input.data[0].type);
	assert((GetTypeId<TYPE>()) == result.type);

	switch(GetTypeId<TYPE>()) {
		case TypeId::VARCHAR: {
			result.vector_type = VectorType::FLAT_VECTOR;
			auto result_data = FlatVector::GetData<string_t>(result);
			auto ldata = FlatVector::GetData<string_t>(input.data[0]);

			FlatVector::SetNullmask(result, FlatVector::Nullmask(input.data[0]));

			for (idx_t i = 0; i < input.size(); i++) {
				auto input_length = ldata[i].GetSize();
				string_t target = StringVector::EmptyString(result, input_length);
				auto target_data = target.GetData();
				memcpy(target_data, ldata[i].GetData(), input_length);
				target.Finalize();
				result_data[i] = target;
			}
			break;
		}
		default: {
			result.vector_type = VectorType::FLAT_VECTOR;
			auto result_data = FlatVector::GetData<TYPE>(result);
			auto ldata = FlatVector::GetData<TYPE>(input.data[0]);

			FlatVector::SetNullmask(result, FlatVector::Nullmask(input.data[0]));

			for (idx_t i = 0; i < input.size(); i++) {
				result_data[i] = ldata[i];
			}
		}
	}
}

/*
 * This vectorized function is a binary one that copies values from the second input vector to the result vector
 */
template<typename TYPE>
static void udf_binary_function(DataChunk &input, ExpressionState &state, Vector &result) {
	assert(input.column_count() == 2);
	assert((GetTypeId<TYPE>()) == input.data[0].type);
	assert((GetTypeId<TYPE>()) == input.data[1].type);
	assert((GetTypeId<TYPE>()) == result.type);

	switch(GetTypeId<TYPE>()) {
		case TypeId::VARCHAR: {
			result.vector_type = VectorType::FLAT_VECTOR;
			auto result_data = FlatVector::GetData<string_t>(result);
			auto ldata = FlatVector::GetData<string_t>(input.data[1]);

			FlatVector::SetNullmask(result, FlatVector::Nullmask(input.data[1]));

			for (idx_t i = 0; i < input.size(); i++) {
				auto input_length = ldata[i].GetSize();
				string_t target = StringVector::EmptyString(result, input_length);
				auto target_data = target.GetData();
				memcpy(target_data, ldata[i].GetData(), input_length);
				target.Finalize();
				result_data[i] = target;
			}
			break;
		}
		default: {
			result.vector_type = VectorType::FLAT_VECTOR;
			auto result_data = FlatVector::GetData<TYPE>(result);
			auto ldata = FlatVector::GetData<TYPE>(input.data[1]);

			FlatVector::SetNullmask(result, FlatVector::Nullmask(input.data[1]));

			for (idx_t i = 0; i < input.size(); i++) {
				result_data[i] = ldata[i];
			}
		}
	}
}

/*
 * This vectorized function is a ternary one that copies values from the third input vector to the result vector
 */
template<typename TYPE>
static void udf_ternary_function(DataChunk &input, ExpressionState &state, Vector &result) {
	assert(input.column_count() == 3);
	assert((GetTypeId<TYPE>()) == input.data[0].type);
	assert((GetTypeId<TYPE>()) == input.data[1].type);
	assert((GetTypeId<TYPE>()) == input.data[2].type);
	assert((GetTypeId<TYPE>()) == result.type);

	switch(GetTypeId<TYPE>()) {
		case TypeId::VARCHAR: {
			result.vector_type = VectorType::FLAT_VECTOR;
			auto result_data = FlatVector::GetData<string_t>(result);
			auto ldata = FlatVector::GetData<string_t>(input.data[2]);

			FlatVector::SetNullmask(result, FlatVector::Nullmask(input.data[2]));

			for (idx_t i = 0; i < input.size(); i++) {
				auto input_length = ldata[i].GetSize();
				string_t target = StringVector::EmptyString(result, input_length);
				auto target_data = target.GetData();
				memcpy(target_data, ldata[i].GetData(), input_length);
				target.Finalize();
				result_data[i] = target;
			}
			break;
		}
		default: {
			result.vector_type = VectorType::FLAT_VECTOR;
			auto result_data = FlatVector::GetData<TYPE>(result);
			auto ldata = FlatVector::GetData<TYPE>(input.data[2]);

			FlatVector::SetNullmask(result, FlatVector::Nullmask(input.data[2]));

			for (idx_t i = 0; i < input.size(); i++) {
				result_data[i] = ldata[i];
			}
		}
	}
}

/*
 * Vectorized function with the number of input as a template parameter
 */
template<typename TYPE, int NUM_INPUT>
static void udf_several_constant_input(DataChunk &input, ExpressionState &state, Vector &result) {
	assert(input.column_count() == NUM_INPUT);
	for(idx_t i = 0; i < NUM_INPUT; ++i) {
		assert((GetTypeId<TYPE>()) == input.data[i].type);
	}
	assert((GetTypeId<TYPE>()) == result.type);

	result.vector_type = VectorType::CONSTANT_VECTOR;
	auto result_data = ConstantVector::GetData<TYPE>(result);
	auto ldata = ConstantVector::GetData<TYPE>(input.data[NUM_INPUT - 1 ]);

	for (idx_t i = 0; i < input.size(); i++) {
		result_data[i] = ldata[i];
	}
}

/*
 * Vectorized MAX function with varargs and constant inputs
 */
template<typename TYPE>
static void udf_max_constant(DataChunk &args, ExpressionState &state, Vector &result) {
	TYPE max = 0;
	result.vector_type = VectorType::CONSTANT_VECTOR;
	for (idx_t col_idx = 0; col_idx < args.column_count(); col_idx++) {
		auto &input = args.data[col_idx];
		assert((GetTypeId<TYPE>()) == input.type);
		if (ConstantVector::IsNull(input)) {
			// constant null, skip
			continue;
		}
		auto input_data = ConstantVector::GetData<TYPE>(input);
		if(max < input_data[0]) {
			max = input_data[0];
		}
	}
	auto result_data = ConstantVector::GetData<TYPE>(result);
	result_data[0] = max;
}

/*
 * Vectorized MAX function with varargs and input columns
 */
template<typename TYPE>
static void udf_max_flat(DataChunk &args, ExpressionState &state, Vector &result) {
	assert(TypeIsNumeric(GetTypeId<TYPE>()));

	result.vector_type = VectorType::FLAT_VECTOR;
	auto result_data = FlatVector::GetData<TYPE>(result);

	//Initialize the result vector with the minimum value from TYPE.
	memset(result_data, std::numeric_limits<TYPE>::min(), args.size() * sizeof(TYPE));

	for (idx_t col_idx = 0; col_idx < args.column_count(); col_idx++) {
		auto &input = args.data[col_idx];
		assert((GetTypeId<TYPE>()) == input.type);
		auto input_data = FlatVector::GetData<TYPE>(input);
		for(idx_t i=0; i < args.size(); ++i) {
			if(result_data[i] < input_data[i]) {
				result_data[i] = input_data[i];
			}
		}
	}
}

// Aggregate UDF to test -------------------------------------------------------------------

// AVG function copied from "src/function/aggregate/algebraic/avg.cpp"
template <class T> struct udf_avg_state_t {
	uint64_t count;
	T sum;
};

struct UDFAverageFunction {
	template <class STATE> static void Initialize(STATE *state) {
		state->count = 0;
		state->sum = 0;
	}

	template <class INPUT_TYPE, class STATE, class OP>
	static void Operation(STATE *state, INPUT_TYPE *input, nullmask_t &nullmask, idx_t idx) {
		state->sum += input[idx];
		state->count++;
	}

	template <class INPUT_TYPE, class STATE, class OP>
	static void ConstantOperation(STATE *state, INPUT_TYPE *input, nullmask_t &nullmask, idx_t count) {
		state->count += count;
		state->sum += input[0] * count;
	}

	template <class STATE, class OP> static void Combine(STATE source, STATE *target) {
		target->count += source.count;
		target->sum += source.sum;
	}

	template <class T, class STATE>
	static void Finalize(Vector &result, STATE *state, T *target, nullmask_t &nullmask, idx_t idx) {
		if (!Value::DoubleIsValid(state->sum)) {
			throw OutOfRangeException("AVG is out of range!");
		} else if (state->count == 0) {
			nullmask[idx] = true;
		} else {
			target[idx] = state->sum / state->count;
		}
	}

	static bool IgnoreNull() {
		return true;
	}
};

// COVAR function copied from "src/function/aggregate/algebraic/covar.cpp"

//------------------ COVAR --------------------------------//
struct udf_covar_state_t {
	uint64_t count;
	double meanx;
	double meany;
	double co_moment;
};

struct UDFCovarOperation {
	template <class STATE> static void Initialize(STATE *state) {
		state->count = 0;
		state->meanx = 0;
		state->meany = 0;
		state->co_moment = 0;
	}

	template <class A_TYPE, class B_TYPE, class STATE, class OP>
	static void Operation(STATE *state, A_TYPE *x_data, B_TYPE *y_data, nullmask_t &anullmask, nullmask_t &bnullmask,
	                      idx_t xidx, idx_t yidx) {
		// update running mean and d^2
		const uint64_t n = ++(state->count);

		const auto x = x_data[xidx];
		const double dx = (x - state->meanx);
		const double meanx = state->meanx + dx / n;

		const auto y = y_data[yidx];
		const double dy = (y - state->meany);
		const double meany = state->meany + dy / n;

		const double C = state->co_moment + dx * (y - meany);

		state->meanx = meanx;
		state->meany = meany;
		state->co_moment = C;
	}

	template <class STATE, class OP> static void Combine(STATE source, STATE *target) {
		if (target->count == 0) {
			*target = source;
		} else if (source.count > 0) {
			const auto count = target->count + source.count;
			const auto meanx = (source.count * source.meanx + target->count * target->meanx) / count;
			const auto meany = (source.count * source.meany + target->count * target->meany) / count;

			//  Schubert and Gertz SSDBM 2018, equation 21
			const auto deltax = target->meanx - source.meanx;
			const auto deltay = target->meany - source.meany;
			target->co_moment =
			    source.co_moment + target->co_moment + deltax * deltay * source.count * target->count / count;
			target->meanx = meanx;
			target->meany = meany;
			target->count = count;
		}
	}

	static bool IgnoreNull() {
		return true;
	}
};

struct UDFCovarPopOperation : public UDFCovarOperation {
	template <class T, class STATE>
	static void Finalize(Vector &result, STATE *state, T *target, nullmask_t &nullmask, idx_t idx) {
		if (state->count == 0) {
			nullmask[idx] = true;
		} else {
			target[idx] = state->co_moment / state->count;
		}
	}
};

// UDFSum function based on "src/function/aggregate/distributive/sum.cpp"

//------------------ UDFSum --------------------------------//
struct UDFSum {
	typedef struct {
		double value;
		bool isset;
	} sum_state_t;

	template <class STATE> static idx_t StateSize() {
		return sizeof(STATE);
	}

	template <class STATE> static void Initialize(data_ptr_t state) {
		((STATE *)state)->value = 0;
		((STATE *)state)->isset = false;
	}

	template <class INPUT_TYPE, class STATE>
	static void Operation(STATE *state, INPUT_TYPE *input, idx_t idx) {
		state->isset = true;
		state->value += input[idx];
	}

	template <class INPUT_TYPE, class STATE>
	static void ConstantOperation(STATE *state, INPUT_TYPE *input, idx_t count) {
		state->isset = true;
		state->value += (INPUT_TYPE)input[0] * (INPUT_TYPE)count;
	}

	template <class STATE_TYPE, class INPUT_TYPE>
	static void Update(Vector inputs[], idx_t input_count, Vector &states, idx_t count) {
		assert(input_count == 1);

		if (inputs[0].vector_type == VectorType::CONSTANT_VECTOR && states.vector_type == VectorType::CONSTANT_VECTOR) {
			if (ConstantVector::IsNull(inputs[0])) {
				// constant NULL input in function that ignores NULL values
				return;
			}
			// regular constant: get first state
			auto idata = ConstantVector::GetData<INPUT_TYPE>(inputs[0]);
			auto sdata = ConstantVector::GetData<STATE_TYPE *>(states);
			UDFSum::ConstantOperation<INPUT_TYPE, STATE_TYPE>(*sdata, idata, count);
		} else if (inputs[0].vector_type == VectorType::FLAT_VECTOR && states.vector_type == VectorType::FLAT_VECTOR) {
			auto idata = FlatVector::GetData<INPUT_TYPE>(inputs[0]);
			auto sdata = FlatVector::GetData<STATE_TYPE *>(states);
			auto nullmask = FlatVector::Nullmask(inputs[0]);
			if (nullmask.any()) {
				// potential NULL values and NULL values are ignored
				for (idx_t i = 0; i < count; i++) {
					if (!nullmask[i]) {
						UDFSum::Operation<INPUT_TYPE, STATE_TYPE>(sdata[i], idata, i);
					}
				}
			} else {
				// quick path: no NULL values or NULL values are not ignored
				for (idx_t i = 0; i < count; i++) {
					UDFSum::Operation<INPUT_TYPE, STATE_TYPE>(sdata[i], idata, i);
				}
			}
		} else {
			throw duckdb::NotImplementedException("UDFSum only supports CONSTANT and FLAT vectors!");
		}
	}

	template <class STATE_TYPE, class INPUT_TYPE>
	static void SimpleUpdate(Vector inputs[], idx_t input_count, data_ptr_t state, idx_t count) {
		assert(input_count == 1);
		switch (inputs[0].vector_type) {
			case VectorType::CONSTANT_VECTOR: {
				if (ConstantVector::IsNull(inputs[0])) {
					return;
				}
				auto idata = ConstantVector::GetData<INPUT_TYPE>(inputs[0]);
				UDFSum::ConstantOperation<INPUT_TYPE, STATE_TYPE>((STATE_TYPE *)state, idata, count);
				break;
			}
			case VectorType::FLAT_VECTOR: {
				auto idata = FlatVector::GetData<INPUT_TYPE>(inputs[0]);
				auto nullmask = FlatVector::Nullmask(inputs[0]);
				if (nullmask.any()) {
					// potential NULL values and NULL values are ignored
					for (idx_t i = 0; i < count; i++) {
						if (!nullmask[i]) {
							UDFSum::Operation<INPUT_TYPE, STATE_TYPE>((STATE_TYPE *)state, idata, i);
						}
					}
				} else {
					// quick path: no NULL values or NULL values are not ignored
					for (idx_t i = 0; i < count; i++) {
						UDFSum::Operation<INPUT_TYPE, STATE_TYPE>((STATE_TYPE *)state, idata, i);
					}
				}
				break;
			}
			default: {
				throw duckdb::NotImplementedException("UDFSum only supports CONSTANT and FLAT vectors!");
			}
		}
	}

	template <class STATE_TYPE> static void Combine(Vector &source, Vector &target, idx_t count) {
		assert(source.type == TypeId::POINTER && target.type == TypeId::POINTER);
		auto sdata = FlatVector::GetData<STATE_TYPE *>(source);
		auto tdata = FlatVector::GetData<STATE_TYPE *>(target);
		// OP::template Combine<STATE_TYPE, OP>(*sdata[i], tdata[i]);
		for (idx_t i = 0; i < count; i++) {
			if (!sdata[i]->isset) {
				// source is NULL, nothing to do
				return;
			}
			if (!tdata[i]->isset) {
				// target is NULL, use source value directly
				*tdata[i] = *sdata[i];
			} else {
				// else perform the operation
				tdata[i]->value += sdata[i]->value;
			}
		}
	}

	template <class STATE_TYPE, class RESULT_TYPE>
	static void Finalize(Vector &states, Vector &result, idx_t count) {
		if (states.vector_type == VectorType::CONSTANT_VECTOR) {
			result.vector_type = VectorType::CONSTANT_VECTOR;

			auto sdata = ConstantVector::GetData<STATE_TYPE *>(states);
			auto rdata = ConstantVector::GetData<RESULT_TYPE>(result);
			UDFSum::Finalize<RESULT_TYPE, STATE_TYPE>(result, *sdata, rdata, ConstantVector::Nullmask(result), 0);
		} else {
			assert(states.vector_type == VectorType::FLAT_VECTOR);
			result.vector_type = VectorType::FLAT_VECTOR;

			auto sdata = FlatVector::GetData<STATE_TYPE *>(states);
			auto rdata = FlatVector::GetData<RESULT_TYPE>(result);
			for (idx_t i = 0; i < count; i++) {
				UDFSum::Finalize<RESULT_TYPE, STATE_TYPE>(result, sdata[i], rdata, FlatVector::Nullmask(result), i);
			}
		}
	}

	template <class T, class STATE>
	static void Finalize(Vector &result, STATE *state, T *target, nullmask_t &nullmask, idx_t idx) {
		if (!state->isset) {
			nullmask[idx] = true;
		} else {
			if (!Value::DoubleIsValid(state->value)) {
				throw OutOfRangeException("SUM is out of range!");
			}
			target[idx] = state->value;
		}
	}
}; // end UDFSum

}; //end namespace
