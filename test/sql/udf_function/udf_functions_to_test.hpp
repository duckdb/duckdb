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
	result.vector_type = VectorType::FLAT_VECTOR;
	auto result_data = FlatVector::GetData<TYPE>(result);
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

}; //end namespace
