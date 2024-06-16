#include "duckdb/core_functions/scalar/generic_functions.hpp"
#include "duckdb/common/vector_operations/generic_executor.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/hugeint.hpp"

namespace duckdb {

hugeint_t GetPreviousPowerOfTen(hugeint_t input) {
	hugeint_t power_of_ten = 1;
	while (power_of_ten < input) {
		power_of_ten *= 10;
	}
	return power_of_ten / 10;
}

hugeint_t MakeNumberNice(hugeint_t input) {
	// we consider numbers nice if they are divisible by 2 or 5 times the power-of-ten one lower than the current
	// e.g. 120 is a nice number because it is divisible by 20
	//      122 is not a nice number -> we make it nice by turning it into 120 [/20]
	//      153 is not a nice number -> we make it nice by turning it into 150 [/50]
	//      1220 is not a nice number -> we turn it into 1200                  [/200]
	// first figure out the previous power of 10 (i.e. for 67 we return 10)
	hugeint_t power_of_ten = GetPreviousPowerOfTen(input);
	// now the power of ten is the power BELOW the current number
	// i.e. for 67, it is not 10
	// now we can get the 2 or 5 divisors
	hugeint_t two = power_of_ten / 5;
	hugeint_t five = power_of_ten / 2;

	// compute the closest round number by adding the divisor / 2 and truncating
	// do this for both divisors
	hugeint_t round_to_two = (input + (two / 2)) / two * two;
	hugeint_t round_to_five = (input + (five / 2)) / five * five;
	// now pick the closest number of the two (i.e. for 147 we pick 150, not 140)
	if (AbsValue(input - round_to_two) < AbsValue(input - round_to_five)) {
		return round_to_two;
	} else {
		return round_to_five;
	}
}

double GetPreviousPowerOfTen(double input) {
	double power_of_ten = 1;
	if (input < 1) {
		while (power_of_ten > input) {
			power_of_ten /= 10;
		}
		return power_of_ten;
	}
	while (power_of_ten < input) {
		power_of_ten *= 10;
	}
	return power_of_ten / 10;
}

double MakeNumberNice(double input) {
	if (input == 0) {
		return 0;
	}
	const double power_of_ten = GetPreviousPowerOfTen(input);
	// now the power of ten is the power BELOW the current number
	// i.e. for 67, it is not 10
	// now we can get the 2 or 5 divisors
	const double two = power_of_ten / 5;
	const double five = power_of_ten / 2;

	const double round_to_two = std::round(input / two) * two;
	const double round_to_five = std::round(input / five) * five;
	if (!Value::IsFinite(round_to_two) || !Value::IsFinite(round_to_five)) {
		return input;
	}
	// now pick the closest number of the two (i.e. for 147 we pick 150, not 140)
	if (AbsValue(input - round_to_two) < AbsValue(input - round_to_five)) {
		return round_to_two;
	} else {
		return round_to_five;
	}
}

struct EquiWidthBinsInteger {
	static constexpr LogicalTypeId LOGICAL_TYPE = LogicalTypeId::BIGINT;

	static vector<PrimitiveType<int64_t>> Operation(int64_t input_min, int64_t input_max, idx_t bin_count,
	                                                bool nice_rounding) {
		vector<PrimitiveType<int64_t>> result;
		// to prevent integer truncation from affecting the bin boundaries we calculate them with numbers multiplied by
		// 1000 we then divide to get the actual boundaries
		const auto FACTOR = hugeint_t(1000);
		auto min = hugeint_t(input_min) * FACTOR;
		auto max = hugeint_t(input_max) * FACTOR;

		const hugeint_t span = max - min;
		hugeint_t step = span / Hugeint::Convert(bin_count);
		if (nice_rounding) {
			// when doing nice rounding we try to make the max/step values nicer
			step = MakeNumberNice(step);
			max = MakeNumberNice(max);
		}

		for (hugeint_t bin_boundary = max; bin_boundary > min; bin_boundary -= step) {
			const hugeint_t target_boundary = Hugeint::Divide(bin_boundary, FACTOR);
			int64_t real_boundary = Hugeint::Cast<int64_t>(target_boundary);
			if (!result.empty()) {
				if (real_boundary <= input_min || result.size() >= bin_count) {
					// we can never generate input_min
					break;
				}
				if (real_boundary == result.back().val) {
					// we cannot generate the same value multiple times in a row - skip this step
					continue;
				}
			}
			result.push_back(real_boundary);
		}
		std::reverse(result.begin(), result.end());
		return result;
	}
};

struct EquiWidthBinsDouble {
	static constexpr LogicalTypeId LOGICAL_TYPE = LogicalTypeId::DOUBLE;

	static vector<PrimitiveType<double>> Operation(double min, double max, idx_t bin_count, bool nice_rounding) {
		if (!Value::IsFinite(min) || !Value::IsFinite(max)) {
			throw InvalidInputException("equi_width_bucket does not support infinite or nan as min/max value");
		}
		vector<PrimitiveType<double>> result;
		const double span = max - min;
		double step;
		if (!Value::IsFinite(span)) {
			// max - min does not fit
			step = max / bin_count - min / bin_count;
		} else {
			step = span / static_cast<double>(bin_count);
		}
		if (nice_rounding) {
			// when doing nice rounding we try to make the max/step values nicer
			step = MakeNumberNice(step);
			max = MakeNumberNice(max);
		}

		const double step_power_of_ten = GetPreviousPowerOfTen(step);
		const double round_multiplication = 10 / step_power_of_ten;
		for (double bin_boundary = max; bin_boundary > min; bin_boundary -= step) {
			// because floating point addition adds inaccuracies, we add rounding at every step
			if (nice_rounding) {
				bin_boundary = std::round(bin_boundary * round_multiplication) / round_multiplication;
			}
			if (bin_boundary <= min || result.size() >= bin_count) {
				// we can never generate input_min
				break;
			}
			result.push_back(bin_boundary);
		}
		std::reverse(result.begin(), result.end());
		return result;
	}
};

unique_ptr<FunctionData> BindEquiWidthFunction(ClientContext &, ScalarFunction &bound_function,
                                                           vector<unique_ptr<Expression>> &arguments) {
	// while internally the bins are computed over a unified type
	// the equi_width_bins function returns the same type as the input MAX
	if (arguments[1]->return_type.id() != LogicalTypeId::UNKNOWN &&
		arguments[1]->return_type.id() != LogicalTypeId::SQLNULL) {
		bound_function.return_type = LogicalType::LIST(arguments[1]->return_type);
	}
	return nullptr;
}

template <class T, class OP>
static void EquiWidthBinFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	static constexpr int64_t MAX_BIN_COUNT = 1000000;
	auto &min_arg = args.data[0];
	auto &max_arg = args.data[1];
	auto &bin_count = args.data[2];
	auto &nice_rounding = args.data[3];

	Vector intermediate_result(LogicalType::LIST(OP::LOGICAL_TYPE));
	GenericExecutor::ExecuteQuaternary<PrimitiveType<T>, PrimitiveType<T>, PrimitiveType<int64_t>, PrimitiveType<bool>,
	                                   GenericListType<PrimitiveType<T>>>(
	    min_arg, max_arg, bin_count, nice_rounding, intermediate_result, args.size(),
	    [&](PrimitiveType<T> min_p, PrimitiveType<T> max_p, PrimitiveType<int64_t> bins_p,
	        PrimitiveType<bool> nice_rounding_p) {
		    if (max_p.val < min_p.val) {
			    throw InvalidInputException("Invalid input for bin function - max value is smaller than min value");
		    }
		    if (bins_p.val <= 0) {
			    throw InvalidInputException("Invalid input for bin function - there must be > 0 bins");
		    }
		    if (bins_p.val > MAX_BIN_COUNT) {
			    throw InvalidInputException("Invalid input for bin function - max bin count of %d exceeded",
			                                MAX_BIN_COUNT);
		    }
		    GenericListType<PrimitiveType<T>> result_bins;
		    if (max_p.val == min_p.val) {
			    // if max = min return a single bucket
			    result_bins.values.push_back(max_p.val);
		    } else {
			    result_bins.values =
			        OP::Operation(min_p.val, max_p.val, static_cast<idx_t>(bins_p.val), nice_rounding_p.val);
		    }
		    return result_bins;
	    });
	VectorOperations::DefaultCast(intermediate_result, result, args.size());
}

ScalarFunctionSet EquiWidthBinsFun::GetFunctions() {
	ScalarFunctionSet functions("equi_width_bins");
	functions.AddFunction(
	    ScalarFunction({LogicalType::BIGINT, LogicalType::BIGINT, LogicalType::BIGINT, LogicalType::BOOLEAN},
	                   LogicalType::LIST(LogicalType::BIGINT), EquiWidthBinFunction<int64_t, EquiWidthBinsInteger>, BindEquiWidthFunction));
	functions.AddFunction(
	    ScalarFunction({LogicalType::DOUBLE, LogicalType::DOUBLE, LogicalType::BIGINT, LogicalType::BOOLEAN},
	                   LogicalType::LIST(LogicalType::DOUBLE), EquiWidthBinFunction<double, EquiWidthBinsDouble>, BindEquiWidthFunction));
	return functions;
}

} // namespace duckdb
