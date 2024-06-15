#include "duckdb/core_functions/scalar/generic_functions.hpp"
#include "duckdb/common/vector_operations/generic_executor.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/hugeint.hpp"

namespace duckdb {

hugeint_t MakeNumberNice(hugeint_t input) {
	// we consider numbers nice if they are divisible by 2 or 5 times the power-of-ten one lower than the current
	// e.g. 120 is a nice number because it is divisible by 20
	//      122 is not a nice number -> we make it nice by turning it into 120 [/20]
	//      153 is not a nice number -> we make it nice by turning it into 150 [/50]
	//      1220 is not a nice number -> we turn it into 1200                  [/200]
	// first figure out the current power of 10
	hugeint_t power_of_ten = 1;
	while(power_of_ten < input) {
		power_of_ten *= 10;
	}
	if (power_of_ten <= 10) {
		// number is too small to make it nice
		return input;
	}
	// power of ten is now the power of ten ABOVE the current number
	// i.e. if the number is 67, power_of_ten is 100
	// divide by 10 to get the power_of_ten below the number
	power_of_ten /= 10;
	// now the power of ten is the power BELOW the current number
	// i.e. for 67, it is not 10
	// now we can get the 2 or 5 divisors
	hugeint_t two = power_of_ten / 5;
	hugeint_t five = power_of_ten / 2;

	// there's probably a better way to do this - but this works
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

static vector<PrimitiveType<int64_t>> EquiWidthBins(int64_t input_min, int64_t input_max, int64_t bin_count, bool nice_rounding) {
	if (input_max < input_min) {
		throw InvalidInputException("Invalid input for bin function - max value %lld is smaller than min value %lld", input_max, input_min);
	}
	if (bin_count <= 0) {
		throw InvalidInputException("Invalid input for bin function - there must be > 0 bins");
	}
	vector<PrimitiveType<int64_t>> result;
	if (input_max == input_min) {
		// if max = min return a single bucket
		result.push_back(input_max);
		return result;
	}
	// to prevent integer truncation from affecting the bin boundaries we calculate them with numbers multiplied by 1000
	// we then divide to get the actual boundaries
	const auto FACTOR = hugeint_t(1000);
	auto min = hugeint_t(input_min) * FACTOR;
	auto max = hugeint_t(input_max) * FACTOR;

	const hugeint_t span = max - min;
	hugeint_t step = span / hugeint_t(bin_count);
	if (nice_rounding) {
		// when doing nice rounding we try to make the max/step values nicer
		step = MakeNumberNice(step);
		max = MakeNumberNice(max);
	}

	for(hugeint_t bin_boundary = max; bin_boundary > min; bin_boundary -= step) {
		const hugeint_t target_boundary = Hugeint::Divide(bin_boundary, FACTOR);
		int64_t real_boundary = Hugeint::Cast<int64_t>(target_boundary);
		if (!result.empty()) {
			if (real_boundary <= input_min) {
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

static void EquiWidthBinFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &min_arg = args.data[0];
	auto &max_arg = args.data[1];
	auto &bin_count = args.data[2];
	auto &nice_rounding = args.data[3];

	GenericExecutor::ExecuteQuaternary<PrimitiveType<int64_t>, PrimitiveType<int64_t>, PrimitiveType<int64_t>, PrimitiveType<bool>,
	                                GenericListType<PrimitiveType<int64_t>>>(
	    min_arg, max_arg, bin_count, nice_rounding, result, args.size(),
	    [&](PrimitiveType<int64_t> min_p, PrimitiveType<int64_t> max_p, PrimitiveType<int64_t> bins_p, PrimitiveType<bool> nice_rounding_p) {
		    GenericListType<PrimitiveType<int64_t>> result_bins;
		    result_bins.values = EquiWidthBins(min_p.val, max_p.val, bins_p.val, nice_rounding_p.val);
		    return result_bins;
	    });
}

ScalarFunctionSet EquiWidthBinsFun::GetFunctions() {
	ScalarFunctionSet functions("equi_width_bins");
	functions.AddFunction(ScalarFunction({LogicalType::BIGINT, LogicalType::BIGINT, LogicalType::BIGINT, LogicalType::BOOLEAN},
	                                     LogicalType::LIST(LogicalType::BIGINT), EquiWidthBinFunction));
	return functions;
}

} // namespace duckdb
