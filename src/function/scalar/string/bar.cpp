#include "duckdb/common/exception.hpp"
#include "duckdb/common/operator/cast_operators.hpp"
#include "duckdb/common/types/string_type.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/common/vector_operations/quaternary_executor.hpp"
#include "duckdb/common/vector_operations/ternary_executor.hpp"
#include "duckdb/function/scalar/string_functions.hpp"

namespace duckdb {

static string_t BarScalarFunction(double x, double min, double max, double max_width, vector<char> &result) {
	// U+2588 "█" full
	constexpr static char FULL_BAR[] = "█";
	// U+258F "▏" one eighth
	// U+258E "▎" one quarter
	// U+258D "▍" three eighths
	// U+258C "▌" half
	// U+258B "▋" five eighths
	// U+258A "▊" three quarters
	// U+2589 "▉" seven eighths
	// 7 elements: 1/8, 2/8, 3/8, 4/8, 5/8, 6/8, 7/8
	constexpr static char FRACTIONAL_BARS[] = "▏▎▍▌▋▊▉";
	constexpr static idx_t GRADES_IN_FULL_BAR = 8;
	static const idx_t UNICODE_BAR_CHAR_SIZE = strlen("█");

	if (!Value::IsFinite(max_width)) {
		throw ValueOutOfRangeException("Max bar width must not be NaN or infinity");
	}
	if (max_width < 1) {
		throw ValueOutOfRangeException("Max bar width must be >= 1");
	}
	if (max_width > 1000) {
		throw ValueOutOfRangeException("Max bar width must be <= 1000");
	}

	double width;

	if (Value::IsNan(x) || Value::IsNan(min) || Value::IsNan(max) || x <= min) {
		width = 0;
	} else if (x >= max) {
		width = max_width;
	} else {
		width = max_width * (x - min) / (max - min);
	}

	if (!Value::IsFinite(width)) {
		throw ValueOutOfRangeException("Bar width must not be NaN or infinity");
	}

	result.clear();

	int64_t width_as_int = static_cast<int64_t>(width * GRADES_IN_FULL_BAR);
	idx_t full_bar_width = (width_as_int / GRADES_IN_FULL_BAR);
	for (idx_t i = 0; i < full_bar_width; i++) {
		result.insert(result.end(), FULL_BAR, FULL_BAR + UNICODE_BAR_CHAR_SIZE);
	}

	idx_t remaining = width_as_int % GRADES_IN_FULL_BAR;

	if (remaining) {
		result.insert(result.end(), FRACTIONAL_BARS + (remaining - 1) * UNICODE_BAR_CHAR_SIZE,
		              FRACTIONAL_BARS + remaining * UNICODE_BAR_CHAR_SIZE);
	}

	return string_t(result.data(), result.size());
}

static void BarFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	D_ASSERT(args.ColumnCount() == 3 || args.ColumnCount() == 4);
	auto &x_arg = args.data[0];
	auto &min_arg = args.data[1];
	auto &max_arg = args.data[2];
	vector<char> buffer;

	if (args.ColumnCount() == 3) {
		TernaryExecutor::Execute<double, double, double, string_t>(
		    x_arg, min_arg, max_arg, result, args.size(), [&](double x, double min, double max) {
			    return StringVector::AddString(result, BarScalarFunction(x, min, max, 80, buffer));
		    });
	} else {
		auto &width_arg = args.data[3];
		QuaternaryExecutor::Execute<double, double, double, double, string_t>(
		    x_arg, min_arg, max_arg, width_arg, result, args.size(),
		    [&](double x, double min, double max, double width) {
			    return StringVector::AddString(result, BarScalarFunction(x, min, max, width, buffer));
		    });
	}
}

void BarFun::RegisterFunction(BuiltinFunctions &set) {
	ScalarFunctionSet bar("bar");
	bar.AddFunction(ScalarFunction({LogicalType::DOUBLE, LogicalType::DOUBLE, LogicalType::DOUBLE, LogicalType::DOUBLE},
	                               LogicalType::VARCHAR, BarFunction));
	bar.AddFunction(ScalarFunction({LogicalType::DOUBLE, LogicalType::DOUBLE, LogicalType::DOUBLE},
	                               LogicalType::VARCHAR, BarFunction));
	set.AddFunction(bar);
}

} // namespace duckdb
