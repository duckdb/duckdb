#include "duckdb/function/scalar/trigonometric_functions.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/common/exception.hpp"

#include <algorithm>
#include <cmath>

using namespace std;

namespace duckdb {

//===--------------------------------------------------------------------===//
// sin
//===--------------------------------------------------------------------===//
struct SinOperator {
	template <class TA, class TR> static inline TR Operation(TA input) {
		return sin(input);
	}
};

void SinFun::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction(ScalarFunction("sin", {SQLType::DOUBLE}, SQLType::DOUBLE,
	                               ScalarFunction::UnaryFunction<double, double, SinOperator>));
}

//===--------------------------------------------------------------------===//
// cos
//===--------------------------------------------------------------------===//
struct CosOperator {
	template <class TA, class TR> static inline TR Operation(TA input) {
		return (double)cos(input);
	}
};

void CosFun::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction(ScalarFunction("cos", {SQLType::DOUBLE}, SQLType::DOUBLE,
	                               ScalarFunction::UnaryFunction<double, double, CosOperator>));
}

//===--------------------------------------------------------------------===//
// tan
//===--------------------------------------------------------------------===//
struct TanOperator {
	template <class TA, class TR> static inline TR Operation(TA input) {
		return (double)tan(input);
	}
};

void TanFun::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction(ScalarFunction("tan", {SQLType::DOUBLE}, SQLType::DOUBLE,
	                               ScalarFunction::UnaryFunction<double, double, TanOperator>));
}

//===--------------------------------------------------------------------===//
// asin
//===--------------------------------------------------------------------===//
struct ASinOperator {
	template <class TA, class TR> static inline TR Operation(TA input) {
		if (input < -1 || input > 1) {
			throw Exception("ASIN is undefined outside [-1,1]");
		}
		return (double)asin(input);
	}
};

void AsinFun::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction(ScalarFunction("asin", {SQLType::DOUBLE}, SQLType::DOUBLE,
	                               ScalarFunction::UnaryFunction<double, double, ASinOperator>));
}

//===--------------------------------------------------------------------===//
// atan
//===--------------------------------------------------------------------===//
struct ATanOperator {
	template <class TA, class TR> static inline TR Operation(TA input) {
		return (double)atan(input);
	}
};

void AtanFun::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction(ScalarFunction("atan", {SQLType::DOUBLE}, SQLType::DOUBLE,
	                               ScalarFunction::UnaryFunction<double, double, ATanOperator>));
}

//===--------------------------------------------------------------------===//
// atan2
//===--------------------------------------------------------------------===//
struct ATan2 {
	template <class TA, class TB, class TR> static inline TR Operation(TA left, TB right) {
		return (double)atan2(left, right);
	}
};

void Atan2Fun::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction(ScalarFunction("atan2", {SQLType::DOUBLE, SQLType::DOUBLE}, SQLType::DOUBLE,
	                               ScalarFunction::BinaryFunction<double, double, double, ATan2>));
}

//===--------------------------------------------------------------------===//
// acos
//===--------------------------------------------------------------------===//
struct ACos {
	template <class TA, class TR> static inline TR Operation(TA input) {
		if (input < -1 || input > 1) {
			throw Exception("ACOS is undefined outside [-1,1]");
		}
		return (double)acos(input);
	}
};

void AcosFun::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction(ScalarFunction("acos", {SQLType::DOUBLE}, SQLType::DOUBLE,
	                               ScalarFunction::UnaryFunction<double, double, ACos>));
}

//===--------------------------------------------------------------------===//
// cot
//===--------------------------------------------------------------------===//
struct CotOperator {
	template <class TA, class TR> static inline TR Operation(TA input) {
		return 1.0 / (double)tan(input);
	}
};

void CotFun::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction(ScalarFunction("cot", {SQLType::DOUBLE}, SQLType::DOUBLE,
	                               ScalarFunction::UnaryFunction<double, double, CotOperator>));
}

} // namespace duckdb
