#include "duckdb/function/function.hpp"
#include "duckdb/function/scalar/statistical_functions.hpp"

#include <cmath>

namespace duckdb {

// TODOs: stat_norm_pdf, stat_norm_cdf, stat_norm_random, stat_norm_quantile

struct StatNormPDFOperator {
	template<class TA, class TB, class TC, class TR>
	static inline TR Operation(TA x, TB mean, TC sd) {
		// taken from boost.math
		// TODO: domain boundries, edge conditions
		TR exponent = x - mean;
		exponent *= -exponent;
		exponent /= 2 * sd * sd;

		TR result = exp(exponent);
		result /= sd * sqrt(2 * M_PI);

		return result;
	}
};

static void StatNormPDFFunction(DataChunk &args, ExpressionState &state, Vector &result){
	auto &x = args.data[0];
	auto &center = args.data[1];
	auto &scale = args.data[2];

 	// TODO: this will probably frequently be called with
	//       constants for center and scale. We should
	//       also make the unary operator for (x, CONST, CONST)
	//       and the binary operator for (x, center, CONST)

	//       maybe we should also have defaults for center and scale

	TernaryExecutor::Execute<double, double, double, double>( x, center, scale, result, args.size(),
	                                                         StatNormPDFOperator::Operation<double, double, double, double>);


}

void StatisticalNormalPDFFun::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction(ScalarFunction("stat_norm",
	                                   {LogicalType::DOUBLE, LogicalType::DOUBLE, LogicalType::DOUBLE}, // input
	                                   LogicalType::DOUBLE,
	                                   StatNormPDFFunction));
}


} // namespace duckdb