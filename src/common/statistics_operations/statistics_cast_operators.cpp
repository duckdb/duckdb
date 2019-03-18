#include "common/exception.hpp"
#include "common/types/statistics.hpp"
#include "common/value_operations/value_operations.hpp"
#include "parser/parsed_expression.hpp"

using namespace duckdb;
using namespace std;

void ExpressionStatistics::Cast(ExpressionStatistics &source, ExpressionStatistics &result) {
	result.has_stats = source.has_stats;
	if (source.has_stats) {
		result.can_have_null = source.can_have_null;
		if (TypeIsNumeric(source.expression.return_type) && TypeIsNumeric(result.expression.return_type)) {
			// both are numeric, preserve numeric stats
			result.min = source.min;
			result.max = source.max;
		} else if (result.expression.return_type == TypeId::VARCHAR) {
			// we are casting to string, check if we can know the maximum string
			// length
			switch (source.expression.return_type) {
			case TypeId::BOOLEAN:
				// false
				result.maximum_string_length = 5;
				break;
			case TypeId::TINYINT:
				// -127
				result.maximum_string_length = 4;
				break;
			case TypeId::SMALLINT:
				// -32768
				result.maximum_string_length = 6;
				break;
			case TypeId::INTEGER:
				// -2147483647
				result.maximum_string_length = 11;
				break;
			case TypeId::BIGINT:
				// -9223372036854775808
				result.maximum_string_length = 20;
				break;
			case TypeId::POINTER:
				// 18446744073709552000
				result.maximum_string_length = 20;
				break;
			// case TypeId::DATE:
			// 	// we use days since 1900 as 32-bit integer, so the maximum year
			// 	// is 1900+2^31/365= the biggest string would therefore be e.g.
			// 	// 01-01-11768933 which has length 14
			// 	result.maximum_string_length = 14;
			// 	break;
			default:
				result.has_stats = false;
				break;
			}
			result.min = Value();
			result.max = Value();
		} else {
			// we might be casting from string, we don't know anything about the
			// result
			result.has_stats = false;
		}
	}
}
