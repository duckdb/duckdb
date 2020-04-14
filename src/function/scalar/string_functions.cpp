#include "duckdb/function/scalar/string_functions.hpp"

using namespace std;

namespace duckdb {

void BuiltinFunctions::RegisterStringFunctions() {
	Register<ReverseFun>();
	Register<LowerFun>();
	Register<UpperFun>();
	Register<ConcatFun>();
	Register<LengthFun>();
	Register<LikeFun>();
	Register<PrintfFun>();
	Register<RegexpFun>();
	Register<SubstringFun>();
	Register<InstrFun>();
	Register<PrefixFun>();
	Register<SuffixFun>();
    Register<ContainsFun>();
}

string StringFunctionsTypeToString(StringFunctionsType type) {
    switch (type) {
		case StringFunctionsType::REVERSE:
			return "REVERSE";
		case StringFunctionsType::LOWER:
			return "LOWER";
		case StringFunctionsType::UPPER:
			return "UPPER";
		case StringFunctionsType::CONCAT:
			return "CONCAT";
		case StringFunctionsType::LENGTH:
			return "LENGTH";
		case StringFunctionsType::LIKE:
			return "LIKE";
		case StringFunctionsType::REGEXP:
			return "REGEXP";
		case StringFunctionsType::SUBSTRING:
			return "SUBSTRING";
		case StringFunctionsType::PRINTF:
			return "PRINTF";
		case StringFunctionsType::INSTR:
			return "INSTR";
		case StringFunctionsType::PREFIX:
			return "PREFIX";
		case StringFunctionsType::SUFFIX:
			return "SUFFIX";
		case StringFunctionsType::CONTAINS:
			return "CONTAINS";
		default:
			return "INVALID";
    }
}

} // namespace duckdb
