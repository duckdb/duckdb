#include "function_list.hpp"
#include "scalar/bit_functions.hpp"
#include "scalar/blob_functions.hpp"

namespace duckdb {

#define DUCKDB_SCALAR_FUNCTION(_PARAM)                                                                                 \
	{ _PARAM::Name, _PARAM::Parameters, _PARAM::Description, _PARAM::Example, _PARAM::GetFunction, nullptr }
#define DUCKDB_SCALAR_FUNCTION_ALIAS(_PARAM)                                                                           \
	{                                                                                                                  \
		_PARAM::Name, _PARAM::ALIAS::Parameters, _PARAM::ALIAS::Description, _PARAM::ALIAS::Example,                   \
		    _PARAM::ALIAS::GetFunction, nullptr                                                                        \
	}
#define FINAL_FUNCTION                                                                                                 \
	{ nullptr, nullptr, nullptr, nullptr, nullptr, nullptr }

static StaticFunctionDefinition internal_functions[] = {
    DUCKDB_SCALAR_FUNCTION(GetBitFun),      DUCKDB_SCALAR_FUNCTION(SetBitFun),
    DUCKDB_SCALAR_FUNCTION(BitPositionFun), DUCKDB_SCALAR_FUNCTION(BitStringFun),
    DUCKDB_SCALAR_FUNCTION(ToBase64Fun),    DUCKDB_SCALAR_FUNCTION_ALIAS(Base64Fun),
    DUCKDB_SCALAR_FUNCTION(FromBase64Fun),  DUCKDB_SCALAR_FUNCTION(EncodeFun),
    DUCKDB_SCALAR_FUNCTION(DecodeFun),      FINAL_FUNCTION};

StaticFunctionDefinition *StaticFunctionDefinition::GetFunctionList() {
	return internal_functions;
}

} // namespace duckdb
