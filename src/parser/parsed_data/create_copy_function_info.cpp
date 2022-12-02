#include "duckdb/parser/parsed_data/create_copy_function_info.hpp"

namespace duckdb {

CreateCopyFunctionInfo::CreateCopyFunctionInfo(CopyFunction function)
	: CreateInfo(CatalogType::COPY_FUNCTION_ENTRY), function(function) {
	this->name = function.name;
	internal = true;
}

void CreateCopyFunctionInfo::SerializeInternal(Serializer &) const {
	throw NotImplementedException("Cannot serialize '%s'", CatalogTypeToString(type));
}

unique_ptr<CreateInfo> CreateCopyFunctionInfo::Copy() const {
	auto result = make_unique<CreateCopyFunctionInfo>(function);
	CopyProperties(*result);
	return move(result);
}

}
