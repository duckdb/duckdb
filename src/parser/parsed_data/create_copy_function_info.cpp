#include "duckdb/parser/parsed_data/create_copy_function_info.hpp"

namespace duckdb {

CreateCopyFunctionInfo::CreateCopyFunctionInfo(CopyFunction function_p)
    : CreateInfo(CatalogType::COPY_FUNCTION_ENTRY), function(std::move(function_p)) {
	this->name = function.name;
	internal = true;
}

void CreateCopyFunctionInfo::SerializeInternal(Serializer &) const {
	throw NotImplementedException("Cannot serialize '%s'", CatalogTypeToString(type));
}

unique_ptr<CreateInfo> CreateCopyFunctionInfo::Copy() const {
	auto result = make_unique<CreateCopyFunctionInfo>(function);
	CopyProperties(*result);
	return std::move(result);
}

} // namespace duckdb
