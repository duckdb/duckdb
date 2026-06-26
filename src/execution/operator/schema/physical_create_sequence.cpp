#include "duckdb/execution/operator/schema/physical_create_sequence.hpp"
#include "duckdb/catalog/catalog.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// Source
//===--------------------------------------------------------------------===//
SourceResultType PhysicalCreateSequence::GetDataInternal(ExecutionContext &context, DataChunk &chunk,
                                                         OperatorSourceInput &input) const {
	auto &catalog = Catalog::GetCatalog(context.client, info->GetQualifiedName().Catalog());
	catalog.CreateSequence(context.client, *info);

	return SourceResultType::FINISHED;
}

} // namespace duckdb
