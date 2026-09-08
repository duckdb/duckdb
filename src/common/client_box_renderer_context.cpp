#include "duckdb/common/box_renderer_context.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"

namespace duckdb {

ClientBoxRendererContext::ClientBoxRendererContext(ClientContext &context) : context(context) {
}

bool ClientBoxRendererContext::IsInterrupted() const {
	return context.IsInterrupted();
}

void ClientBoxRendererContext::CastToVarchar(DataChunk &source, DataChunk &result, idx_t count) {
	for (idx_t c = 0; c < source.ColumnCount(); c++) {
		VectorOperations::TryCast(context, source.data[c], result.data[c], count, nullptr, false);
	}
}

Allocator &ClientBoxRendererContext::GetAllocator() {
	return Allocator::Get(context);
}

} // namespace duckdb
