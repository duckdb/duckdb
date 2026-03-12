#include "duckdb/common/box_renderer_context.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"

namespace duckdb {

ClientBoxRendererContext::ClientBoxRendererContext(ClientContext &context) : context(context) {
}

bool ClientBoxRendererContext::IsInterrupted() const {
	return context.IsInterrupted();
}

void ClientBoxRendererContext::Cast(Vector &source, Vector &result, idx_t count, bool strict) {
	VectorOperations::TryCast(context, source, result, count, nullptr, strict);
}

Allocator &ClientBoxRendererContext::GetAllocator() {
	return Allocator::Get(context);
}

} // namespace duckdb
