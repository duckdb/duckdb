//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/box_renderer_context.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/constants.hpp"

namespace duckdb {
class Allocator;
class ClientContext;
class DataChunk;
class Vector;

//! BoxRendererContext provides the minimal interface needed by the BoxRenderer
//! for allocating memory, casting vectors, and checking for interrupts.
class BoxRendererContext {
public:
	virtual ~BoxRendererContext() = default;

	//! Whether execution has been interrupted
	virtual bool IsInterrupted() const = 0;

	//! Get the allocator for temporary data
	virtual Allocator &GetAllocator() = 0;

	//! Cast a single source vector to VARCHAR. Delegates to the chunk-level virtual.
	void CastToVarchar(Vector &source, Vector &result, idx_t count);

protected:
	//! Cast source chunk columns to VARCHAR into result chunk. Derived classes implement this.
	virtual void CastToVarchar(DataChunk &source, DataChunk &result, idx_t count) = 0;
};

//! ClientBoxRendererContext wraps a ClientContext to implement BoxRendererContext.
class ClientBoxRendererContext : public BoxRendererContext {
public:
	explicit ClientBoxRendererContext(ClientContext &context);

	bool IsInterrupted() const override;
	Allocator &GetAllocator() override;

protected:
	void CastToVarchar(DataChunk &source, DataChunk &result, idx_t count) override;

private:
	ClientContext &context;
};

} // namespace duckdb
