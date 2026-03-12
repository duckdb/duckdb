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
class Vector;

//! BoxRendererContext provides the minimal interface needed by the BoxRenderer
//! for allocating memory, casting vectors, and checking for interrupts.
class BoxRendererContext {
public:
	virtual ~BoxRendererContext() = default;

	//! Whether execution has been interrupted
	virtual bool IsInterrupted() const = 0;

	//! Cast source vector into result vector
	virtual void Cast(Vector &source, Vector &result, idx_t count, bool strict = false) = 0;

	//! Get the allocator for temporary data
	virtual Allocator &GetAllocator() = 0;
};

//! ClientBoxRendererContext wraps a ClientContext to implement BoxRendererContext.
class ClientBoxRendererContext : public BoxRendererContext {
public:
	explicit ClientBoxRendererContext(ClientContext &context);

	bool IsInterrupted() const override;
	void Cast(Vector &source, Vector &result, idx_t count, bool strict = false) override;
	Allocator &GetAllocator() override;

private:
	ClientContext &context;
};

} // namespace duckdb
