#pragma once

#include "duckdb/function/cast/variant/to_variant_fwd.hpp"
#include "duckdb/function/function.hpp"

namespace duckdb {
namespace variant {

//! Converts vectors of a given type to VARIANT, shredding the values using the source type where possible.
//! The shredder owns the shredded data that the produced VARIANT vectors refer to, so it must outlive them.
class DUCKDB_API VariantShredder {
public:
	VariantShredder(const LogicalType &source_type, Allocator &allocator);
	~VariantShredder();

public:
	//! Convert 'source' (of the type the shredder was constructed with) into 'result' (type VARIANT)
	bool Convert(Vector &source, Vector &result, idx_t count);
	//! A reusable vector of the source type to write the values that are shredded into the VARIANT
	Vector &GetScratchVector();
	//! Whether 'Convert' produces a shredded VARIANT rather than an unshredded one
	bool SupportsShredding() const {
		return supports_shredded_cast;
	}
	//! The type that 'source' must have
	const LogicalType &GetSourceType() const {
		return source_type;
	}

private:
	//! The type that 'source' must have
	LogicalType source_type;
	//! The scratch vector returned by GetScratchVector
	unique_ptr<Vector> scratch;
	//! The cast local state - it owns the shredded data the produced VARIANT vectors refer to
	unique_ptr<FunctionLocalState> local_state;
	//! Whether the source type can be emitted as a shredded VARIANT
	bool supports_shredded_cast;
};

} // namespace variant
} // namespace duckdb
