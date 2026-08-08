//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/vector/for_view.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/enums/expression_type.hpp"
#include "duckdb/common/optional_ptr.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/types/validity_mask.hpp"
#include "duckdb/common/types/value.hpp"

namespace duckdb {

class Vector;

//! ForView: a narrow-payload view of a FOR-vector column-vs-constant comparison.
struct ForView {
	PhysicalType narrow_type = PhysicalType::INVALID;
	const_data_ptr_t data = nullptr;
	int64_t rewritten_constant = 0;
	optional_ptr<const ValidityMask> original_validity;
	bool always_false = false;
	bool always_true = false;
};

//! Returns true for FOR UINT8/16/32/64 payloads, including short-circuit comparisons.
bool TryResolveForView(const Vector &col, ExpressionType op, const Value &constant, ForView &out);

} // namespace duckdb
