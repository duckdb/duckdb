//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/vector/for_view.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/constants.hpp"
#include "duckdb/common/enums/expression_type.hpp"
#include "duckdb/common/optional_ptr.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/types/validity_mask.hpp"
#include "duckdb/common/types/value.hpp"

namespace duckdb {

class Vector;

//! ForView: a narrow-payload view of a column-vs-constant comparison.
//!
//! Unifies FOR vectors and ordinary FLAT thin-integer vectors as a narrow
//! payload pointer + a rewritten constant in the narrow type's domain (a FLAT
//! narrow column is, conceptually, a FOR view with offset 0). The comparison
//! can then run as `narrow[i] <op> rewritten_constant` for i in [0, count),
//! reading the raw payload directly: the inner loop ignores any selection
//! vector and the original validity. The original validity is captured here
//! and folded in only at the conversion boundary.
struct ForView {
	enum class Kind { NONE, FLAT, FOR };

	Kind kind = Kind::NONE;
	//! Narrow physical type for the inner loop: INT8/16/32 (FLAT signed) or
	//! UINT8/16/32 (FLAT unsigned or FOR).
	PhysicalType narrow_type = PhysicalType::INVALID;
	//! Pointer to the start of the narrow data buffer (count elements wide).
	const_data_ptr_t data = nullptr;
	//! Rewritten constant. Always representable in the narrow type's domain
	//! when !always_false && !always_true. Held as int64_t to cover all thin
	//! types (signed: sign-extended; unsigned: zero-extended).
	int64_t rewritten_constant = 0;
	//! Original validity mask. nullptr if the column has no possible NULLs.
	optional_ptr<const ValidityMask> original_validity;
	//! Short-circuit flags: the kernel is skipped when either is set.
	bool always_false = false;
	bool always_true = false;
};

//! Attempt to construct a ForView for `col <op> constant`.
//!
//! Returns true if the column resolves to a supported FLAT (u)int8/16/32 payload
//! (kind = FLAT) or a FOR vector with UINT8/16/32 stored type (kind = FOR), even
//! if the result short-circuits via `always_false`/`always_true`.
//!
//! Returns false (kind = NONE) for any other vector type, wider integers, or
//! unsupported operators.
bool TryResolveForView(const Vector &col, ExpressionType op, const Value &constant, ForView &out);

} // namespace duckdb
