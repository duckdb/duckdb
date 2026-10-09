//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/in_value_set.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/common/types/vector.hpp"

namespace duckdb {

class BoundOperatorExpression;

//! The constant values of an IN, probed with the same equality semantics as VectorOperations::Equals
class InValueSet {
public:
	//! Minimum number of values for an IN to be evaluated with a lookup set
	static constexpr idx_t MIN_VALUE_COUNT = 5;

public:
	virtual ~InValueSet() = default;

	//! Whether the (NOT) IN has enough constant values of a supported type to use a lookup set
	static bool IsSupported(const BoundOperatorExpression &expr);
	//! Builds the lookup set from the constant values of a supported (NOT) IN
	static unique_ptr<InValueSet> Create(const BoundOperatorExpression &expr);

	void Probe(const Vector &input, idx_t count, bool negate, Vector &result) const;

protected:
	virtual void Add(const Value &value) = 0;
	virtual void Finalize() = 0;
	virtual void ProbeValues(const Vector &input, idx_t count, bool negate, Vector &result) const = 0;

protected:
	bool has_null = false;
};

} // namespace duckdb
