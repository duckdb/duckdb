//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/expression_executor/in_list_lookup.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/common/types/vector.hpp"

namespace duckdb {

class BoundOperatorExpression;

//! Lookup over the constant values of an IN, probed with the same equality semantics as VectorOperations::Equals
class InListLookup {
public:
	//! Minimum number of values for an IN to be evaluated with a lookup
	static constexpr idx_t MIN_VALUE_COUNT = 5;

public:
	virtual ~InListLookup() = default;

	//! Whether the (NOT) IN has enough constant values of a supported type to use a lookup
	static bool IsSupported(const BoundOperatorExpression &expr);
	//! Builds the lookup from the constant values of a supported (NOT) IN
	static unique_ptr<InListLookup> Create(const BoundOperatorExpression &expr);

	void Probe(const Vector &input, idx_t count, bool negate, Vector &result) const;

protected:
	virtual void Add(const Value &value) = 0;
	virtual void Finalize() = 0;
	virtual void ProbeValues(const Vector &input, idx_t count, bool negate, Vector &result) const = 0;

protected:
	bool has_null = false;
};

} // namespace duckdb
