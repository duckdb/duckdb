//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/duplicate_eliminated_domain/duplicate_eliminated_domain_builder.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/unique_ptr.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/types.hpp"

namespace duckdb {

class Binder;
class LogicalOperator;

//! Builds a duplicate-free projection of selected columns from a logical source.
class DuplicateEliminatedDomainBuilder {
public:
	static unique_ptr<LogicalOperator> TryBuild(Binder &binder, unique_ptr<LogicalOperator> source,
	                                            const vector<idx_t> &key_indices, const vector<LogicalType> &key_types);
};

} // namespace duckdb
