//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/row_operations/row_matcher.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/enums/expression_type.hpp"
#include "duckdb/common/types.hpp"

namespace duckdb {

class Vector;
class DataChunk;
class TupleDataLayout;
struct TupleDataVectorFormat;
struct SelectionVector;
struct MatchFunction;

typedef idx_t (*match_function_t)(Vector &lhs_vector, const TupleDataVectorFormat &lhs_format, SelectionVector &sel,
                                  const idx_t count, const TupleDataLayout &rhs_layout, Vector &rhs_row_locations,
                                  const idx_t col_idx, const vector<MatchFunction> &child_functions,
                                  SelectionVector *no_match_sel, idx_t &no_match_count);

struct MatchFunction {
	match_function_t function;
	vector<MatchFunction> child_functions;
};

struct RowMatcher {
public:
	using Predicates = vector<ExpressionType>;

	//! Initializes the RowMatcher, filling match_functions using layout and equality_predicates
	//! If columns is empty, we assume column indices 0, 1, 2, etc.
	void Initialize(const bool no_match_sel, const TupleDataLayout &layout, const Predicates &predicates,
	                vector<column_t> columns = {});

public:
	//! Given a DataChunk on the LHS, on which we've called TupleDataCollection::ToUnifiedFormat,
	//! we match it with rows on the RHS, according to the given layout and locations.
	//! Initially, 'sel' has 'count' entries which point to what needs to be compared.
	//! After matching is done, this returns how many matching entries there are, which 'sel' is modified to point to
	idx_t Match(DataChunk &lhs, const vector<TupleDataVectorFormat> &lhs_formats, SelectionVector &sel, idx_t count,
	            Vector &rhs_row_locations, SelectionVector *no_match_sel, idx_t &no_match_count);

private:
	//! Gets the templated match function for a given column
	MatchFunction GetMatchFunction(const bool no_match_sel, const LogicalType &type, const ExpressionType predicate);
	template <bool NO_MATCH_SEL>
	MatchFunction GetMatchFunction(const LogicalType &type, const ExpressionType predicate);
	template <bool NO_MATCH_SEL, class T>
	MatchFunction GetMatchFunction(const ExpressionType predicate);
	template <bool NO_MATCH_SEL>
	MatchFunction GetStructMatchFunction(const LogicalType &type, const ExpressionType predicate);
	template <bool NO_MATCH_SEL>
	MatchFunction GetListMatchFunction(const ExpressionType predicate);

private:
	optional_ptr<const TupleDataLayout> rhs_layout;
	vector<column_t> columns;
	vector<MatchFunction> match_functions;
	vector<LogicalType> rhs_types;
};

} // namespace duckdb
