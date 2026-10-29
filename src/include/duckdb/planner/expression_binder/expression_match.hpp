//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/expression_binder/expression_match.hpp
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/optional_idx.hpp"
#include "duckdb/common/shared_ptr.hpp"
#include "duckdb/parser/expression_map.hpp"

namespace duckdb {
class Binder;
class CaseCompatibilityKey;

//! An original expression and the immutable matching facts established by binding it.
class ExpressionMatchDescriptor {
public:
	explicit ExpressionMatchDescriptor(unique_ptr<ParsedExpression> expression);
	~ExpressionMatchDescriptor();

public:
	static unique_ptr<ParsedExpression> CreateSnapshot(Binder &binder, ParsedExpression &expression);
	const ParsedExpression &OriginalExpression() const;
	void ApplyBindingFacts(const ParsedExpression &expression) const;

private:
	friend class ExpressionMatchIndex;

	unique_ptr<ParsedExpression> original_expression;
	unique_ptr<CaseCompatibilityKey> compatibility_key;
};

enum class ExpressionMatchPolicy : uint8_t { FIRST, REPLACE, AMBIGUOUS };

class ExpressionMatch {
public:
	ExpressionMatch() = default;

	bool IsValid() const {
		return index.IsValid();
	}
	bool IsAmbiguous() const {
		return ambiguous;
	}
	idx_t GetIndex() const {
		return index.GetIndex();
	}

	shared_ptr<const ExpressionMatchDescriptor> descriptor;

private:
	friend class ExpressionMatchIndex;

	ExpressionMatch(idx_t index, shared_ptr<const ExpressionMatchDescriptor> descriptor);
	static ExpressionMatch Ambiguous();

	optional_idx index;
	bool ambiguous = false;
};

//! Exact and compatible lookup with context-specific duplicate handling.
class ExpressionMatchIndex {
public:
	ExpressionMatchIndex(ExpressionMatchPolicy exact_policy, ExpressionMatchPolicy compatible_policy);

public:
	//! Borrowed provisional expressions must remain alive and unchanged until lookup is complete.
	void AddExact(const ParsedExpression &expression, idx_t index);
	void AddExact(unique_ptr<ParsedExpression> expression, idx_t index);
	void Add(shared_ptr<const ExpressionMatchDescriptor> expression, idx_t index);
	ExpressionMatch Find(const ParsedExpression &expression) const;
	bool Empty() const {
		return exact_entries.empty();
	}

private:
	void AddExact(const ParsedExpression &expression, ExpressionMatch match);

private:
	ExpressionMatchPolicy exact_policy;
	ExpressionMatchPolicy compatible_policy;
	vector<shared_ptr<const ExpressionMatchDescriptor>> descriptors;
	vector<unique_ptr<ParsedExpression>> provisional_expressions;
	parsed_expression_map_t<ExpressionMatch> exact_entries;
	parsed_expression_map_t<vector<ExpressionMatch>> compatible_entries;
};
} // namespace duckdb
