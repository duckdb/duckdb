#include "duckdb/planner/expression_binder/expression_match.hpp"
#include "duckdb/parser/expression/case_expression.hpp"
#include "duckdb/parser/expression/subquery_expression.hpp"
#include "duckdb/parser/parsed_expression_iterator.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/expression_binder/case_compatibility.hpp"

namespace duckdb {

static bool ContainsCase(const ParsedExpression &expression) {
	if (expression.GetExpressionClass() == ExpressionClass::CASE) {
		return true;
	}
	if (expression.GetExpressionClass() == ExpressionClass::SUBQUERY) {
		auto &query = expression.Cast<SubqueryExpression>().Subquery();
		bool found = false;
		if (query) {
			ParsedExpressionIterator::EnumerateQueryNodeChildren(
			    *query->node, [&](unique_ptr<ParsedExpression> &child) { found |= ContainsCase(*child); });
		}
		if (found) {
			return true;
		}
	}
	for (auto &child : expression.Children()) {
		if (ContainsCase(child)) {
			return true;
		}
	}
	return false;
}

ExpressionMatchDescriptor::ExpressionMatchDescriptor(unique_ptr<ParsedExpression> expression)
    : original_expression(std::move(expression)) {
	D_ASSERT(original_expression);
	if (ContainsCase(*original_expression) && !CaseExpression::ContainsLambdaReference(*original_expression)) {
		compatibility_key = make_uniq<CaseCompatibilityKey>(*original_expression);
	}
}

ExpressionMatchDescriptor::~ExpressionMatchDescriptor() = default;

unique_ptr<ParsedExpression> ExpressionMatchDescriptor::CreateSnapshot(Binder &binder, ParsedExpression &expression) {
	binder.GetCaseCompatibility().Prepare(expression);
	return expression.Copy();
}

const ParsedExpression &ExpressionMatchDescriptor::OriginalExpression() const {
	return *original_expression;
}

void ExpressionMatchDescriptor::ApplyBindingFacts(const ParsedExpression &expression) const {
	if (compatibility_key) {
		compatibility_key->TransferTo(expression);
	}
}

ExpressionMatch::ExpressionMatch(idx_t index_p, shared_ptr<const ExpressionMatchDescriptor> descriptor_p)
    : descriptor(std::move(descriptor_p)), index(index_p) {
}

ExpressionMatch ExpressionMatch::Ambiguous() {
	ExpressionMatch result;
	result.ambiguous = true;
	return result;
}

ExpressionMatchIndex::ExpressionMatchIndex(ExpressionMatchPolicy exact_policy_p,
                                           ExpressionMatchPolicy compatible_policy_p)
    : exact_policy(exact_policy_p), compatible_policy(compatible_policy_p) {
}

void ExpressionMatchIndex::AddExact(const ParsedExpression &expression, ExpressionMatch match) {
	auto result = exact_entries.emplace(expression, match);
	if (result.second) {
		return;
	}
	auto &existing = result.first->second;
	if (exact_policy == ExpressionMatchPolicy::REPLACE) {
		existing = std::move(match);
	} else if (!existing.IsAmbiguous() && existing.GetIndex() == match.GetIndex()) {
		if (match.descriptor) {
			existing = std::move(match);
		}
	} else if (exact_policy == ExpressionMatchPolicy::AMBIGUOUS) {
		existing = ExpressionMatch::Ambiguous();
	}
}

void ExpressionMatchIndex::AddExact(const ParsedExpression &expression, idx_t index) {
	AddExact(expression, ExpressionMatch(index, nullptr));
}

void ExpressionMatchIndex::AddExact(unique_ptr<ParsedExpression> expression, idx_t index) {
	D_ASSERT(expression);
	AddExact(*expression, index);
	provisional_expressions.push_back(std::move(expression));
}

void ExpressionMatchIndex::Add(shared_ptr<const ExpressionMatchDescriptor> expression, idx_t index) {
	D_ASSERT(expression);
	descriptors.push_back(expression);
	ExpressionMatch match(index, expression);
	AddExact(expression->OriginalExpression(), match);
	if (expression->compatibility_key) {
		compatible_entries[expression->compatibility_key->Expression()].push_back(std::move(match));
	}
}

ExpressionMatch ExpressionMatchIndex::Find(const ParsedExpression &expression) const {
	auto exact = exact_entries.find(expression);
	if (exact != exact_entries.end()) {
		return exact->second;
	}
	if (compatible_entries.empty() || !ContainsCase(expression) ||
	    CaseExpression::ContainsLambdaReference(expression)) {
		return ExpressionMatch();
	}
	CaseCompatibilityKey lookup(expression);
	auto bucket = compatible_entries.find(lookup.Expression());
	if (bucket == compatible_entries.end()) {
		return ExpressionMatch();
	}
	ExpressionMatch result;
	for (auto &candidate : bucket->second) {
		if (!candidate.descriptor->compatibility_key->Matches(lookup)) {
			continue;
		}
		if (result.IsValid() && compatible_policy == ExpressionMatchPolicy::AMBIGUOUS &&
		    result.GetIndex() != candidate.GetIndex()) {
			return ExpressionMatch::Ambiguous();
		}
		result = candidate;
		if (compatible_policy == ExpressionMatchPolicy::FIRST) {
			break;
		}
	}
	return result;
}
} // namespace duckdb
