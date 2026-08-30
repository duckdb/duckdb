#include "duckdb/planner/bound_expression_map.hpp"

#include "duckdb/parser/parsed_expression_iterator.hpp"

namespace duckdb {

void BoundExpressionMap::Insert(const ParsedExpression &node, unique_ptr<Expression> expr) {
	if (scope_stack.empty()) {
		throw InternalException("BoundExpressionMap::Insert called without an open BoundExpressionScope");
	}
	D_ASSERT(expr);
	MapEntry entry;
	entry.expression = std::move(expr);
	entry.scope_index = scope_stack.size() - 1;
#ifdef DEBUG
	entry.expr_class = node.GetExpressionClass();
	entry.expr_type = node.GetExpressionType();
#endif
	entries[node] = std::move(entry);
	scope_stack.back().push_back(node);
}

bool BoundExpressionMap::IsBound(const ParsedExpression &node) const {
	auto entry = entries.find(node);
	if (entry == entries.end()) {
		return false;
	}
	VerifyEntry(node);
	return true;
}

const BoundExpressionMap::MapEntry &BoundExpressionMap::GetEntry(const ParsedExpression &node) const {
	auto entry = entries.find(node);
	if (entry == entries.end()) {
		throw InternalException("BoundExpressionMap does not contain a bound expression for this node");
	}
	VerifyEntry(node);
	return entry->second;
}

void BoundExpressionMap::VerifyEntry(const ParsedExpression &node) const {
#ifdef DEBUG
	auto &entry = entries.find(node)->second;
	D_ASSERT(entry.expr_class == node.GetExpressionClass());
	D_ASSERT(entry.expr_type == node.GetExpressionType());
#endif
}

Expression &BoundExpressionMap::Get(const ParsedExpression &node) const {
	return *GetEntry(node).expression;
}

unique_ptr<Expression> &BoundExpressionMap::GetMutable(const ParsedExpression &node) {
	auto entry = entries.find(node);
	if (entry == entries.end()) {
		throw InternalException("BoundExpressionMap does not contain a bound expression for this node");
	}
	VerifyEntry(node);
	return entry->second.expression;
}

unique_ptr<Expression> BoundExpressionMap::Consume(const ParsedExpression &node) {
	auto entry = entries.find(node);
	if (entry == entries.end()) {
		throw InternalException("BoundExpressionMap::Consume called on a node without a bound expression");
	}
	VerifyEntry(node);
	auto result = std::move(entry->second.expression);
	entries.erase(entry);
	return result;
}

bool BoundExpressionMap::HasBoundDescendant(const ParsedExpression &node) const {
	if (entries.empty()) {
		return false;
	}
	if (IsBound(node)) {
		return true;
	}
	bool found = false;
	ParsedExpressionIterator::EnumerateChildren(node, [&](const ParsedExpression &child) {
		if (!found && HasBoundDescendant(child)) {
			found = true;
		}
	});
	return found;
}

void BoundExpressionMap::EraseSubtree(const ParsedExpression &node) {
	if (entries.empty()) {
		return;
	}
	entries.erase(node);
	ParsedExpressionIterator::EnumerateChildren(node, [&](const ParsedExpression &child) { EraseSubtree(child); });
}

BoundExpressionScope::BoundExpressionScope(BoundExpressionMap &bound_expressions)
    : bound_expressions(bound_expressions) {
	bound_expressions.scope_stack.emplace_back();
}

BoundExpressionScope::~BoundExpressionScope() {
	auto scope_index = bound_expressions.scope_stack.size() - 1;
	for (auto &node : bound_expressions.scope_stack.back()) {
		auto entry = bound_expressions.entries.find(node);
		if (entry != bound_expressions.entries.end() && entry->second.scope_index == scope_index) {
			bound_expressions.entries.erase(entry);
		}
	}
	bound_expressions.scope_stack.pop_back();
}

} // namespace duckdb
