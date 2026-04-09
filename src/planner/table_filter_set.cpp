#include "duckdb/planner/table_filter_set.hpp"
#include "duckdb/planner/filter/expression_filter.hpp"
#include "duckdb/planner/filter/conjunction_filter.hpp"
#include "duckdb/planner/filter/constant_filter.hpp"
#include "duckdb/planner/filter/dynamic_filter.hpp"
#include "duckdb/planner/filter/in_filter.hpp"
#include "duckdb/planner/filter/null_filter.hpp"
#include "duckdb/planner/filter/optional_filter.hpp"
#include "duckdb/planner/filter/struct_filter.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression/bound_operator_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/serializer/serializer.hpp"
#include "duckdb/common/serializer/deserializer.hpp"
#include "duckdb/function/scalar/struct_utils.hpp"

namespace duckdb {

struct LegacyStructPathEntry {
	idx_t child_idx;
	string child_name;
};

static bool ContainsInternalTableFilterFunction(const Expression &expr) {
	bool found = false;
	ExpressionIterator::EnumerateChildren(expr, [&](const Expression &child) {
		if (found) {
			return;
		}
		found = ContainsInternalTableFilterFunction(child);
	});
	return found;
}

static unique_ptr<TableFilter> SerializeExpressionToLegacyFilter(const Expression &expr);

static ExpressionType FlipComparisonType(ExpressionType type) {
	switch (type) {
	case ExpressionType::COMPARE_LESSTHAN:
		return ExpressionType::COMPARE_GREATERTHAN;
	case ExpressionType::COMPARE_LESSTHANOREQUALTO:
		return ExpressionType::COMPARE_GREATERTHANOREQUALTO;
	case ExpressionType::COMPARE_GREATERTHAN:
		return ExpressionType::COMPARE_LESSTHAN;
	case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
		return ExpressionType::COMPARE_LESSTHANOREQUALTO;
	default:
		return type;
	}
}

static bool IsSupportedConstantComparison(ExpressionType type) {
	switch (type) {
	case ExpressionType::COMPARE_EQUAL:
	case ExpressionType::COMPARE_NOTEQUAL:
	case ExpressionType::COMPARE_LESSTHAN:
	case ExpressionType::COMPARE_LESSTHANOREQUALTO:
	case ExpressionType::COMPARE_GREATERTHAN:
	case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
		return true;
	default:
		return false;
	}
}

static bool TryExtractLegacySubject(const Expression &expr, vector<LegacyStructPathEntry> &struct_path) {
	switch (expr.GetExpressionClass()) {
	case ExpressionClass::BOUND_REF:
	case ExpressionClass::BOUND_COLUMN_REF:
		return true;
	case ExpressionClass::BOUND_FUNCTION: {
		auto &func = expr.Cast<BoundFunctionExpression>();
		idx_t child_idx;
		if (!TryGetStructExtractChildIndex(func, child_idx) || func.children.empty()) {
			return false;
		}
		if (!TryExtractLegacySubject(*func.children[0], struct_path)) {
			return false;
		}
		string child_name;
		if (func.children[0]->return_type.id() == LogicalTypeId::STRUCT &&
		    !StructType::IsUnnamed(func.children[0]->return_type)) {
			child_name = StructType::GetChildName(func.children[0]->return_type, child_idx);
		}
		struct_path.push_back({child_idx, std::move(child_name)});
		return true;
	}
	default:
		return false;
	}
}

static unique_ptr<TableFilter> WrapStructFilterPath(unique_ptr<TableFilter> filter,
                                                    const vector<LegacyStructPathEntry> &struct_path) {
	for (auto it = struct_path.rbegin(); it != struct_path.rend(); ++it) {
		filter = make_uniq<StructFilter>(it->child_idx, it->child_name, std::move(filter));
	}
	return filter;
}

static void NormalizeLegacyExpression(unique_ptr<Expression> &expr) {
	ExpressionIterator::VisitExpressionMutable<BoundColumnRefExpression>(
	    expr, [](BoundColumnRefExpression &col_ref, unique_ptr<Expression> &owned_expr) {
		    owned_expr = make_uniq<BoundReferenceExpression>(col_ref.alias, col_ref.return_type, 0ULL);
	    });
	ExpressionIterator::VisitExpressionMutable<BoundReferenceExpression>(
	    expr, [](BoundReferenceExpression &ref, unique_ptr<Expression> &owned_expr) { ref.index = 0; });
}

static unique_ptr<TableFilter> TrySerializeComparisonToLegacyFilter(const BoundComparisonExpression &comparison) {
	const Expression *subject = nullptr;
	const Value *constant = nullptr;
	auto comparison_type = comparison.type;
	if (comparison.right->GetExpressionClass() == ExpressionClass::BOUND_CONSTANT) {
		subject = comparison.left.get();
		constant = &comparison.right->Cast<BoundConstantExpression>().value;
	} else if (comparison.left->GetExpressionClass() == ExpressionClass::BOUND_CONSTANT) {
		subject = comparison.right.get();
		constant = &comparison.left->Cast<BoundConstantExpression>().value;
		comparison_type = FlipComparisonType(comparison_type);
	} else {
		return nullptr;
	}

	vector<LegacyStructPathEntry> struct_path;
	if (!TryExtractLegacySubject(*subject, struct_path)) {
		return nullptr;
	}
	if (constant->IsNull()) {
		switch (comparison_type) {
		case ExpressionType::COMPARE_DISTINCT_FROM:
			return WrapStructFilterPath(make_uniq<IsNotNullFilter>(), struct_path);
		case ExpressionType::COMPARE_NOT_DISTINCT_FROM:
			return WrapStructFilterPath(make_uniq<IsNullFilter>(), struct_path);
		default:
			return nullptr;
		}
	}
	if (!IsSupportedConstantComparison(comparison_type)) {
		return nullptr;
	}
	return WrapStructFilterPath(make_uniq<ConstantFilter>(comparison_type, *constant), struct_path);
}

static unique_ptr<TableFilter> TrySerializeOperatorToLegacyFilter(const BoundOperatorExpression &op) {
	switch (op.type) {
	case ExpressionType::OPERATOR_IS_NULL:
	case ExpressionType::OPERATOR_IS_NOT_NULL: {
		if (op.children.size() != 1) {
			return nullptr;
		}
		vector<LegacyStructPathEntry> struct_path;
		if (!TryExtractLegacySubject(*op.children[0], struct_path)) {
			return nullptr;
		}
		if (op.type == ExpressionType::OPERATOR_IS_NULL) {
			return WrapStructFilterPath(make_uniq<IsNullFilter>(), struct_path);
		}
		return WrapStructFilterPath(make_uniq<IsNotNullFilter>(), struct_path);
	}
	case ExpressionType::COMPARE_IN: {
		if (op.children.empty()) {
			return nullptr;
		}
		vector<LegacyStructPathEntry> struct_path;
		if (!TryExtractLegacySubject(*op.children[0], struct_path)) {
			return nullptr;
		}
		vector<Value> values;
		values.reserve(op.children.size() - 1);
		for (idx_t i = 1; i < op.children.size(); i++) {
			if (op.children[i]->GetExpressionClass() != ExpressionClass::BOUND_CONSTANT) {
				return nullptr;
			}
			auto value = op.children[i]->Cast<BoundConstantExpression>().value;
			if (value.IsNull()) {
				return nullptr;
			}
			values.push_back(std::move(value));
		}
		if (values.empty()) {
			return nullptr;
		}
		return WrapStructFilterPath(make_uniq<InFilter>(std::move(values)), struct_path);
	}
	default:
		return nullptr;
	}
}

static unique_ptr<TableFilter> SerializeConjunctionToLegacyFilter(const BoundConjunctionExpression &conjunction) {
	unique_ptr<ConjunctionFilter> result;
	if (conjunction.type == ExpressionType::CONJUNCTION_AND) {
		result = make_uniq<ConjunctionAndFilter>();
	} else if (conjunction.type == ExpressionType::CONJUNCTION_OR) {
		result = make_uniq<ConjunctionOrFilter>();
	} else {
		throw SerializationException("Unsupported conjunction type %s during table-filter serialization",
		                             EnumUtil::ToString(conjunction.type));
	}
	for (auto &child : conjunction.children) {
		auto child_filter = SerializeExpressionToLegacyFilter(*child);
		if (!child_filter) {
			return nullptr;
		}
		result->child_filters.push_back(std::move(child_filter));
	}
	return std::move(result);
}

static unique_ptr<TableFilter> SerializeExpressionToLegacyFilter(const Expression &expr) {
	if (expr.GetExpressionClass() == ExpressionClass::BOUND_CONJUNCTION) {
		return SerializeConjunctionToLegacyFilter(expr.Cast<BoundConjunctionExpression>());
	}
	if (expr.GetExpressionClass() == ExpressionClass::BOUND_COMPARISON) {
		auto result = TrySerializeComparisonToLegacyFilter(expr.Cast<BoundComparisonExpression>());
		if (result) {
			return result;
		}
	}
	if (expr.GetExpressionClass() == ExpressionClass::BOUND_OPERATOR) {
		auto result = TrySerializeOperatorToLegacyFilter(expr.Cast<BoundOperatorExpression>());
		if (result) {
			return result;
		}
	}
	if (ContainsInternalTableFilterFunction(expr)) {
		return nullptr;
	}
	auto normalized_expr = expr.Copy();
	NormalizeLegacyExpression(normalized_expr);
	return make_uniq<ExpressionFilter>(std::move(normalized_expr));
}

TableFilterSet::ConstTableFilterIteratorEntry::ConstTableFilterIteratorEntry(
    map<ProjectionIndex, unique_ptr<TableFilter>>::const_iterator it)
    : iterator(it) {
}

ProjectionIndex TableFilterSet::ConstTableFilterIteratorEntry::GetIndex() const {
	return iterator->first;
}

const TableFilter &TableFilterSet::ConstTableFilterIteratorEntry::Filter() const {
	return *iterator->second;
}

TableFilterSet::TableFilterIteratorEntry::TableFilterIteratorEntry(
    map<ProjectionIndex, unique_ptr<TableFilter>>::iterator it)
    : iterator(it) {
}

ProjectionIndex TableFilterSet::TableFilterIteratorEntry::GetIndex() const {
	return iterator->first;
}

TableFilter &TableFilterSet::TableFilterIteratorEntry::Filter() {
	return *iterator->second;
}

const TableFilter &TableFilterSet::TableFilterIteratorEntry::Filter() const {
	return *iterator->second;
}

unique_ptr<TableFilter> TableFilterSet::TableFilterIteratorEntry::TakeFilter() {
	return std::move(iterator->second);
}

bool TableFilterSet::HasFilters() const {
	return !filters.empty();
}
idx_t TableFilterSet::FilterCount() const {
	return filters.size();
}
bool TableFilterSet::HasFilter(ProjectionIndex col_idx) const {
	return filters.find(col_idx) != filters.end();
}

const TableFilter &TableFilterSet::GetFilterByColumnIndex(ProjectionIndex col_idx) const {
	auto filter = TryGetFilterByColumnIndex(col_idx);
	if (!filter) {
		throw InternalException("Table filter set does not have a filter for column idx %d", col_idx);
	}
	return *filter;
}

optional_ptr<const TableFilter> TableFilterSet::TryGetFilterByColumnIndex(ProjectionIndex col_idx) const {
	if (!col_idx.IsValid()) {
		throw InternalException("TableFilterSet::TryGetFilterByColumnIndex called with invalid column index");
	}
	auto entry = filters.find(col_idx);
	if (entry == filters.end()) {
		return nullptr;
	}
	return *entry->second;
}

TableFilter &TableFilterSet::GetFilterByColumnIndexMutable(ProjectionIndex col_idx) {
	auto filter = TryGetFilterByColumnIndexMutable(col_idx);
	if (!filter) {
		throw InternalException("Table filter set does not have a filter for column idx %d", col_idx);
	}
	return *filter;
}

optional_ptr<TableFilter> TableFilterSet::TryGetFilterByColumnIndexMutable(ProjectionIndex col_idx) {
	auto entry = filters.find(col_idx);
	if (entry == filters.end()) {
		return nullptr;
	}
	return *entry->second;
}

void TableFilterSet::RemoveFilterByColumnIndex(ProjectionIndex col_idx) {
	filters.erase(col_idx);
}

void TableFilterSet::SetFilterByColumnIndex(ProjectionIndex col_idx, unique_ptr<TableFilter> filter) {
	filters[col_idx] = std::move(filter);
}

void TableFilterSet::ClearFilters() {
	filters.clear();
}

bool TableFilterSet::Equals(TableFilterSet &other) {
	if (filters.size() != other.filters.size()) {
		return false;
	}
	for (auto &entry : filters) {
		auto other_entry = other.filters.find(entry.first);
		if (other_entry == other.filters.end()) {
			return false;
		}
		if (!entry.second->Equals(*other_entry->second)) {
			return false;
		}
	}
	return true;
}

bool TableFilterSet::Equals(TableFilterSet *left, TableFilterSet *right) {
	if (left == right) {
		return true;
	}
	if (!left || !right) {
		return false;
	}
	return left->Equals(*right);
}

unique_ptr<TableFilterSet> TableFilterSet::Copy() const {
	auto copy = make_uniq<TableFilterSet>();
	for (auto &it : filters) {
		copy->filters.emplace(it.first, it.second->Copy());
	}
	return copy;
}

void TableFilterSet::PushFilter(ProjectionIndex col_idx, unique_ptr<TableFilter> filter) {
	if (!col_idx.IsValid()) {
		throw InternalException("Cannot push a filter over an invalid ProjectionIndex");
	}
	auto entry = filters.find(col_idx);
	if (entry == filters.end()) {
		// no filter yet: push the filter directly
		filters[col_idx] = std::move(filter);
	} else {
		// there is already a filter: AND it together
		if (entry->second->filter_type == TableFilterType::CONJUNCTION_AND) {
			auto &and_filter = entry->second->Cast<ConjunctionAndFilter>();
			and_filter.child_filters.push_back(std::move(filter));
		} else {
			auto and_filter = make_uniq<ConjunctionAndFilter>();
			and_filter->child_filters.push_back(std::move(entry->second));
			and_filter->child_filters.push_back(std::move(filter));
			filters[col_idx] = std::move(and_filter);
		}
	}
}

void DynamicTableFilterSet::ClearFilters(const PhysicalOperator &op) {
	lock_guard<mutex> l(lock);
	filters.erase(op);
}

void DynamicTableFilterSet::PushFilter(const PhysicalOperator &op, ProjectionIndex column_index,
                                       unique_ptr<TableFilter> filter) {
	lock_guard<mutex> l(lock);
	auto entry = filters.find(op);
	optional_ptr<TableFilterSet> filter_ptr;
	if (entry == filters.end()) {
		auto filter_set = make_uniq<TableFilterSet>();
		filter_ptr = filter_set.get();
		filters[op] = std::move(filter_set);
	} else {
		filter_ptr = entry->second.get();
	}
	filter_ptr->PushFilter(column_index, std::move(filter));
}

bool DynamicTableFilterSet::HasFilters() const {
	lock_guard<mutex> l(lock);
	return !filters.empty();
}

unique_ptr<TableFilterSet>
DynamicTableFilterSet::GetFinalTableFilters(const PhysicalTableScan &scan,
                                            optional_ptr<TableFilterSet> existing_filters) const {
	lock_guard<mutex> l(lock);
	D_ASSERT(!filters.empty());
	auto result = make_uniq<TableFilterSet>();
	if (existing_filters) {
		for (auto &filter_entry : *existing_filters) {
			result->PushFilter(filter_entry.GetIndex(), filter_entry.Filter().Copy());
		}
	}
	for (auto &entry : filters) {
		for (auto &filter_entry : *entry.second) {
			result->PushFilter(filter_entry.GetIndex(), filter_entry.Filter().Copy());
		}
	}
	if (!result->HasFilters()) {
		return nullptr;
	}
	return result;
}

map<ProjectionIndex, unique_ptr<TableFilter>>
TableFilterSet::GetTableFiltersForSerialization(Serializer &serializer) const {
	map<ProjectionIndex, unique_ptr<TableFilter>> result;
	for (auto &entry : filters) {
		if (entry.second->filter_type != TableFilterType::EXPRESSION_FILTER) {
			result.emplace(entry.first, entry.second->Copy());
			continue;
		}
		auto &expr_filter =
		    ExpressionFilter::GetExpressionFilter(*entry.second, "TableFilterSet::GetTableFiltersForSerialization");
		auto serialized_filter = SerializeExpressionToLegacyFilter(*expr_filter.expr);
		if (!serialized_filter) {
			throw SerializationException(
			    "Could not serialize table filter for projection index %llu to the legacy format",
			    entry.first.GetIndex());
		}
		result.emplace(entry.first, std::move(serialized_filter));
	}
	return result;
}

map<ProjectionIndex, unique_ptr<TableFilter>> &
TableFilterSet::GetTableFiltersForDeserialization(Deserializer &deserializer) {
	return filters;
}

} // namespace duckdb
