#include "duckdb/main/relation/join_relation.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/parser/expression/star_expression.hpp"
#include "duckdb/parser/tableref/joinref.hpp"
#include "duckdb/common/enum_util.hpp"
#include "duckdb/parser/parsed_expression_iterator.hpp"
#include "duckdb/parser/tableref/pivotref.hpp"
#include "duckdb/parser/tableref/table_function_ref.hpp"

namespace duckdb {

JoinRelation::JoinRelation(shared_ptr<Relation> left_p, shared_ptr<Relation> right_p,
                           unique_ptr<ParsedExpression> condition_p, JoinType type, JoinRefType join_ref_type)
    : Relation(left_p->context, RelationType::JOIN_RELATION), left(std::move(left_p)), right(std::move(right_p)),
      condition(std::move(condition_p)), join_type(type), join_ref_type(join_ref_type) {
	if (left->context.GetContext() != right->context.GetContext()) {
		throw InvalidInputException("Cannot combine LEFT and RIGHT relations of different connections!");
	}
	context.GetContext()->TryBindRelation(*this, this->columns);
}

JoinRelation::JoinRelation(shared_ptr<Relation> left_p, shared_ptr<Relation> right_p, vector<string> using_columns_p,
                           JoinType type, JoinRefType join_ref_type)
    : Relation(left_p->context, RelationType::JOIN_RELATION), left(std::move(left_p)), right(std::move(right_p)),
      using_columns(std::move(using_columns_p)), join_type(type), join_ref_type(join_ref_type) {
	if (left->context.GetContext() != right->context.GetContext()) {
		throw InvalidInputException("Cannot combine LEFT and RIGHT relations of different connections!");
	}
	context.GetContext()->TryBindRelation(*this, this->columns);
}

unique_ptr<QueryNode> JoinRelation::GetQueryNode() {
	auto result = make_uniq<SelectNode>();
	result->select_list.push_back(make_uniq<StarExpression>());
	result->from_table = GetTableRef();
	return std::move(result);
}

static string GetDeduplicatedName(case_insensitive_map_t<string> &bindings, const string &name) {
	auto it = bindings.find(name);
	if (it == bindings.end()) {
		// Binding not seen, safe to assume it's unique (I assume this errors later anyways?)
		return name;
	}

	// If 'second' is not empty, this is the start of a deduplication chain that we need to follow
	while (!it->second.empty()) {
		it = bindings.find(it->second);
	}
	return it->first;
}

static void ReplaceQualificationRecursive(unique_ptr<ParsedExpression> &expr,
                                          case_insensitive_map_t<string> &changed_bindings) {
	if (expr->GetExpressionType() == ExpressionType::COLUMN_REF) {
		auto &col_ref = expr->Cast<ColumnRefExpression>();
		if (col_ref.IsQualified()) {
			auto deduplicated_name = GetDeduplicatedName(changed_bindings, col_ref.GetTableName());
			col_ref.ReplaceOrRemoveTableName(deduplicated_name);
		}
	} else {
		ParsedExpressionIterator::EnumerateChildren(*expr, [&changed_bindings](unique_ptr<ParsedExpression> &child) {
			ReplaceQualificationRecursive(child, changed_bindings);
		});
	}
}

static void AddBinding(case_insensitive_map_t<string> &bindings, string &name) {
	auto it = bindings.find(name);
	if (it == bindings.end()) {
		bindings[name] = "";
		return;
	}
	// If the value is not an empty string, the binding name already has a duplicate
	// this creates a chain of name -> name_1 -> name_1_1 -> ...
	while (!it->second.empty()) {
		it = bindings.find(it->second);
	}

	string deduplicated_name = it->first + "_1";
	it->second = deduplicated_name;
	bindings[deduplicated_name] = "";

	// Update the alias of the tableref
	name = deduplicated_name;
}

unique_ptr<TableRef> DeduplicateBindings(unique_ptr<TableRef> original, case_insensitive_map_t<string> &bindings) {
	auto &ref = *original;
	switch (ref.type) {
	case TableReferenceType::BASE_TABLE:
	case TableReferenceType::EMPTY_FROM:
	case TableReferenceType::SHOW_REF:
	case TableReferenceType::COLUMN_DATA:
	case TableReferenceType::DELIM_GET:
	case TableReferenceType::SUBQUERY:
	case TableReferenceType::EXPRESSION_LIST: {
		if (!ref.alias.empty()) {
			AddBinding(bindings, ref.alias);
		}
		return std::move(original);
	}
	case TableReferenceType::JOIN: {
		auto &j_ref = ref.Cast<JoinRef>();
		j_ref.left = DeduplicateBindings(std::move(j_ref.left), bindings);
		j_ref.right = DeduplicateBindings(std::move(j_ref.right), bindings);
		if (j_ref.condition) {
			ReplaceQualificationRecursive(j_ref.condition, bindings);
		}
		return std::move(original);
	}
	case TableReferenceType::PIVOT: {
		auto &p_ref = ref.Cast<PivotRef>();
		if (!ref.alias.empty()) {
			AddBinding(bindings, ref.alias);
		}
		p_ref.source = DeduplicateBindings(std::move(p_ref.source), bindings);
		for (auto &aggr : p_ref.aggregates) {
			ReplaceQualificationRecursive(aggr, bindings);
		}
		return std::move(original);
	}
	case TableReferenceType::TABLE_FUNCTION: {
		auto &tf_ref = ref.Cast<TableFunctionRef>();
		if (!ref.alias.empty()) {
			AddBinding(bindings, ref.alias);
		}
		ReplaceQualificationRecursive(tf_ref.function, bindings);
		return std::move(original);
	}
	default:
		break;
	}
	throw NotImplementedException("TableRef type (%s) not implemented for traversal", EnumUtil::ToString(ref.type));
}

unique_ptr<TableRef> JoinRelation::GetTableRef() {
	auto join_ref = make_uniq<JoinRef>(join_ref_type);
	join_ref->left = left->GetTableRef();
	join_ref->right = right->GetTableRef();
	if (condition) {
		join_ref->condition = condition->Copy();
	}
	join_ref->using_columns = using_columns;
	join_ref->type = join_type;
	join_ref->delim_flipped = delim_flipped;
	for (auto &col : duplicate_eliminated_columns) {
		join_ref->duplicate_eliminated_columns.emplace_back(col->Copy());
	}
	case_insensitive_map_t<string> bindings;
	return DeduplicateBindings(std::move(join_ref), bindings);
}

const vector<ColumnDefinition> &JoinRelation::Columns() {
	return this->columns;
}

string JoinRelation::ToString(idx_t depth) {
	string str = RenderWhitespace(depth);
	str += "Join " + EnumUtil::ToString(join_ref_type) + " " + EnumUtil::ToString(join_type);
	if (condition) {
		str += " " + condition->GetName();
	}

	return str + "\n" + left->ToString(depth + 1) + "\n" + right->ToString(depth + 1);
}

} // namespace duckdb
