#include "duckdb/main/relation/join_relation.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/parser/expression/star_expression.hpp"
#include "duckdb/parser/tableref/joinref.hpp"
#include "duckdb/common/enum_util.hpp"

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

// static void ReplaceQualificationRecursive(unique_ptr<ParsedExpression> &expr, case_insensitive_map_t<string>
// &changed_bindings) { 	if (expr->GetExpressionType() == ExpressionType::COLUMN_REF) { 		auto &col_ref =
//expr->Cast<ColumnRefExpression>(); 		auto &col_names = col_ref.column_names; 		if (col_ref.IsQualified()) { 			auto it =
//changed_bindings.find(col_ref.GetTableName()); 			if (it != changed_bindings.end()) { 				col_names
//			}
//			col_names.erase(col_names.begin());
//		}
//	} else {
//		ParsedExpressionIterator::EnumerateChildren(*expr, [&table_name](unique_ptr<ParsedExpression> &child) {
//			ReplaceQualificationRecursive(child, table_name);
//		});
//	}
//}

// unique_ptr<TableRef> DeduplicateBindings(unique_ptr<TableRef> original, ) {
//	auto &ref = *original;
//	switch (ref.type) {
//	case TableReferenceType::EXPRESSION_LIST: {
//		auto &el_ref = ref.Cast<ExpressionListRef>();
//		for (idx_t i = 0; i < el_ref.values.size(); i++) {
//			for (idx_t j = 0; j < el_ref.values[i].size(); j++) {
//				expr_callback(el_ref.values[i][j]);
//			}
//		}
//		break;
//	}
//	case TableReferenceType::JOIN: {
//		auto &j_ref = ref.Cast<JoinRef>();
//		EnumerateTableRefChildren(*j_ref.left, expr_callback, ref_callback);
//		EnumerateTableRefChildren(*j_ref.right, expr_callback, ref_callback);
//		if (j_ref.condition) {
//			expr_callback(j_ref.condition);
//		}
//		break;
//	}
//	case TableReferenceType::PIVOT: {
//		auto &p_ref = ref.Cast<PivotRef>();
//		EnumerateTableRefChildren(*p_ref.source, expr_callback, ref_callback);
//		for (auto &aggr : p_ref.aggregates) {
//			expr_callback(aggr);
//		}
//		break;
//	}
//	case TableReferenceType::SUBQUERY: {
//		auto &sq_ref = ref.Cast<SubqueryRef>();
//		EnumerateQueryNodeChildren(*sq_ref.subquery->node, expr_callback, ref_callback);
//		break;
//	}
//	case TableReferenceType::TABLE_FUNCTION: {
//		auto &tf_ref = ref.Cast<TableFunctionRef>();
//		expr_callback(tf_ref.function);
//		break;
//	}
//	case TableReferenceType::BASE_TABLE:
//	case TableReferenceType::EMPTY_FROM:
//	case TableReferenceType::SHOW_REF:
//	case TableReferenceType::COLUMN_DATA:
//	case TableReferenceType::DELIM_GET:
//		// these TableRefs do not need to be unfolded
//		break;
//	case TableReferenceType::INVALID:
//	case TableReferenceType::CTE:
//		throw NotImplementedException("TableRef type not implemented for traversal");
//	}
//}

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
	return std::move(join_ref);
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
