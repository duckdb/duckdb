#include "duckdb/main/relation/join_relation.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/parser/expression/star_expression.hpp"
#include "duckdb/parser/tableref/joinref.hpp"
#include "duckdb/common/enum_util.hpp"
#include "duckdb/parser/parsed_expression_iterator.hpp"
#include "duckdb/parser/tableref/pivotref.hpp"
#include "duckdb/parser/tableref/table_function_ref.hpp"
#include "duckdb/parser/tableref/basetableref.hpp"
#include "duckdb/main/client_context_wrapper.hpp"

namespace duckdb {

JoinRelation::JoinRelation(shared_ptr<Relation> left_p, shared_ptr<Relation> right_p,
                           unique_ptr<ParsedExpression> condition_p, JoinType type, JoinRefType join_ref_type)
    : Relation(left_p->context, RelationType::JOIN_RELATION), left(std::move(left_p)), right(std::move(right_p)),
      condition(std::move(condition_p)), join_type(type), join_ref_type(join_ref_type) {
	if (left->context->GetContext() != right->context->GetContext()) {
		throw InvalidInputException("Cannot combine LEFT and RIGHT relations of different connections!");
	}
	TryBindRelation(columns);
}

JoinRelation::JoinRelation(shared_ptr<Relation> left_p, shared_ptr<Relation> right_p, vector<string> using_columns_p,
                           JoinType type, JoinRefType join_ref_type)
    : Relation(left_p->context, RelationType::JOIN_RELATION), left(std::move(left_p)), right(std::move(right_p)),
      using_columns(std::move(using_columns_p)), join_type(type), join_ref_type(join_ref_type) {
	if (left->context->GetContext() != right->context->GetContext()) {
		throw InvalidInputException("Cannot combine LEFT and RIGHT relations of different connections!");
	}
	TryBindRelation(columns);
}

unique_ptr<QueryNode> JoinRelation::GetQueryNode() {
	auto result = make_uniq<SelectNode>();
	result->select_list.push_back(make_uniq<StarExpression>());
	result->from_table = GetTableRef();
	return std::move(result);
}

namespace {

class BindingDeduplicator {
public:
	BindingDeduplicator() {
	}

public:
	unique_ptr<TableRef> DeduplicateBindings(unique_ptr<TableRef> original) {
		auto &ref = *original;
		switch (ref.type) {
		case TableReferenceType::BASE_TABLE:
		case TableReferenceType::EMPTY_FROM:
		case TableReferenceType::SHOW_REF:
		case TableReferenceType::COLUMN_DATA:
		case TableReferenceType::DELIM_GET:
		case TableReferenceType::SUBQUERY:
		case TableReferenceType::EXPRESSION_LIST: {
			auto &alias = ref.alias;
			if (alias.empty() && ref.type == TableReferenceType::BASE_TABLE) {
				auto &base_table = ref.Cast<BaseTableRef>();
				alias = base_table.table_name;
			}
			if (!alias.empty()) {
				AddBinding(alias);
			}
			return std::move(original);
		}
		case TableReferenceType::JOIN: {
			auto &j_ref = ref.Cast<JoinRef>();
			j_ref.left = DeduplicateBindings(std::move(j_ref.left));
			j_ref.right = DeduplicateBindings(std::move(j_ref.right));
			if (j_ref.condition) {
				ReplaceQualificationRecursive(j_ref.condition);
			}
			return std::move(original);
		}
		case TableReferenceType::PIVOT: {
			auto &p_ref = ref.Cast<PivotRef>();
			if (!ref.alias.empty()) {
				AddBinding(ref.alias);
			}
			p_ref.source = DeduplicateBindings(std::move(p_ref.source));
			for (auto &aggr : p_ref.aggregates) {
				ReplaceQualificationRecursive(aggr);
			}
			return std::move(original);
		}
		case TableReferenceType::TABLE_FUNCTION: {
			auto &tf_ref = ref.Cast<TableFunctionRef>();
			if (!ref.alias.empty()) {
				AddBinding(ref.alias);
			}
			ReplaceQualificationRecursive(tf_ref.function);
			return std::move(original);
		}
		default:
			break;
		}
		throw NotImplementedException("TableRef type (%s) not implemented for traversal", EnumUtil::ToString(ref.type));
	}

private:
	string GetDeduplicatedName(const string &name) {
		auto it = bindings.find(name);
		D_ASSERT(it != bindings.end());

		if (it->second == 1) {
			return name;
		}
		D_ASSERT(it->second > 1);
		return StringUtil::Format("%s_%d", name, it->second - 1);
	}

	void ReplaceQualificationRecursive(unique_ptr<ParsedExpression> &expr) {
		if (expr->GetExpressionType() == ExpressionType::COLUMN_REF) {
			auto &col_ref = expr->Cast<ColumnRefExpression>();
			if (col_ref.IsQualified()) {
				auto deduplicated_name = GetDeduplicatedName(col_ref.GetTableName());
				col_ref.ReplaceOrRemoveTableName(deduplicated_name);
			}
		} else {
			ParsedExpressionIterator::EnumerateChildren(
			    *expr, [this](unique_ptr<ParsedExpression> &child) { ReplaceQualificationRecursive(child); });
		}
	}

	void AddBinding(string &name) {
		// put it all lower_case
		auto low_name = StringUtil::Lower(name);
		if (bindings.find(low_name) == bindings.end()) {
			// Name does not exist yet
			bindings[low_name]++;
		} else {
			// Name already exists, we add _x where x is the repetition number
			string binding_name = name + "_" + std::to_string(bindings[low_name]);
			auto binding_name_low = StringUtil::Lower(binding_name);
			while (bindings.find(binding_name_low) != bindings.end()) {
				// This name is already here due to a previous definition
				bindings[low_name]++;
				binding_name = name + "_" + std::to_string(bindings[low_name]);
				binding_name_low = StringUtil::Lower(binding_name);
			}
			name = binding_name;
			bindings[binding_name_low]++;
		}
	}

private:
	//! Records how many times a given binding has been seen
	case_insensitive_map_t<idx_t> bindings;
};

} // namespace

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
	BindingDeduplicator deduplicator;
	auto res = deduplicator.DeduplicateBindings(std::move(join_ref));
	return res;
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
