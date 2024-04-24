#include "duckdb/parser/parsed_data/create_index_info.hpp"
#include "duckdb/parser/parsed_expression_iterator.hpp"
#include "duckdb/parser/expression/columnref_expression.hpp"

namespace duckdb {

CreateIndexInfo::CreateIndexInfo() : CreateInfo(CatalogType::INDEX_ENTRY) {
}

CreateIndexInfo::CreateIndexInfo(const duckdb::CreateIndexInfo &info)
    : CreateInfo(CatalogType::INDEX_ENTRY), table(info.table), index_name(info.index_name), options(info.options),
      index_type(info.index_type), constraint_type(info.constraint_type), column_ids(info.column_ids),
      scan_types(info.scan_types), names(info.names) {
}

static void RemoveTableQualificationRecursive(unique_ptr<ParsedExpression> &expr, const string &table_name) {
	if (expr->GetExpressionType() == ExpressionType::COLUMN_REF) {
		auto &col_ref = expr->Cast<ColumnRefExpression>();
		auto &col_names = col_ref.column_names;
		if (col_ref.IsQualified() && col_ref.GetTableName() == table_name) {
			col_names.erase(col_names.begin());
		}
	} else {
		ParsedExpressionIterator::EnumerateChildren(*expr, [&table_name](unique_ptr<ParsedExpression> &child) {
			RemoveTableQualificationRecursive(child, table_name);
		});
	}
}

string CreateIndexInfo::ToString() const {
	string result;

	result += "CREATE";
	D_ASSERT(constraint_type == IndexConstraintType::UNIQUE || constraint_type == IndexConstraintType::NONE);
	if (constraint_type == IndexConstraintType::UNIQUE) {
		result += " UNIQUE";
	}
	result += " INDEX ";
	if (on_conflict == OnCreateConflict::IGNORE_ON_CONFLICT) {
		result += "IF NOT EXISTS ";
	}
	result += KeywordHelper::WriteOptionallyQuoted(index_name);
	result += " ON ";
	result += QualifierToString(temporary ? "" : catalog, schema, table);
	if (index_type != "ART") {
		result += " USING ";
		result += KeywordHelper::WriteOptionallyQuoted(index_type);
		result += " ";
	}
	result += "(";
	for (idx_t i = 0; i < parsed_expressions.size(); i++) {
		auto &expr = parsed_expressions[i];
		auto copy = expr->Copy();
		if (i > 0) {
			result += ", ";
		}
		// column ref expressions are qualified with the table name
		// we need to remove them to reproduce the original query
		RemoveTableQualificationRecursive(copy, table);
		bool add_parenthesis = true;
		if (copy->type == ExpressionType::COLUMN_REF) {
			auto &column_ref = copy->Cast<ColumnRefExpression>();
			if (!column_ref.IsQualified()) {
				// Only when column references are not qualified, i.e (col1, col2)
				// then these expressions do not need to be wrapped in parenthesis
				add_parenthesis = false;
			}
		}
		if (add_parenthesis) {
			result += StringUtil::Format("(%s)", copy->ToString());
		} else {
			result += StringUtil::Format("%s", copy->ToString());
		}
	}
	result += ")";
	if (!options.empty()) {
		result += " WITH (";
		idx_t i = 0;
		for (auto &opt : options) {
			result += StringUtil::Format("%s = %s", opt.first, opt.second.ToString());
			if (i > 0) {
				result += ", ";
			}
			i++;
		}
		result += " )";
	}
	result += ";";
	return result;
}

unique_ptr<CreateInfo> CreateIndexInfo::Copy() const {

	auto result = make_uniq<CreateIndexInfo>(*this);
	CopyProperties(*result);

	for (auto &expr : expressions) {
		result->expressions.push_back(expr->Copy());
	}
	for (auto &expr : parsed_expressions) {
		result->parsed_expressions.push_back(expr->Copy());
	}

	return std::move(result);
}

} // namespace duckdb
