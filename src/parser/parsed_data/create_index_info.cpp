#include "duckdb/parser/parsed_data/create_index_info.hpp"

#include "duckdb/parser/expression/columnref_expression.hpp"
#include "duckdb/parser/parsed_expression_iterator.hpp"

namespace duckdb {

CreateIndexInfo::CreateIndexInfo() : CreateInfo(CatalogType::INDEX_ENTRY, INVALID_SCHEMA) {
}

CreateIndexInfo::CreateIndexInfo(const duckdb::CreateIndexInfo &info)
    : CreateInfo(CatalogType::INDEX_ENTRY, info.schema), table(info.table), index_name(info.index_name),
      options(info.options), index_type(info.index_type), constraint_type(info.constraint_type),
      column_ids(info.column_ids), scan_types(info.scan_types), names(info.names) {
}

static void RemoveTableQualificationRecursive(unique_ptr<ParsedExpression> &expr, const string &table_name) {
	if (expr->GetExpressionType() != ExpressionType::COLUMN_REF) {
		ParsedExpressionIterator::EnumerateChildren(*expr, [&table_name](unique_ptr<ParsedExpression> &child) {
			RemoveTableQualificationRecursive(child, table_name);
		});
		return;
	}

	auto &col_ref = expr->Cast<ColumnRefExpression>();
	auto &col_names = col_ref.column_names;
	if (col_ref.IsQualified() && col_ref.GetTableName() == table_name) {
		col_names.erase(col_names.begin());
	}
}

vector<string> CreateIndexInfo::ExpressionsToList() const {
	vector<string> list;

	for (idx_t i = 0; i < parsed_expressions.size(); i++) {
		auto &expr = parsed_expressions[i];
		auto copy = expr->Copy();

		// Column reference expressions are qualified with the table name.
		// We need to remove them to reproduce the original query.
		RemoveTableQualificationRecursive(copy, table);
		bool add_parenthesis = true;
		if (copy->GetExpressionType() == ExpressionType::COLUMN_REF) {
			auto &column_ref = copy->Cast<ColumnRefExpression>();
			if (!column_ref.IsQualified()) {
				// Only not qualified references like (col1, col2) don't need parenthesis.
				add_parenthesis = false;
			}
		}

		if (add_parenthesis) {
			list.push_back(StringUtil::Format("(%s)", copy->ToString()));
		} else {
			list.push_back(StringUtil::Format("%s", copy->ToString()));
		}
	}
	return list;
}

string CreateIndexInfo::ExpressionsToString() const {
	auto list = ExpressionsToList();
	return StringUtil::Join(list, ", ");
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
	result += ExpressionsToString();
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
