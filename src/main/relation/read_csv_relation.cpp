#include "duckdb/main/relation/read_csv_relation.hpp"
#include "duckdb/parser/tableref/table_function_ref.hpp"
#include "duckdb/parser/tableref/basetableref.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/parser/expression/star_expression.hpp"
#include "duckdb/parser/expression/columnref_expression.hpp"
#include "duckdb/parser/expression/comparison_expression.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/execution/operator/persistent/buffered_csv_reader.hpp"

namespace duckdb {

void ReadCSVRelation::AddNamedParameter(const string &name, Value argument) {
	arguments.push_back(make_unique<ComparisonExpression>(ExpressionType::COMPARE_EQUAL,
	                                                      make_unique<ColumnRefExpression>(name),
	                                                      make_unique<ConstantExpression>(std::move(argument))));
}

ReadCSVRelation::ReadCSVRelation(const std::shared_ptr<ClientContext> &context, string csv_file_p,
                                 vector<ColumnDefinition> columns_p, bool auto_detect, string alias_p)
    : Relation(context, RelationType::READ_CSV_RELATION), csv_file(std::move(csv_file_p)), auto_detect(auto_detect),
      alias(std::move(alias_p)), columns(std::move(columns_p)) {
	if (alias.empty()) {
		alias = StringUtil::Split(csv_file, ".")[0];
	}

	child_list_t<Value> column_names;
	for (idx_t i = 0; i < columns.size(); i++) {
		column_names.push_back(make_pair(columns[i].Name(), Value(columns[i].Type().ToString())));
	}
	AddNamedParameter("columns", Value::STRUCT(std::move(column_names)));
}

ReadCSVRelation::ReadCSVRelation(const std::shared_ptr<ClientContext> &context, string csv_file_p,
                                 BufferedCSVReaderOptions options, string alias_p)
    : Relation(context, RelationType::READ_CSV_RELATION), csv_file(std::move(csv_file_p)), auto_detect(true),
      alias(std::move(alias_p)) {
	if (alias.empty()) {
		alias = StringUtil::Split(csv_file, ".")[0];
	}
	D_ASSERT(options.auto_detect == true);
	BufferedCSVReader reader(*context, move(options));

	for (idx_t i = 0; i < reader.return_types.size(); i++) {
		columns.emplace_back(reader.names[i], reader.return_types[i]);
	}
	AddNamedParameter("auto_detect", Value::BOOLEAN(true));
}

unique_ptr<QueryNode> ReadCSVRelation::GetQueryNode() {
	auto result = make_unique<SelectNode>();
	result->select_list.push_back(make_unique<StarExpression>());
	result->from_table = GetTableRef();
	return std::move(result);
}

unique_ptr<TableRef> ReadCSVRelation::GetTableRef() {
	auto table_ref = make_unique<TableFunctionRef>();
	table_ref->alias = alias;

	vector<unique_ptr<ParsedExpression>> children;
	children.push_back(make_unique<ConstantExpression>(Value(csv_file)));
	for (idx_t i = 0; i < arguments.size(); i++) {
		children.push_back(arguments[i]->Copy());
	}

	if (auto_detect) {
		table_ref->function = make_unique<FunctionExpression>("read_csv_auto", std::move(children));
	} else {
		table_ref->function = make_unique<FunctionExpression>("read_csv", std::move(children));
	}
	return std::move(table_ref);
}

string ReadCSVRelation::GetAlias() {
	return alias;
}

const vector<ColumnDefinition> &ReadCSVRelation::Columns() {
	return columns;
}

string ReadCSVRelation::ToString(idx_t depth) {
	return RenderWhitespace(depth) + "Read CSV [" + csv_file + "]";
}

} // namespace duckdb
