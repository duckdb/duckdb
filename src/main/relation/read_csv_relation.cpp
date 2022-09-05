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

namespace duckdb {

ReadCSVRelation::ReadCSVRelation(const std::shared_ptr<ClientContext> &context, string csv_file_p,
                                 vector<ColumnDefinition> columns_p, bool auto_detect, string alias_p)
    : Relation(context, RelationType::READ_CSV_RELATION), csv_file(move(csv_file_p)), auto_detect(auto_detect),
      alias(move(alias_p)), columns(move(columns_p)) {
	if (alias.empty()) {
		alias = StringUtil::Split(csv_file, ".")[0];
	}
}

unique_ptr<QueryNode> ReadCSVRelation::GetQueryNode() {
	auto result = make_unique<SelectNode>();
	result->select_list.push_back(make_unique<StarExpression>());
	result->from_table = GetTableRef();
	return move(result);
}

unique_ptr<TableRef> ReadCSVRelation::GetTableRef() {
	auto table_ref = make_unique<TableFunctionRef>();
	table_ref->alias = alias;
	vector<unique_ptr<ParsedExpression>> children;
	// CSV file
	children.push_back(make_unique<ConstantExpression>(Value(csv_file)));
	if (!auto_detect) {
		// parameters
		child_list_t<Value> column_names;
		for (idx_t i = 0; i < columns.size(); i++) {
			column_names.push_back(make_pair(columns[i].Name(), Value(columns[i].Type().ToString())));
		}
		auto colnames = make_unique<ConstantExpression>(Value::STRUCT(move(column_names)));
		children.push_back(make_unique<ComparisonExpression>(
		    ExpressionType::COMPARE_EQUAL, make_unique<ColumnRefExpression>("columns"), move(colnames)));
	} else {
		children.push_back(make_unique<ComparisonExpression>(ExpressionType::COMPARE_EQUAL,
		                                                     make_unique<ColumnRefExpression>("auto_detect"),
		                                                     make_unique<ConstantExpression>(Value::BOOLEAN(true))));
	}
	table_ref->function = make_unique<FunctionExpression>("read_csv", move(children));
	return move(table_ref);
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
