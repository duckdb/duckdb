#include "duckdb/parser/generated_column_definition.hpp"
#include "duckdb/common/field_writer.hpp"
#include "duckdb/parser/column_definition.hpp"
#include "duckdb/parser/parsed_expression_iterator.hpp"
#include "duckdb/parser/expression/columnref_expression.hpp"

namespace duckdb {

GeneratedColumnDefinition::GeneratedColumnDefinition(string name_p, LogicalType type_p,
                                                     unique_ptr<ParsedExpression> expression)
    : name(move(name_p)), type(move(type_p)), expression(move(expression)) {
}

GeneratedColumnDefinition GeneratedColumnDefinition::Copy() const {
	GeneratedColumnDefinition copy(name, type, expression->Copy());
	copy.oid = oid;
	copy.compression_type = compression_type;
	return copy;
}

void GeneratedColumnDefinition::Serialize(Serializer &serializer) const {
	FieldWriter writer(serializer);
	writer.WriteString(name);
	writer.WriteSerializable(type);
	writer.WriteSerializable(*expression);
	writer.Finalize();
}

GeneratedColumnDefinition GeneratedColumnDefinition::Deserialize(Deserializer &source) {
	FieldReader reader(source);
	auto column_name = reader.ReadRequired<string>();
	auto column_type = reader.ReadRequiredSerializable<LogicalType, LogicalType>();
	auto expression = ParsedExpression::Deserialize(source);
	reader.Finalize();

	return GeneratedColumnDefinition(column_name, column_type, move(expression));
}

static bool ColumnsContainsColumnRef(const vector<ColumnDefinition> &columns, const string &columnref) {
	for (auto &col : columns) {
		if (col.name == columnref) {
			return true;
		}
	}
	return false;
}

static void VerifyAndRenameExpression(const string &name, const vector<ColumnDefinition> &columns,
                                      ParsedExpression &expr, vector<string> &unresolved_columns) {
	if (expr.type == ExpressionType::COLUMN_REF) {
		auto column_ref = (ColumnRefExpression &)expr;
		auto &column_name = column_ref.GetColumnName();
		if ((!column_ref.IsQualified() || column_ref.GetTableName() == name) &&
		    ColumnsContainsColumnRef(columns, column_name)) {
			if (!column_ref.IsQualified()) {
				auto &names = column_ref.column_names;
				names.insert(names.begin(), name);
			}
		} else {
			unresolved_columns.push_back(column_name);
		}
	}
	ParsedExpressionIterator::EnumerateChildren(expr, [&](const ParsedExpression &child) {
		VerifyAndRenameExpression(name, columns, (ParsedExpression &)child, unresolved_columns);
	});
}

void GeneratedColumnDefinition::CheckValidity(const vector<ColumnDefinition> &columns, const string &table_name) {
	vector<string> unresolved_columns;

	VerifyAndRenameExpression(name, columns, *expression, unresolved_columns);
	if (unresolved_columns.size()) {
		throw BinderException(
		    "Not all columns referenced in the generated column expression could be resolved to the table \"%s\"",
		    table_name);
	}
}

} // namespace duckdb
