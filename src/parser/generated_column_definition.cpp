#include "duckdb/parser/generated_column_definition.hpp"
#include "duckdb/common/field_writer.hpp"
#include "duckdb/parser/column_definition.hpp"
#include "duckdb/parser/parsed_expression_iterator.hpp"
#include "duckdb/parser/expression/columnref_expression.hpp"
#include "duckdb/parser/parsed_data/alter_table_info.hpp"

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
	if (columnref == "rowid") {
		return true;
	}
	for (auto &col : columns) {
		if (col.name == columnref) {
			return true;
		}
	}
	return false;
}

static void StripTableName(ColumnRefExpression &expr, const string &table_name) {
	D_ASSERT(expr.IsQualified());
	auto &name = expr.GetTableName();
	auto &col_name = expr.GetColumnName();
	if (name != table_name) {
		throw BinderException("Column \"%s\" tries to reference a table outside of %s", expr.GetColumnName(),
		                      table_name);
	}
	// Replace the column_names vector with only the column name
	expr.column_names = vector<string> {col_name};
}

static void VerifyColumnRefs(const string &name, const vector<ColumnDefinition> &columns, ParsedExpression &expr) {
	if (expr.type == ExpressionType::COLUMN_REF) {
		auto &column_ref = (ColumnRefExpression &)expr;
		auto &column_name = column_ref.GetColumnName();
		bool exists_in_table = ColumnsContainsColumnRef(columns, column_name);
		if (!exists_in_table) {
			throw BinderException("Column \"%s\" could not be found in table %s", column_name, name);
		}
		if (column_ref.IsQualified()) {
			StripTableName(column_ref, name);
		}
	}
	ParsedExpressionIterator::EnumerateChildren(
	    expr, [&](const ParsedExpression &child) { VerifyColumnRefs(name, columns, (ParsedExpression &)child); });
}

static void RenameExpression(ParsedExpression &expr, RenameColumnInfo &info) {
	if (expr.type == ExpressionType::COLUMN_REF) {
		auto &colref = (ColumnRefExpression &)expr;
		if (colref.column_names.back() == info.old_name) {
			colref.column_names.back() = info.new_name;
		}
	}
	ParsedExpressionIterator::EnumerateChildren(
	    expr, [&](const ParsedExpression &child) { RenameExpression((ParsedExpression &)child, info); });
}

void GeneratedColumnDefinition::RenameColumnRefs(RenameColumnInfo &info) {
	RenameExpression(*expression, info);
}

void GeneratedColumnDefinition::CheckValidity(const vector<ColumnDefinition> &columns, const string &table_name) {
	VerifyColumnRefs(table_name, columns, *expression);
}

} // namespace duckdb
