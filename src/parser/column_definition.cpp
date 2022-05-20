#include "duckdb/parser/column_definition.hpp"
#include "duckdb/common/field_writer.hpp"
#include "duckdb/parser/parsed_expression_iterator.hpp"
#include "duckdb/parser/expression/columnref_expression.hpp"
#include "duckdb/parser/parsed_data/alter_table_info.hpp"

namespace duckdb {

ColumnDefinition::ColumnDefinition(string name_p, LogicalType type_p) : name(move(name_p)), type(move(type_p)) {
}

ColumnDefinition::ColumnDefinition(string name_p, LogicalType type_p, ColumnExpression expression)
    : name(move(name_p)), type(move(type_p)) {
	switch (expression.type) {
	case ColumnExpressionType::DEFAULT: {
		default_value = move(expression.expression);
		break;
	}
	case ColumnExpressionType::GENERATED: {
		generated_expression = move(expression.expression);
		category = TableColumnType::GENERATED;
		break;
	}
	default: {
		throw InternalException("Type not implemented for ColumnExpressionType");
	}
	}
}

ColumnDefinition ColumnDefinition::Copy() const {
	ColumnDefinition copy(name, type);
	copy.oid = oid;
	copy.storage_oid = oid;
	copy.default_value = default_value ? default_value->Copy() : nullptr;
	copy.generated_expression = generated_expression ? generated_expression->Copy() : nullptr;
	copy.compression_type = compression_type;
	copy.category = category;
	return copy;
}

void ColumnDefinition::Serialize(Serializer &serializer) const {
	FieldWriter writer(serializer);
	writer.WriteString(name);
	writer.WriteSerializable(type);
	writer.WriteOptional(default_value);
	writer.WriteOptional(generated_expression);
	writer.Finalize();
}

ColumnDefinition ColumnDefinition::Deserialize(Deserializer &source) {
	FieldReader reader(source);
	auto column_name = reader.ReadRequired<string>();
	auto column_type = reader.ReadRequiredSerializable<LogicalType, LogicalType>();
	auto default_value = reader.ReadOptional<ParsedExpression>(nullptr);
	auto generated_expression = reader.ReadOptional<ParsedExpression>(nullptr);
	reader.Finalize();

	auto category = TableColumnType::STANDARD;
	if (generated_expression) {
		category = TableColumnType::GENERATED;
	}

	switch (category) {
	case TableColumnType::STANDARD:
		return ColumnDefinition(column_name, column_type, ColumnExpression(move(default_value)));
	case TableColumnType::GENERATED:
		return ColumnDefinition(column_name, column_type,
		                        ColumnExpression(move(generated_expression), ColumnExpressionType::GENERATED));
	default:
		throw NotImplementedException("Type not implemented for TableColumnType");
	}
}

bool ColumnDefinition::Generated() const {
	return category == TableColumnType::GENERATED;
}

//===--------------------------------------------------------------------===//
// Generated Columns (VIRTUAL)
//===--------------------------------------------------------------------===//
static bool ColumnsContainsColumnRef(const vector<ColumnDefinition> &columns, const string &columnref) {
	if (columnref == "rowid") {
		return true;
	}
	for (auto &col : columns) {
		// if (col.Generated()) {
		//	continue;
		// }
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

static void InnerGetListOfDependencies(ParsedExpression &expr, vector<string> &dependencies) {
	if (expr.type == ExpressionType::COLUMN_REF) {
		auto columnref = (ColumnRefExpression &)expr;
		auto &name = columnref.GetColumnName();
		dependencies.push_back(name);
	}
	ParsedExpressionIterator::EnumerateChildren(expr, [&](const ParsedExpression &child) {
		InnerGetListOfDependencies((ParsedExpression &)child, dependencies);
	});
}

void ColumnDefinition::GetListOfDependencies(vector<string> &dependencies) {
	D_ASSERT(category == TableColumnType::GENERATED);
	InnerGetListOfDependencies(*generated_expression, dependencies);
}

void ColumnDefinition::CheckValidity(const vector<ColumnDefinition> &columns, const string &table_name) {
	D_ASSERT(category == TableColumnType::GENERATED);
	VerifyColumnRefs(table_name, columns, *generated_expression);
}

void ColumnDefinition::SetGeneratedExpression(unique_ptr<ParsedExpression> expression) {
	if (default_value) {
		throw InvalidInputException("DEFAULT constraint on GENERATED column \"%s\" is not allowed", name);
	}
	category = TableColumnType::GENERATED;
	generated_expression = move(expression);
}

ParsedExpression &ColumnDefinition::GeneratedExpression() {
	D_ASSERT(category == TableColumnType::GENERATED);
	return *generated_expression;
}

} // namespace duckdb
