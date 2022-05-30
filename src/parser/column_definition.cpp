#include "duckdb/parser/column_definition.hpp"
#include "duckdb/common/field_writer.hpp"
#include "duckdb/parser/parsed_expression_iterator.hpp"
#include "duckdb/parser/expression/columnref_expression.hpp"
#include "duckdb/parser/parsed_data/alter_table_info.hpp"
#include "duckdb/parser/expression/cast_expression.hpp"

namespace duckdb {

ColumnDefinition::ColumnDefinition(string name_p, LogicalType type_p) : name(move(name_p)), type(move(type_p)) {
}

ColumnDefinition::ColumnDefinition(string name_p, LogicalType type_p, unique_ptr<ParsedExpression> expression,
                                   TableColumnType category)
    : name(move(name_p)), type(move(type_p)), category(category) {
	switch (category) {
	case TableColumnType::STANDARD: {
		default_value = move(expression);
		break;
	}
	case TableColumnType::GENERATED: {
		generated_expression = move(expression);
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
	copy.storage_oid = storage_oid;
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
	if (Generated()) {
		writer.WriteOptional(generated_expression);
	} else {
		writer.WriteOptional(default_value);
	}
	writer.WriteField<TableColumnType>(category);
	writer.Finalize();
}

ColumnDefinition ColumnDefinition::Deserialize(Deserializer &source) {
	FieldReader reader(source);
	auto column_name = reader.ReadRequired<string>();
	auto column_type = reader.ReadRequiredSerializable<LogicalType, LogicalType>();
	auto expression = reader.ReadOptional<ParsedExpression>(nullptr);
	auto category = reader.ReadField<TableColumnType>(TableColumnType::STANDARD);
	reader.Finalize();

	switch (category) {
	case TableColumnType::STANDARD:
		return ColumnDefinition(column_name, column_type, move(expression), TableColumnType::STANDARD);
	case TableColumnType::GENERATED:
		return ColumnDefinition(column_name, column_type, move(expression), TableColumnType::GENERATED);
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

	bool type_is_any = type.id() == LogicalTypeId::ANY;
	if (type_is_any) {
		generated_expression = move(expression);
		return;
	}
	// Always wrap the expression in a cast, that way we can always update the cast when we change the type
	// Except if the type is LogicalType::ANY (no type specified)
	generated_expression = make_unique_base<ParsedExpression, CastExpression>(type, move(expression));
}

void ColumnDefinition::ChangeGeneratedExpressionType(const LogicalType &type) {
	D_ASSERT(Generated());
	// First time the type is set, add a cast around the expression
	if (this->type.id() == LogicalTypeId::ANY && this->generated_expression->type != ExpressionType::OPERATOR_CAST) {
		generated_expression = make_unique_base<ParsedExpression, CastExpression>(type, move(generated_expression));
		return;
	}
	// Every generated expression should be wrapped in a cast on creation
	D_ASSERT(generated_expression->type == ExpressionType::OPERATOR_CAST);
	auto &cast_expr = (CastExpression &)*generated_expression;
	auto base_expr = move(cast_expr.child);
	generated_expression = make_unique_base<ParsedExpression, CastExpression>(type, move(base_expr));
}

ParsedExpression &ColumnDefinition::GeneratedExpression() const {
	D_ASSERT(category == TableColumnType::GENERATED);
	return *generated_expression;
}

} // namespace duckdb
