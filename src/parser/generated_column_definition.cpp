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
		auto& column_ref = (ColumnRefExpression &)expr;
		auto &column_name = column_ref.GetColumnName();
		if ((!column_ref.IsQualified() || column_ref.GetTableName() == name) &&
		    ColumnsContainsColumnRef(columns, column_name)) {
			if (!column_ref.IsQualified()) {
				auto &names = column_ref.column_names;
				// dprintf(2, "TABLE NAME %s BAKED INTO COLUMN REF %s\n", name.c_str(), column_name.c_str());
				names.insert(names.begin(), name);
				// dprintf(2, "ColumnRef names after: TABLE[%s] - COL[%s]\n", column_ref.column_names[0].c_str(), column_ref.column_names[1].c_str());
			}
		} else {
			unresolved_columns.push_back(column_name);
		}
	}
	ParsedExpressionIterator::EnumerateChildren(expr, [&](const ParsedExpression &child) {
		VerifyAndRenameExpression(name, columns, (ParsedExpression &)child, unresolved_columns);
	});
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

static void _RenameTable(ParsedExpression& expr, const RenameTableInfo& info) {
	if (expr.type == ExpressionType::COLUMN_REF) {
		auto &colref = (ColumnRefExpression &)expr;
		colref.column_names[0] = info.new_table_name;
	}

	ParsedExpressionIterator::EnumerateChildren(
	    expr, [&](const ParsedExpression &child) { _RenameTable((ParsedExpression &)child, info); });
}

void GeneratedColumnDefinition::RenameTable(const RenameTableInfo& info) {
	_RenameTable(*expression, info);
}

void GeneratedColumnDefinition::RenameColumnRefs(RenameColumnInfo &info) {
	RenameExpression(*expression, info);
}

void GeneratedColumnDefinition::CheckValidity(const vector<ColumnDefinition> &columns, const string &table_name) {
	vector<string> unresolved_columns;

	VerifyAndRenameExpression(table_name, columns, *expression, unresolved_columns);
	if (unresolved_columns.size()) {
		throw BinderException(
		    "Not all columns referenced in the generated column expression could be resolved to the table \"%s\"",
		    table_name);
	}
}

} // namespace duckdb
