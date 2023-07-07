#include "duckdb/parser/column_definition.hpp"
#include "duckdb/common/field_writer.hpp"
#include "duckdb/parser/parsed_expression_iterator.hpp"
#include "duckdb/parser/expression/columnref_expression.hpp"
#include "duckdb/parser/parsed_data/alter_table_info.hpp"
#include "duckdb/parser/expression/cast_expression.hpp"

namespace duckdb {

ColumnDefinition::ColumnDefinition(string name_p, LogicalType type_p)
    : name(std::move(name_p)), type(std::move(type_p)) {
}

ColumnDefinition::ColumnDefinition(string name_p, LogicalType type_p, unique_ptr<ParsedExpression> expression,
                                   TableColumnType category)
    : name(std::move(name_p)), type(std::move(type_p)), category(category) {
	switch (category) {
	case TableColumnType::STANDARD: {
		default_value = std::move(expression);
		break;
	}
	case TableColumnType::GENERATED: {
		generated_expression = std::move(expression);
		break;
	}
	default: {
		throw InternalException("Type not implemented for TableColumnType");
	}
	}
}

ColumnDefinition ColumnDefinition::Copy() const {
	ColumnDefinition copy(name, type);
	copy.oid = oid;
	copy.storage_oid = storage_oid;
	copy.SetDefaultValue(default_value ? default_value->Copy() : nullptr);
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
	writer.WriteField<duckdb::CompressionType>(compression_type);
	writer.Finalize();
}

ColumnDefinition ColumnDefinition::Deserialize(Deserializer &source) {
	FieldReader reader(source);
	auto column_name = reader.ReadRequired<string>();
	auto column_type = reader.ReadRequiredSerializable<LogicalType, LogicalType>();
	auto expression = reader.ReadOptional<ParsedExpression>(nullptr);
	auto category = reader.ReadField<TableColumnType>(TableColumnType::STANDARD);
	auto compression_type = reader.ReadField<duckdb::CompressionType>(duckdb::CompressionType::COMPRESSION_AUTO);
	reader.Finalize();

	ColumnDefinition result(column_name, column_type, std::move(expression), category);
	result.compression_type = compression_type;
	return result;
}

const unique_ptr<ParsedExpression> &ColumnDefinition::DefaultValue() const {
	return default_value;
}

void ColumnDefinition::SetDefaultValue(unique_ptr<ParsedExpression> default_value) {
	this->default_value = std::move(default_value);
}

const LogicalType &ColumnDefinition::Type() const {
	return type;
}

LogicalType &ColumnDefinition::TypeMutable() {
	return type;
}

void ColumnDefinition::SetType(const LogicalType &type) {
	this->type = type;
}

const string &ColumnDefinition::Name() const {
	return name;
}

void ColumnDefinition::SetName(const string &name) {
	this->name = name;
}

const duckdb::CompressionType &ColumnDefinition::CompressionType() const {
	return compression_type;
}

void ColumnDefinition::SetCompressionType(duckdb::CompressionType compression_type) {
	this->compression_type = compression_type;
}

const storage_t &ColumnDefinition::StorageOid() const {
	return storage_oid;
}

LogicalIndex ColumnDefinition::Logical() const {
	return LogicalIndex(oid);
}

PhysicalIndex ColumnDefinition::Physical() const {
	return PhysicalIndex(storage_oid);
}

void ColumnDefinition::SetStorageOid(storage_t storage_oid) {
	this->storage_oid = storage_oid;
}

const column_t &ColumnDefinition::Oid() const {
	return oid;
}

void ColumnDefinition::SetOid(column_t oid) {
	this->oid = oid;
}

const TableColumnType &ColumnDefinition::Category() const {
	return category;
}

bool ColumnDefinition::Generated() const {
	return category == TableColumnType::GENERATED;
}

//===--------------------------------------------------------------------===//
// Generated Columns (VIRTUAL)
//===--------------------------------------------------------------------===//

static void VerifyColumnRefs(ParsedExpression &expr) {
	if (expr.type == ExpressionType::COLUMN_REF) {
		auto &column_ref = expr.Cast<ColumnRefExpression>();
		if (column_ref.IsQualified()) {
			throw ParserException(
			    "Qualified (tbl.name) column references are not allowed inside of generated column expressions");
		}
	}
	ParsedExpressionIterator::EnumerateChildren(
	    expr, [&](const ParsedExpression &child) { VerifyColumnRefs((ParsedExpression &)child); });
}

static void InnerGetListOfDependencies(ParsedExpression &expr, vector<string> &dependencies) {
	if (expr.type == ExpressionType::COLUMN_REF) {
		auto columnref = expr.Cast<ColumnRefExpression>();
		auto &name = columnref.GetColumnName();
		dependencies.push_back(name);
	}
	ParsedExpressionIterator::EnumerateChildren(expr, [&](const ParsedExpression &child) {
		if (expr.type == ExpressionType::LAMBDA) {
			throw NotImplementedException("Lambda functions are currently not supported in generated columns.");
		}
		InnerGetListOfDependencies((ParsedExpression &)child, dependencies);
	});
}

void ColumnDefinition::GetListOfDependencies(vector<string> &dependencies) const {
	D_ASSERT(Generated());
	InnerGetListOfDependencies(*generated_expression, dependencies);
}

string ColumnDefinition::GetName() const {
	return name;
}

LogicalType ColumnDefinition::GetType() const {
	return type;
}

void ColumnDefinition::SetGeneratedExpression(unique_ptr<ParsedExpression> expression) {
	category = TableColumnType::GENERATED;

	if (expression->HasSubquery()) {
		throw ParserException("Expression of generated column \"%s\" contains a subquery, which isn't allowed", name);
	}

	VerifyColumnRefs(*expression);
	if (type.id() == LogicalTypeId::ANY) {
		generated_expression = std::move(expression);
		return;
	}
	// Always wrap the expression in a cast, that way we can always update the cast when we change the type
	// Except if the type is LogicalType::ANY (no type specified)
	generated_expression = make_uniq_base<ParsedExpression, CastExpression>(type, std::move(expression));
}

void ColumnDefinition::ChangeGeneratedExpressionType(const LogicalType &type) {
	D_ASSERT(Generated());
	// First time the type is set, add a cast around the expression
	D_ASSERT(this->type.id() == LogicalTypeId::ANY);
	generated_expression = make_uniq_base<ParsedExpression, CastExpression>(type, std::move(generated_expression));
	// Every generated expression should be wrapped in a cast on creation
	// D_ASSERT(generated_expression->type == ExpressionType::OPERATOR_CAST);
	// auto &cast_expr = generated_expression->Cast<CastExpression>();
	// auto base_expr = std::move(cast_expr.child);
	// generated_expression = make_uniq_base<ParsedExpression, CastExpression>(type, std::move(base_expr));
}

const ParsedExpression &ColumnDefinition::GeneratedExpression() const {
	D_ASSERT(Generated());
	return *generated_expression;
}

ParsedExpression &ColumnDefinition::GeneratedExpressionMutable() {
	D_ASSERT(Generated());
	return *generated_expression;
}

} // namespace duckdb
