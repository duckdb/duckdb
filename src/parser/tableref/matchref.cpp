#include "duckdb/parser/tableref/matchref.hpp"
#include "duckdb/parser/expression/columnref_expression.hpp"
#include "duckdb/parser/expression/star_expression.hpp"
#include "duckdb/common/field_writer.hpp"

namespace duckdb {

string MatchExpression::ToString() const {
	string result = "GRAPH_TABLE (";
	result += pg_name + " MATCH";

	for (idx_t i = 0; i < path_patterns.size(); i++) {
		(i > 0) ? result += ", " : result;
		for (idx_t j = 0; j < path_patterns[i]->path_elements.size(); j++) {
			auto &path_reference = path_patterns[i]->path_elements[j];
			switch (path_reference->path_reference_type) {
			case PGQPathReferenceType::PATH_ELEMENT: {
				auto path_element = reinterpret_cast<PathElement *>(path_reference.get());
				result += " " + path_element->ToString();
				break;
			}
			case PGQPathReferenceType::SUBPATH: {
				auto subpath = reinterpret_cast<SubPath *>(path_reference.get());
				result += " " + subpath->ToString();
				break;
			}
			default:
				throw InternalException("Unknown path reference type found in ToString()");
			}
		}
	}
	result += where_clause ? "\nWHERE " + where_clause->ToString() : "";

	result += "\nCOLUMNS (";
	for (idx_t i = 0; i < column_list.size(); i++) {
		if (column_list[i]->type == ExpressionType::STAR) {
			auto &star = (StarExpression &)*column_list[i];
			result += star.ToString();
			break;
		} else if (column_list[i]->type == ExpressionType::COLUMN_REF) {
			auto &column = (ColumnRefExpression &)*column_list[i];
			result += (i > 0 ? ", " : "") + column.ToString() + (column.alias.empty() ? "" : " AS " + column.alias);
		} else {
			throw ConstraintException("Unhandled type of expression in COLUMNS");
		}
	}
	result += ")\n";
	result += ")" + alias;

	return result;
}

bool MatchExpression::Equals(const BaseExpression &other_p) const {
	if (!ParsedExpression::Equals(other_p)) {
		return false;
	}

	auto &other = other_p.Cast<MatchExpression>();
	if (pg_name != other.pg_name) {
		return false;
	}

	if (alias != other.alias) {
		return false;
	}

	if (path_patterns.size() != other.path_patterns.size()) {
		return false;
	}

	// path_list
	for (idx_t i = 0; i < path_patterns.size(); i++) {
		if (!path_patterns[i]->Equals(other.path_patterns[i].get())) {
			return false;
		}
	}

	if (column_list.size() != column_list.size()) {
		return false;
	}

	// columns
	for (idx_t i = 0; i < column_list.size(); i++) {
		if (!ParsedExpression::Equals(column_list[i], other.column_list[i])) {
			return false;
		}
	}

	// where clause
	if (where_clause && other.where_clause.get()) {
		if (!ParsedExpression::Equals(where_clause, other.where_clause)) {
			return false;
		}
	}
	if ((where_clause && !other.where_clause.get()) || (!where_clause && other.where_clause.get())) {
		return false;
	}

	return true;
}

unique_ptr<ParsedExpression> MatchExpression::Copy() const {
	auto copy = make_uniq<MatchExpression>();
	copy->pg_name = pg_name;
	copy->alias = alias;

	for (auto &path : path_patterns) {
		copy->path_patterns.push_back(path->Copy());
	}

	for (auto &column : column_list) {
		copy->column_list.push_back(column->Copy());
	}

	copy->where_clause = where_clause ? where_clause->Copy() : nullptr;

	return std::move(copy);
}

void MatchExpression::Serialize(FieldWriter &writer) const {
	writer.WriteString(pg_name);
	writer.WriteString(alias);
	writer.WriteSerializableList<PathPattern>(path_patterns);
	writer.WriteSerializableList<ParsedExpression>(column_list);
	writer.WriteOptional(where_clause);
}

unique_ptr<ParsedExpression> MatchExpression::Deserialize(FieldReader &reader) {
	auto result = make_uniq<MatchExpression>();
	result->pg_name = reader.ReadRequired<string>();
	result->alias = reader.ReadRequired<string>();
	result->path_patterns = reader.ReadRequiredSerializableList<PathPattern>();
	result->column_list = reader.ReadRequiredSerializableList<ParsedExpression>();
	result->where_clause = reader.ReadOptional<ParsedExpression>(nullptr);
	return std::move(result);
}

} // namespace duckdb
