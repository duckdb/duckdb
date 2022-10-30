#include "duckdb/parser/expression/star_expression.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/field_writer.hpp"

namespace duckdb {

StarExpression::StarExpression(string relation_name_p)
    : ParsedExpression(ExpressionType::STAR, ExpressionClass::STAR), relation_name(move(relation_name_p)) {
}

string StarExpression::ToString() const {
	if (!regex.empty()) {
		D_ASSERT(columns);
		return "COLUMNS('" + regex + "')";
	}
	string result;
	if (columns) {
		result += "COLUMNS(";
	}
	result += relation_name.empty() ? "*" : relation_name + ".*";
	if (!exclude_list.empty()) {
		result += " EXCLUDE (";
		bool first_entry = true;
		for (auto &entry : exclude_list) {
			if (!first_entry) {
				result += ", ";
			}
			result += entry;
			first_entry = false;
		}
		result += ")";
	}
	if (!replace_list.empty()) {
		result += " REPLACE (";
		bool first_entry = true;
		for (auto &entry : replace_list) {
			if (!first_entry) {
				result += ", ";
			}
			result += entry.second->ToString();
			result += " AS ";
			result += entry.first;
			first_entry = false;
		}
		result += ")";
	}
	if (columns) {
		result += ")";
	}
	return result;
}

bool StarExpression::Equals(const StarExpression *a, const StarExpression *b) {
	if (a->relation_name != b->relation_name || a->exclude_list != b->exclude_list) {
		return false;
	}
	if (a->columns != b->columns) {
		return false;
	}
	if (a->replace_list.size() != b->replace_list.size()) {
		return false;
	}
	for (auto &entry : a->replace_list) {
		auto other_entry = b->replace_list.find(entry.first);
		if (other_entry == b->replace_list.end()) {
			return false;
		}
		if (!entry.second->Equals(other_entry->second.get())) {
			return false;
		}
	}
	if (a->regex != b->regex) {
		return false;
	}
	return true;
}

void StarExpression::Serialize(FieldWriter &writer) const {
	auto &serializer = writer.GetSerializer();

	writer.WriteString(relation_name);

	// in order to write the exclude_list/replace_list as single fields we directly use the field writers' internal
	// serializer
	writer.WriteField<uint32_t>(exclude_list.size());
	for (auto &exclusion : exclude_list) {
		serializer.WriteString(exclusion);
	}
	writer.WriteField<uint32_t>(replace_list.size());
	for (auto &entry : replace_list) {
		serializer.WriteString(entry.first);
		entry.second->Serialize(serializer);
	}
	writer.WriteField<bool>(columns);
	writer.WriteString(regex);
}

unique_ptr<ParsedExpression> StarExpression::Deserialize(ExpressionType type, FieldReader &reader) {
	auto &source = reader.GetSource();

	auto result = make_unique<StarExpression>();
	result->relation_name = reader.ReadRequired<string>();
	auto exclusion_count = reader.ReadRequired<uint32_t>();
	for (idx_t i = 0; i < exclusion_count; i++) {
		result->exclude_list.insert(source.Read<string>());
	}
	auto replace_count = reader.ReadRequired<uint32_t>();
	for (idx_t i = 0; i < replace_count; i++) {
		auto name = source.Read<string>();
		auto expr = ParsedExpression::Deserialize(source);
		result->replace_list.insert(make_pair(name, move(expr)));
	}
	result->columns = reader.ReadField<bool>(false);
	result->regex = reader.ReadField<string>(string());
	return move(result);
}

unique_ptr<ParsedExpression> StarExpression::Copy() const {
	auto copy = make_unique<StarExpression>(relation_name);
	copy->exclude_list = exclude_list;
	for (auto &entry : replace_list) {
		copy->replace_list[entry.first] = entry.second->Copy();
	}
	copy->columns = columns;
	copy->regex = regex;
	copy->CopyProperties(*this);
	return move(copy);
}

} // namespace duckdb
