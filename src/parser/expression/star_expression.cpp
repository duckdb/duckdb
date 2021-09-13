#include "duckdb/parser/expression/star_expression.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/serializer.hpp"

namespace duckdb {

StarExpression::StarExpression(string relation_name_p)
    : ParsedExpression(ExpressionType::STAR, ExpressionClass::STAR), relation_name(move(relation_name_p)) {
}

string StarExpression::ToString() const {
	string result = relation_name.empty() ? "*" : relation_name + ".*";
	if (!exclude_list.empty()) {
		result += "EXCEPT (";
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
		result += "REPLACE (";
		bool first_entry = true;
		for (auto &entry : replace_list) {
			if (!first_entry) {
				result += ", ";
			}
			result += entry.first;
			result += " AS ";
			result += entry.second->ToString();
			first_entry = false;
		}
		result += ")";
	}
	return result;
}

bool StarExpression::Equals(const StarExpression *a, const StarExpression *b) {
	if (a->relation_name != b->relation_name || a->exclude_list != b->exclude_list) {
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
	return true;
}

void StarExpression::Serialize(Serializer &serializer) {
	ParsedExpression::Serialize(serializer);
	serializer.WriteString(relation_name);
	serializer.Write<uint32_t>(exclude_list.size());
	for (auto &exclusion : exclude_list) {
		serializer.WriteString(exclusion);
	}
	serializer.Write<uint32_t>(replace_list.size());
	for (auto &entry : replace_list) {
		serializer.WriteString(entry.first);
		entry.second->Serialize(serializer);
	}
}

unique_ptr<ParsedExpression> StarExpression::Deserialize(ExpressionType type, Deserializer &source) {
	auto result = make_unique<StarExpression>();
	result->relation_name = source.Read<string>();
	auto exclusion_count = source.Read<uint32_t>();
	for (idx_t i = 0; i < exclusion_count; i++) {
		result->exclude_list.insert(source.Read<string>());
	}
	auto replace_count = source.Read<uint32_t>();
	for (idx_t i = 0; i < replace_count; i++) {
		auto name = source.Read<string>();
		auto expr = ParsedExpression::Deserialize(source);
		result->replace_list.insert(make_pair(name, move(expr)));
	}
	return move(result);
}

unique_ptr<ParsedExpression> StarExpression::Copy() const {
	auto copy = make_unique<StarExpression>(relation_name);
	copy->exclude_list = exclude_list;
	for (auto &entry : replace_list) {
		copy->replace_list[entry.first] = entry.second->Copy();
	}
	copy->CopyProperties(*this);
	return move(copy);
}

} // namespace duckdb
