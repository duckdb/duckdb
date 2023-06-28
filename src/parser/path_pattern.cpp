#include "duckdb/common/field_writer.hpp"
#include "duckdb/parser/path_pattern.hpp"

namespace duckdb {

void PathPattern::Serialize(Serializer &serializer) const {
	FieldWriter writer(serializer);
	writer.WriteSerializableList(path_elements);
	writer.WriteOptional(where_clause);
	writer.WriteField<bool>(all);
	writer.WriteField<bool>(shortest);
	writer.WriteField<bool>(group);
	writer.WriteField<int32_t>(topk);
	writer.Finalize();
}

unique_ptr<PathPattern> PathPattern::Deserialize(Deserializer &deserializer) {
	auto result = make_uniq<PathPattern>();
	FieldReader reader(deserializer);
	result->path_elements = reader.ReadRequiredSerializableList<PathReference>();
	result->where_clause = reader.ReadOptional<ParsedExpression>(nullptr);
	result->all = reader.ReadRequired<bool>();
	result->shortest = reader.ReadRequired<bool>();
	result->group = reader.ReadRequired<bool>();
	result->topk = reader.ReadRequired<int32_t>();
	reader.Finalize();
	return result;
}

bool PathPattern::Equals(const PathPattern *other_p) const {
	if (where_clause && other_p->where_clause.get()) {
		if (!ParsedExpression::Equals(where_clause, other_p->where_clause)) {
			return false;
		}
	}
	if ((where_clause && !other_p->where_clause.get()) || (!where_clause && other_p->where_clause.get())) {
		return false;
	}
	if (path_elements.size() != other_p->path_elements.size()) {
		return false;
	}
	for (idx_t idx = 0; idx < path_elements.size(); idx++) {
		if (!path_elements[idx]->Equals(other_p->path_elements[idx].get())) {
			return false;
		}
	}

	if (all != other_p->all) {
		return false;
	}

	if (shortest != other_p->shortest) {
		return false;
	}

	if (group != other_p->group) {
		return false;
	}

	if (topk != other_p->topk) {
		return false;
	}

	return true;
}
unique_ptr<PathPattern> PathPattern::Copy() {
	auto result = make_uniq<PathPattern>();

	for (auto &path_element : path_elements) {
		result->path_elements.push_back(path_element->Copy());
	}

	if (result->where_clause) {
		result->where_clause = where_clause->Copy();
	}

	result->all = all;
	result->shortest = shortest;
	result->group = group;
	result->topk = topk;

	return result;
}

} // namespace duckdb
