#include "duckdb/parser/path_pattern.hpp"

namespace duckdb {

void PathPattern::Serialize(Serializer &serializer) const {
	serializer.WriteProperty(100, "path_elements", path_elements);
	serializer.WriteProperty(101, "where_clause", where_clause);
	serializer.WriteProperty(102, "all", all);
	serializer.WriteProperty(103, "shortest", shortest);
	serializer.WriteProperty(104, "group", group);
	serializer.WriteProperty(105, "topk", topk);
}

unique_ptr<PathPattern> PathPattern::Deserialize(Deserializer &deserializer) {
	auto result = make_uniq<PathPattern>();
	deserializer.ReadProperty(100, "path_elements", result->path_elements);
	deserializer.ReadProperty(101, "where_clause", result->where_clause);
	deserializer.ReadProperty(102, "all", result->all);
	deserializer.ReadProperty(103, "shortest", result->shortest);
	deserializer.ReadProperty(104, "group", result->group);
	deserializer.ReadProperty(105, "topk", result->topk);
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
