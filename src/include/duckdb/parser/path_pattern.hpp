
#pragma once

#include "duckdb/parser/tableref.hpp"
#include "duckdb/parser/parsed_expression.hpp"
#include "duckdb/parser/path_reference.hpp"
#include "duckdb/parser/path_element.hpp"
#include "duckdb/parser/subpath_element.hpp"

namespace duckdb {

class PathPattern {
public:
	unique_ptr<ParsedExpression> where_clause;
	vector<unique_ptr<PathReference>> path_elements;

	bool all = false;
	bool shortest = false;
	bool group = false;
	int32_t topk = false;

	PathPattern() = default;

	unique_ptr<PathPattern> Copy();

	bool Equals(const PathPattern *other_p) const;

	void Serialize(Serializer &serializer) const;

	static unique_ptr<PathPattern> Deserialize(Deserializer &deserializer);
};

} // namespace duckdb
