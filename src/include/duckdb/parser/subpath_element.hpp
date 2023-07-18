
#pragma once

#include "duckdb/parser/path_reference.hpp"

namespace duckdb {

enum class PGQPathMode : uint8_t { NONE, WALK, SIMPLE, TRAIL, ACYCLIC };

class SubPath : public PathReference {

public:
	vector<unique_ptr<PathReference>> path_list;

	unique_ptr<ParsedExpression> where_clause;

	PGQPathMode path_mode;

	bool single_bind;
	int64_t lower, upper;

    string path_variable;

	// TODO cost_expr, default_value
public:
	explicit SubPath(PGQPathReferenceType path_reference_type) : PathReference(path_reference_type) {
	}

	string ToString() const override;

	unique_ptr<PathReference> Copy() override;

	bool Equals(const PathReference *other_p) const override;

	void Serialize(FieldWriter &writer) const override;

	static unique_ptr<PathReference> Deserialize(FieldReader &reader);
};

} // namespace duckdb
