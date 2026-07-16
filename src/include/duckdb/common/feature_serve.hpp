//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/feature_serve.hpp
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"

namespace duckdb {

class ClientContext;
class SelectStatement;
class Serializer;
class Deserializer;

struct FeatureServeEntityMapping {
	string feature_column;
	string spine_column;

	bool operator==(const FeatureServeEntityMapping &other) const {
		return feature_column == other.feature_column && spine_column == other.spine_column;
	}

	void Serialize(Serializer &serializer) const;
	static FeatureServeEntityMapping Deserialize(Deserializer &deserializer);
};

struct ServeFeatureRequest {
	string feature_name;
	vector<FeatureServeEntityMapping> entity_mappings;

	bool operator==(const ServeFeatureRequest &other) const {
		return feature_name == other.feature_name && entity_mappings == other.entity_mappings;
	}

	void Serialize(Serializer &serializer) const;
	static ServeFeatureRequest Deserialize(Deserializer &deserializer);
};

unique_ptr<SelectStatement> BuildServeFeatureSelect(ClientContext &context, const vector<ServeFeatureRequest> &features,
                                                    const string &spine_table, const string &spine_entity_override,
                                                    const string &spine_asof_column);

} // namespace duckdb
