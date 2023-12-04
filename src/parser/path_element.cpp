#include "duckdb/common/serializer/serializer.hpp"
#include "duckdb/common/serializer/deserializer.hpp"
#include "duckdb/parser/path_element.hpp"
#include "duckdb/parser/path_reference.hpp"

namespace duckdb {

bool PathElement::Equals(const PathReference *other_p) const {
	if (!PathReference::Equals(other_p)) {
		return false;
	}
	auto other = (PathElement *)other_p;
	if (match_type != other->match_type) {
		return false;
	}
	if (label != other->label) {
		return false;
	}
	if (variable_binding != other->variable_binding) {
		return false;
	}

	return true;
}

void PathElement::Serialize(Serializer &serializer) const {
	serializer.WriteProperty(100, "match_type", match_type);
	serializer.WriteProperty(101, "label", label);
	serializer.WriteProperty(101, "variable_binding", variable_binding);
}

unique_ptr<PathReference> PathElement::Deserialize(Deserializer &deserializer) {
	auto result = make_uniq<PathElement>(PGQPathReferenceType::PATH_ELEMENT);
	deserializer.ReadProperty(100, "match_type", result->match_type);
	deserializer.ReadProperty(101, "label", result->label);
	deserializer.ReadProperty(102, "variable_binding", result->variable_binding);
//	result->match_type = reader.ReadRequired<PGQMatchType>();
//	result->label = reader.ReadRequired<string>();
//	result->variable_binding = reader.ReadRequired<string>();
	return std::move(result);
}

unique_ptr<PathReference> PathElement::Copy() {
	auto result = make_uniq<PathElement>(PGQPathReferenceType::PATH_ELEMENT);
	result->path_reference_type = path_reference_type;
	result->match_type = match_type;
	result->label = label;
	result->variable_binding = variable_binding;
	return std::move(result);
}
string PathElement::ToString() const {
	string result = "";
	switch (match_type) {
	case PGQMatchType::MATCH_VERTEX:
		result += "(" + variable_binding + ":" + label + ")";
		break;
	case PGQMatchType::MATCH_EDGE_ANY:
		result += "-[" + variable_binding + ":" + label + "]-";
		break;
	case PGQMatchType::MATCH_EDGE_LEFT:
		result += "<-[" + variable_binding + ":" + label + "]-";
		break;
	case PGQMatchType::MATCH_EDGE_RIGHT:
		result += "-[" + variable_binding + ":" + label + "]->";
		break;
	case PGQMatchType::MATCH_EDGE_LEFT_RIGHT:
		result += "<-[" + variable_binding + ":" + label + "]->";
		break;
	}
	return result;
}

} // namespace duckdb
