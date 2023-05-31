#include "duckdb/parser/path_element.hpp"

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

void PathElement::Serialize(FieldWriter &writer) const {
	writer.WriteField<PGQMatchType>(match_type);
	writer.WriteString(label);
	writer.WriteString(variable_binding);
}

unique_ptr<PathReference> PathElement::Deserialize(FieldReader &reader) {
	auto result = make_unique<PathElement>(PGQPathReferenceType::PATH_ELEMENT);
	result->match_type = reader.ReadRequired<PGQMatchType>();
	result->label = reader.ReadRequired<string>();
	result->variable_binding = reader.ReadRequired<string>();
	return result;
}

unique_ptr<PathReference> PathElement::Copy() {
	auto result = make_unique<PathElement>(PGQPathReferenceType::PATH_ELEMENT);
	result->path_reference_type = path_reference_type;
	result->match_type = match_type;
	result->label = label;
	result->variable_binding = variable_binding;
	return result;
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
