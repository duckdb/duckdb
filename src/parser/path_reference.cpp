
#include "duckdb/parser/path_reference.hpp"
#include "duckdb/common/field_writer.hpp"

namespace duckdb {

bool PathReference::Equals(const PathReference *other_p) const {
	if (!other_p) {
		return false;
	}
	if (path_reference_type != other_p->path_reference_type) {
		return false;
	}
	return true;
}
void PathReference::Serialize(Serializer &serializer) const {
	FieldWriter writer(serializer);
	writer.WriteField<PGQPathReferenceType>(path_reference_type);
	Serialize(writer);
	writer.Finalize();
}

unique_ptr<PathReference> PathReference::Deserialize(Deserializer &deserializer) {
	FieldReader reader(deserializer);

	auto path_reference_type = reader.ReadRequired<PGQPathReferenceType>();

	unique_ptr<PathReference> result;

	switch (path_reference_type) {
	case PGQPathReferenceType::PATH_ELEMENT:
		result = PathElement::Deserialize(reader);
		break;
	case PGQPathReferenceType::SUBPATH:
		result = SubPath::Deserialize(reader);
		break;
	default:
		throw InternalException("Unknown path reference type in deserializer.");
	}
	reader.Finalize();

	result->path_reference_type = path_reference_type;
	return result;
}

} // namespace duckdb
