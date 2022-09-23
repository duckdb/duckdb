#include "duckdb/parser/constraint.hpp"

#include "duckdb/common/printer.hpp"
#include "duckdb/common/serializer.hpp"
#include "duckdb/parser/constraints/list.hpp"
#include "duckdb/common/field_writer.hpp"

namespace duckdb {

Constraint::Constraint(ConstraintType type) : type(type) {
}

Constraint::~Constraint() {
}

void Constraint::Serialize(Serializer &serializer) const {
	FieldWriter writer(serializer);
	writer.WriteField<ConstraintType>(type);
	Serialize(writer);
	writer.Finalize();
}

unique_ptr<Constraint> Constraint::Deserialize(Deserializer &source) {
	FieldReader reader(source);
	auto type = reader.ReadRequired<ConstraintType>();
	unique_ptr<Constraint> result;
	switch (type) {
	case ConstraintType::NOT_NULL:
		result = NotNullConstraint::Deserialize(reader);
		break;
	case ConstraintType::CHECK:
		result = CheckConstraint::Deserialize(reader);
		break;
	case ConstraintType::UNIQUE:
		result = UniqueConstraint::Deserialize(reader);
		break;
	case ConstraintType::FOREIGN_KEY:
		result = ForeignKeyConstraint::Deserialize(reader);
		break;
	default:
		throw InternalException("Unrecognized constraint type for serialization");
	}
	reader.Finalize();
	return result;
}

void Constraint::Print() const {
	Printer::Print(ToString());
}

} // namespace duckdb
