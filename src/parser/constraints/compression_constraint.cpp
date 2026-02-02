#include "duckdb/parser/constraints/compression_constraint.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/enum_util.hpp"

namespace duckdb {

CompressionConstraint::CompressionConstraint(string column_name, CompressionType compression_type)
    : Constraint(ConstraintType::COMPRESSION), column_name(std::move(column_name)), compression_type(compression_type) {
}

CompressionConstraint::~CompressionConstraint() {
}

string CompressionConstraint::ToString() const {
	return StringUtil::Format("%s USING COMPRESSION %s", column_name, CompressionTypeToString(compression_type));
}

unique_ptr<Constraint> CompressionConstraint::Copy() const {
	return make_uniq<CompressionConstraint>(column_name, compression_type);
}

} // namespace duckdb
