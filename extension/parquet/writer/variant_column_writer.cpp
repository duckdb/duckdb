#include "writer/variant_column_writer.hpp"
#include "duckdb/common/types/variant.hpp"
#include "duckdb/common/helper.hpp"

namespace duckdb {

LogicalType VariantColumnWriter::TransformedType() {
	child_list_t<LogicalType> children;
	for (auto &child_writer : child_writers) {
		children.emplace_back(child_writer->Schema().name, child_writer->Type());
	}
	return LogicalType::STRUCT(std::move(children));
}

} // namespace duckdb
