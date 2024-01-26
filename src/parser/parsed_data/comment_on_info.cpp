#include "duckdb/parser/parsed_data/comment_on_info.hpp"

namespace duckdb {

CommentOnInfo::CommentOnInfo()
    : ParseInfo(TYPE), catalog(INVALID_CATALOG), schema(INVALID_SCHEMA), name(""), comment(Value()) {
}

unique_ptr<CommentOnInfo> CommentOnInfo::Copy() const {
	auto result = make_uniq<CommentOnInfo>();
	result->type = type;
	result->catalog = catalog;
	result->schema = schema;
	result->name = name;
	result->comment = comment;
	return result;
}

} // namespace duckdb
