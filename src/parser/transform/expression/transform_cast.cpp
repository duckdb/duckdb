#include <string>
#include <utility>

#include "duckdb/parser/expression/cast_expression.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/transformer.hpp"
#include "duckdb/common/types/blob.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/common/numeric_utils.hpp"
#include "duckdb/common/optional_idx.hpp"
#include "duckdb/common/optional_ptr.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/typedefs.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/common/unique_ptr.hpp"
#include "duckdb/function/cast/default_casts.hpp"
#include "duckdb/parser/parsed_expression.hpp"
#include "nodes/nodes.hpp"
#include "nodes/parsenodes.hpp"

namespace duckdb {

unique_ptr<ParsedExpression> Transformer::TransformTypeCast(duckdb_libpgquery::PGTypeCast &root) {
	// get the type to cast to
	auto type_name = root.typeName;
	LogicalType target_type = TransformTypeName(*type_name);

	// check for a constant BLOB value, then return ConstantExpression with BLOB
	if (!root.tryCast && target_type == LogicalType::BLOB && root.arg->type == duckdb_libpgquery::T_PGAConst) {
		auto c = PGPointerCast<duckdb_libpgquery::PGAConst>(root.arg);
		if (c->val.type == duckdb_libpgquery::T_PGString) {
			CastParameters parameters;
			if (root.location >= 0) {
				parameters.query_location = NumericCast<idx_t>(root.location);
			}
			auto blob_data = Blob::ToBlob(string(c->val.val.str), parameters);
			auto result = make_uniq<ConstantExpression>(Value::BLOB_RAW(blob_data));
			SetQueryLocation(*result, root.location);
			return std::move(result);
		}
	}
	// transform the expression node
	auto expression = TransformExpression(root.arg);
	bool try_cast = root.tryCast;

	// now create a cast operation
	auto result = make_uniq<CastExpression>(target_type, std::move(expression), try_cast);
	SetQueryLocation(*result, root.location);
	return std::move(result);
}

} // namespace duckdb
