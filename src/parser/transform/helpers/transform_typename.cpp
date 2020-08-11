#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/parser/transformer.hpp"
#include "duckdb/common/types/decimal.hpp"

using namespace std;

namespace duckdb {

SQLType Transformer::TransformTypeName(PGTypeName *type_name) {
	auto name = (reinterpret_cast<PGValue *>(type_name->names->tail->data.ptr_value)->val.str);
	// transform it to the SQL type
	SQLType base_type = TransformStringToSQLType(name);
	// check any modifiers
	int modifier_idx = 0;
	if (type_name->typmods) {
		for (auto node = type_name->typmods->head; node; node = node->next) {
			auto &const_val = *((PGAConst*)node->data.ptr_value);
			if (const_val.type != T_PGAConst || const_val.val.type != T_PGInteger) {
				throw ParserException("Expected an integer constant as type modifier");
			}
			if (const_val.val.val.ival < 0) {
				throw ParserException("Negative modifier not supported");
			}
			if (modifier_idx == 0) {
				base_type.width = const_val.val.val.ival;
			} else if (modifier_idx == 1) {
				base_type.scale = const_val.val.val.ival;
			} else {
				throw ParserException("A maximum of two modifiers is supported");
			}
			modifier_idx++;
		}
	}
	switch(base_type.id) {
	case SQLTypeId::VARCHAR:
		if (modifier_idx > 1) {
			throw ParserException("VARCHAR only supports a single modifier");
		}
		break;
	case SQLTypeId::DECIMAL:
		if (modifier_idx == 1) {
			// only width is provided: set scale to 0
			base_type.scale = 0;
		}
		if (base_type.width > Decimal::MAX_WIDTH_DECIMAL) {
			throw ParserException("Width bigger than %d is not supported!", (int) Decimal::MAX_WIDTH_DECIMAL);
		}
		if (base_type.scale > base_type.width) {
			throw ParserException("Scale cannot be bigger than width");
		}
		break;
	default:
		if (modifier_idx > 0) {
			throw ParserException("Type %s does not support any modifiers!", SQLTypeIdToString(base_type.id).c_str());
		}
	}

	return base_type;
}

}
