#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/types/bit.hpp"
#include "duckdb/common/arrow/arrow.hpp"
#include "duckdb/common/arrow/arrow_converter.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/common/types/interval.hpp"
#include "duckdb/common/types/sel_cache.hpp"
#include "duckdb/common/types/vector_cache.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/common/vector.hpp"
#include <list>
#include "duckdb/common/arrow/arrow_appender.hpp"

namespace duckdb {

void ArrowConverter::ToArrowArray(DataChunk &input, ArrowArray *out_array, ClientProperties options) {
	ArrowAppender appender(input.GetTypes(), input.size(), std::move(options));
	appender.Append(input, 0, input.size(), input.size());
	*out_array = appender.Finalize();
}

unsafe_unique_array<char> AddName(const string &name) {
	auto name_ptr = make_unsafe_uniq_array<char>(name.size() + 1);
	for (size_t i = 0; i < name.size(); i++) {
		name_ptr[i] = name[i];
	}
	name_ptr[name.size()] = '\0';
	return name_ptr;
}

//===--------------------------------------------------------------------===//
// Arrow Schema
//===--------------------------------------------------------------------===//
struct DuckDBArrowSchemaHolder {
	// unused in children
	vector<ArrowSchema> children;
	// unused in children
	vector<ArrowSchema *> children_ptrs;
	//! used for nested structures
	std::list<vector<ArrowSchema>> nested_children;
	std::list<vector<ArrowSchema *>> nested_children_ptr;
	//! This holds strings created to represent decimal types
	vector<unsafe_unique_array<char>> owned_type_names;
	vector<unsafe_unique_array<char>> owned_column_names;
};

static void ReleaseDuckDBArrowSchema(ArrowSchema *schema) {
	if (!schema || !schema->release) {
		return;
	}
	schema->release = nullptr;
	auto holder = static_cast<DuckDBArrowSchemaHolder *>(schema->private_data);
	delete holder;
}

void InitializeChild(ArrowSchema &child, DuckDBArrowSchemaHolder &root_holder, const string &name = "") {
	//! Child is cleaned up by parent
	child.private_data = nullptr;
	child.release = ReleaseDuckDBArrowSchema;

	// Store the child schema
	child.flags = ARROW_FLAG_NULLABLE;
	root_holder.owned_type_names.push_back(AddName(name));

	child.name = root_holder.owned_type_names.back().get();
	child.n_children = 0;
	child.children = nullptr;
	child.metadata = nullptr;
	child.dictionary = nullptr;
}

void SetArrowFormat(DuckDBArrowSchemaHolder &root_holder, ArrowSchema &child, const LogicalType &type,
                    const ClientProperties &options);

void SetArrowMapFormat(DuckDBArrowSchemaHolder &root_holder, ArrowSchema &child, const LogicalType &type,
                       const ClientProperties &options) {
	child.format = "+m";
	//! Map has one child which is a struct
	child.n_children = 1;
	root_holder.nested_children.emplace_back();
	root_holder.nested_children.back().resize(1);
	root_holder.nested_children_ptr.emplace_back();
	root_holder.nested_children_ptr.back().push_back(&root_holder.nested_children.back()[0]);
	InitializeChild(root_holder.nested_children.back()[0], root_holder);
	child.children = &root_holder.nested_children_ptr.back()[0];
	child.children[0]->name = "entries";
	SetArrowFormat(root_holder, **child.children, ListType::GetChildType(type), options);
}

void SetArrowFormat(DuckDBArrowSchemaHolder &root_holder, ArrowSchema &child, const LogicalType &type,
                    const ClientProperties &options) {
	switch (type.id()) {
	case LogicalTypeId::BOOLEAN:
		child.format = "b";
		break;
	case LogicalTypeId::TINYINT:
		child.format = "c";
		break;
	case LogicalTypeId::SMALLINT:
		child.format = "s";
		break;
	case LogicalTypeId::INTEGER:
		child.format = "i";
		break;
	case LogicalTypeId::BIGINT:
		child.format = "l";
		break;
	case LogicalTypeId::UTINYINT:
		child.format = "C";
		break;
	case LogicalTypeId::USMALLINT:
		child.format = "S";
		break;
	case LogicalTypeId::UINTEGER:
		child.format = "I";
		break;
	case LogicalTypeId::UBIGINT:
		child.format = "L";
		break;
	case LogicalTypeId::FLOAT:
		child.format = "f";
		break;
	case LogicalTypeId::HUGEINT:
		child.format = "d:38,0";
		break;
	case LogicalTypeId::DOUBLE:
		child.format = "g";
		break;
	case LogicalTypeId::UUID:
	case LogicalTypeId::VARCHAR:
		if (options.produce_arrow_string_view) {
			child.format = "vu";
		} else {
			if (options.arrow_offset_size == ArrowOffsetSize::LARGE) {
				child.format = "U";
			} else {
				child.format = "u";
			}
		}
		break;
	case LogicalTypeId::DATE:
		child.format = "tdD";
		break;
	case LogicalTypeId::TIME_TZ:
	case LogicalTypeId::TIME:
		child.format = "ttu";
		break;
	case LogicalTypeId::TIMESTAMP:
		child.format = "tsu:";
		break;
	case LogicalTypeId::TIMESTAMP_TZ: {
		string format = "tsu:" + options.time_zone;
		root_holder.owned_type_names.push_back(AddName(format));
		child.format = root_holder.owned_type_names.back().get();
		break;
	}
	case LogicalTypeId::TIMESTAMP_SEC:
		child.format = "tss:";
		break;
	case LogicalTypeId::TIMESTAMP_NS:
		child.format = "tsn:";
		break;
	case LogicalTypeId::TIMESTAMP_MS:
		child.format = "tsm:";
		break;
	case LogicalTypeId::INTERVAL:
		child.format = "tin";
		break;
	case LogicalTypeId::DECIMAL: {
		uint8_t width, scale;
		type.GetDecimalProperties(width, scale);
		string format = "d:" + to_string(width) + "," + to_string(scale);
		root_holder.owned_type_names.push_back(AddName(format));
		child.format = root_holder.owned_type_names.back().get();
		break;
	}
	case LogicalTypeId::SQLNULL: {
		child.format = "n";
		break;
	}
	case LogicalTypeId::BLOB:
	case LogicalTypeId::BIT: {
		if (options.arrow_offset_size == ArrowOffsetSize::LARGE) {
			child.format = "Z";
		} else {
			child.format = "z";
		}
		break;
	}
	case LogicalTypeId::LIST: {
		if (options.arrow_use_list_view) {
			if (options.arrow_offset_size == ArrowOffsetSize::LARGE) {
				child.format = "+vL";
			} else {
				child.format = "+vl";
			}
		} else {
			if (options.arrow_offset_size == ArrowOffsetSize::LARGE) {
				child.format = "+L";
			} else {
				child.format = "+l";
			}
		}
		child.n_children = 1;
		root_holder.nested_children.emplace_back();
		root_holder.nested_children.back().resize(1);
		root_holder.nested_children_ptr.emplace_back();
		root_holder.nested_children_ptr.back().push_back(&root_holder.nested_children.back()[0]);
		InitializeChild(root_holder.nested_children.back()[0], root_holder);
		child.children = &root_holder.nested_children_ptr.back()[0];
		child.children[0]->name = "l";
		SetArrowFormat(root_holder, **child.children, ListType::GetChildType(type), options);
		break;
	}
	case LogicalTypeId::STRUCT: {
		child.format = "+s";
		auto &child_types = StructType::GetChildTypes(type);
		child.n_children = NumericCast<int64_t>(child_types.size());
		root_holder.nested_children.emplace_back();
		root_holder.nested_children.back().resize(child_types.size());
		root_holder.nested_children_ptr.emplace_back();
		root_holder.nested_children_ptr.back().resize(child_types.size());
		for (idx_t type_idx = 0; type_idx < child_types.size(); type_idx++) {
			root_holder.nested_children_ptr.back()[type_idx] = &root_holder.nested_children.back()[type_idx];
		}
		child.children = &root_holder.nested_children_ptr.back()[0];
		for (size_t type_idx = 0; type_idx < child_types.size(); type_idx++) {

			InitializeChild(*child.children[type_idx], root_holder);

			root_holder.owned_type_names.push_back(AddName(child_types[type_idx].first));

			child.children[type_idx]->name = root_holder.owned_type_names.back().get();
			SetArrowFormat(root_holder, *child.children[type_idx], child_types[type_idx].second, options);
		}
		break;
	}
	case LogicalTypeId::ARRAY: {
		auto array_size = ArrayType::GetSize(type);
		auto &child_type = ArrayType::GetChildType(type);
		auto format = "+w:" + to_string(array_size);
		root_holder.owned_type_names.push_back(AddName(format));
		child.format = root_holder.owned_type_names.back().get();

		child.n_children = 1;
		root_holder.nested_children.emplace_back();
		root_holder.nested_children.back().resize(1);
		root_holder.nested_children_ptr.emplace_back();
		root_holder.nested_children_ptr.back().push_back(&root_holder.nested_children.back()[0]);
		InitializeChild(root_holder.nested_children.back()[0], root_holder);
		child.children = &root_holder.nested_children_ptr.back()[0];
		SetArrowFormat(root_holder, **child.children, child_type, options);
		break;
	}
	case LogicalTypeId::MAP: {
		SetArrowMapFormat(root_holder, child, type, options);
		break;
	}
	case LogicalTypeId::UNION: {
		std::string format = "+us:";

		auto &child_types = UnionType::CopyMemberTypes(type);
		child.n_children = NumericCast<int64_t>(child_types.size());
		root_holder.nested_children.emplace_back();
		root_holder.nested_children.back().resize(child_types.size());
		root_holder.nested_children_ptr.emplace_back();
		root_holder.nested_children_ptr.back().resize(child_types.size());
		for (idx_t type_idx = 0; type_idx < child_types.size(); type_idx++) {
			root_holder.nested_children_ptr.back()[type_idx] = &root_holder.nested_children.back()[type_idx];
		}
		child.children = &root_holder.nested_children_ptr.back()[0];
		for (size_t type_idx = 0; type_idx < child_types.size(); type_idx++) {

			InitializeChild(*child.children[type_idx], root_holder);

			root_holder.owned_type_names.push_back(AddName(child_types[type_idx].first));

			child.children[type_idx]->name = root_holder.owned_type_names.back().get();
			SetArrowFormat(root_holder, *child.children[type_idx], child_types[type_idx].second, options);

			format += to_string(type_idx) + ",";
		}

		format.pop_back();

		root_holder.owned_type_names.push_back(AddName(format));
		child.format = root_holder.owned_type_names.back().get();

		break;
	}
	case LogicalTypeId::ENUM: {
		// TODO what do we do with pointer enums here?
		switch (EnumType::GetPhysicalType(type)) {
		case PhysicalType::UINT8:
			child.format = "C";
			break;
		case PhysicalType::UINT16:
			child.format = "S";
			break;
		case PhysicalType::UINT32:
			child.format = "I";
			break;
		default:
			throw InternalException("Unsupported Enum Internal Type");
		}
		root_holder.nested_children.emplace_back();
		root_holder.nested_children.back().resize(1);
		root_holder.nested_children_ptr.emplace_back();
		root_holder.nested_children_ptr.back().push_back(&root_holder.nested_children.back()[0]);
		InitializeChild(root_holder.nested_children.back()[0], root_holder);
		child.dictionary = root_holder.nested_children_ptr.back()[0];
		child.dictionary->format = "u";
		break;
	}
	default:
		throw NotImplementedException("Unsupported Arrow type " + type.ToString());
	}
}

void ArrowConverter::ToArrowSchema(ArrowSchema *out_schema, const vector<LogicalType> &types,
                                   const vector<string> &names, const ClientProperties &options) {
	D_ASSERT(out_schema);
	D_ASSERT(types.size() == names.size());
	idx_t column_count = types.size();
	// Allocate as unique_ptr first to cleanup properly on error
	auto root_holder = make_uniq<DuckDBArrowSchemaHolder>();

	// Allocate the children
	root_holder->children.resize(column_count);
	root_holder->children_ptrs.resize(column_count, nullptr);
	for (size_t i = 0; i < column_count; ++i) {
		root_holder->children_ptrs[i] = &root_holder->children[i];
	}
	out_schema->children = root_holder->children_ptrs.data();
	out_schema->n_children = NumericCast<int64_t>(column_count);

	// Store the schema
	out_schema->format = "+s"; // struct apparently
	out_schema->flags = 0;
	out_schema->metadata = nullptr;
	out_schema->name = "duckdb_query_result";
	out_schema->dictionary = nullptr;

	// Configure all child schemas
	for (idx_t col_idx = 0; col_idx < column_count; col_idx++) {
		root_holder->owned_column_names.push_back(AddName(names[col_idx]));
		auto &child = root_holder->children[col_idx];
		InitializeChild(child, *root_holder, names[col_idx]);
		SetArrowFormat(*root_holder, child, types[col_idx], options);
	}

	// Release ownership to caller
	out_schema->private_data = root_holder.release();
	out_schema->release = ReleaseDuckDBArrowSchema;
}

} // namespace duckdb
