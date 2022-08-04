#include "duckdb/common/types/data_chunk.hpp"

#include "duckdb/common/arrow/arrow.hpp"
#include "duckdb/common/arrow/arrow_converter.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/common/serializer.hpp"
#include "duckdb/common/types/interval.hpp"
#include "duckdb/common/types/sel_cache.hpp"
#include "duckdb/common/types/vector_cache.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/common/vector.hpp"
#include <list>
#include "duckdb/common/arrow/arrow_appender.hpp"

namespace duckdb {

void ArrowConverter::ToArrowArray(DataChunk &input, ArrowArray *out_array) {
	ArrowAppender appender(input.GetTypes(), input.size());
	appender.Append(input);
	*out_array = appender.Finalize();
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
	std::list<std::vector<ArrowSchema>> nested_children;
	std::list<std::vector<ArrowSchema *>> nested_children_ptr;
	//! This holds strings created to represent decimal types
	vector<unique_ptr<char[]>> owned_type_names;
};

static void ReleaseDuckDBArrowSchema(ArrowSchema *schema) {
	if (!schema || !schema->release) {
		return;
	}
	schema->release = nullptr;
	auto holder = static_cast<DuckDBArrowSchemaHolder *>(schema->private_data);
	delete holder;
}

void InitializeChild(ArrowSchema &child, const string &name = "") {
	//! Child is cleaned up by parent
	child.private_data = nullptr;
	child.release = ReleaseDuckDBArrowSchema;

	//! Store the child schema
	child.flags = ARROW_FLAG_NULLABLE;
	child.name = name.c_str();
	child.n_children = 0;
	child.children = nullptr;
	child.metadata = nullptr;
	child.dictionary = nullptr;
}
void SetArrowFormat(DuckDBArrowSchemaHolder &root_holder, ArrowSchema &child, const LogicalType &type,
                    string &config_timezone);

void SetArrowMapFormat(DuckDBArrowSchemaHolder &root_holder, ArrowSchema &child, const LogicalType &type,
                       string &config_timezone) {
	child.format = "+m";
	//! Map has one child which is a struct
	child.n_children = 1;
	root_holder.nested_children.emplace_back();
	root_holder.nested_children.back().resize(1);
	root_holder.nested_children_ptr.emplace_back();
	root_holder.nested_children_ptr.back().push_back(&root_holder.nested_children.back()[0]);
	InitializeChild(root_holder.nested_children.back()[0]);
	child.children = &root_holder.nested_children_ptr.back()[0];
	child.children[0]->name = "entries";
	child_list_t<LogicalType> struct_child_types;
	struct_child_types.push_back(std::make_pair("key", ListType::GetChildType(StructType::GetChildType(type, 0))));
	struct_child_types.push_back(std::make_pair("value", ListType::GetChildType(StructType::GetChildType(type, 1))));
	auto struct_type = LogicalType::STRUCT(move(struct_child_types));
	SetArrowFormat(root_holder, *child.children[0], struct_type, config_timezone);
}

void SetArrowFormat(DuckDBArrowSchemaHolder &root_holder, ArrowSchema &child, const LogicalType &type,
                    string &config_timezone) {
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
	case LogicalTypeId::JSON:
	case LogicalTypeId::VARCHAR:
		child.format = "u";
		break;
	case LogicalTypeId::DATE:
		child.format = "tdD";
		break;
	case LogicalTypeId::TIME:
	case LogicalTypeId::TIME_TZ:
		child.format = "ttu";
		break;
	case LogicalTypeId::TIMESTAMP:
		child.format = "tsu:";
		break;
	case LogicalTypeId::TIMESTAMP_TZ: {
		string format = "tsu:" + config_timezone;
		unique_ptr<char[]> format_ptr = unique_ptr<char[]>(new char[format.size() + 1]);
		for (size_t i = 0; i < format.size(); i++) {
			format_ptr[i] = format[i];
		}
		format_ptr[format.size()] = '\0';
		root_holder.owned_type_names.push_back(move(format_ptr));
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
		child.format = "tDm";
		break;
	case LogicalTypeId::DECIMAL: {
		uint8_t width, scale;
		type.GetDecimalProperties(width, scale);
		string format = "d:" + to_string(width) + "," + to_string(scale);
		unique_ptr<char[]> format_ptr = unique_ptr<char[]>(new char[format.size() + 1]);
		for (size_t i = 0; i < format.size(); i++) {
			format_ptr[i] = format[i];
		}
		format_ptr[format.size()] = '\0';
		root_holder.owned_type_names.push_back(move(format_ptr));
		child.format = root_holder.owned_type_names.back().get();
		break;
	}
	case LogicalTypeId::SQLNULL: {
		child.format = "n";
		break;
	}
	case LogicalTypeId::BLOB: {
		child.format = "z";
		break;
	}
	case LogicalTypeId::LIST: {
		child.format = "+l";
		child.n_children = 1;
		root_holder.nested_children.emplace_back();
		root_holder.nested_children.back().resize(1);
		root_holder.nested_children_ptr.emplace_back();
		root_holder.nested_children_ptr.back().push_back(&root_holder.nested_children.back()[0]);
		InitializeChild(root_holder.nested_children.back()[0]);
		child.children = &root_holder.nested_children_ptr.back()[0];
		child.children[0]->name = "l";
		SetArrowFormat(root_holder, **child.children, ListType::GetChildType(type), config_timezone);
		break;
	}
	case LogicalTypeId::STRUCT: {
		child.format = "+s";
		auto &child_types = StructType::GetChildTypes(type);
		child.n_children = child_types.size();
		root_holder.nested_children.emplace_back();
		root_holder.nested_children.back().resize(child_types.size());
		root_holder.nested_children_ptr.emplace_back();
		root_holder.nested_children_ptr.back().resize(child_types.size());
		for (idx_t type_idx = 0; type_idx < child_types.size(); type_idx++) {
			root_holder.nested_children_ptr.back()[type_idx] = &root_holder.nested_children.back()[type_idx];
		}
		child.children = &root_holder.nested_children_ptr.back()[0];
		for (size_t type_idx = 0; type_idx < child_types.size(); type_idx++) {

			InitializeChild(*child.children[type_idx]);

			auto &struct_col_name = child_types[type_idx].first;
			unique_ptr<char[]> name_ptr = unique_ptr<char[]>(new char[struct_col_name.size() + 1]);
			for (size_t i = 0; i < struct_col_name.size(); i++) {
				name_ptr[i] = struct_col_name[i];
			}
			name_ptr[struct_col_name.size()] = '\0';
			root_holder.owned_type_names.push_back(move(name_ptr));

			child.children[type_idx]->name = root_holder.owned_type_names.back().get();
			SetArrowFormat(root_holder, *child.children[type_idx], child_types[type_idx].second, config_timezone);
		}
		break;
	}
	case LogicalTypeId::MAP: {
		SetArrowMapFormat(root_holder, child, type, config_timezone);
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
		InitializeChild(root_holder.nested_children.back()[0]);
		child.dictionary = root_holder.nested_children_ptr.back()[0];
		child.dictionary->format = "u";
		break;
	}
	default:
		throw InternalException("Unsupported Arrow type " + type.ToString());
	}
}

void ArrowConverter::ToArrowSchema(ArrowSchema *out_schema, vector<LogicalType> &types, vector<string> &names,
                                   string &config_timezone) {
	D_ASSERT(out_schema);
	D_ASSERT(types.size() == names.size());
	idx_t column_count = types.size();
	// Allocate as unique_ptr first to cleanup properly on error
	auto root_holder = make_unique<DuckDBArrowSchemaHolder>();

	// Allocate the children
	root_holder->children.resize(column_count);
	root_holder->children_ptrs.resize(column_count, nullptr);
	for (size_t i = 0; i < column_count; ++i) {
		root_holder->children_ptrs[i] = &root_holder->children[i];
	}
	out_schema->children = root_holder->children_ptrs.data();
	out_schema->n_children = column_count;

	// Store the schema
	out_schema->format = "+s"; // struct apparently
	out_schema->flags = 0;
	out_schema->metadata = nullptr;
	out_schema->name = "duckdb_query_result";
	out_schema->dictionary = nullptr;

	// Configure all child schemas
	for (idx_t col_idx = 0; col_idx < column_count; col_idx++) {

		auto &child = root_holder->children[col_idx];
		InitializeChild(child, names[col_idx]);
		SetArrowFormat(*root_holder, child, types[col_idx], config_timezone);
	}

	// Release ownership to caller
	out_schema->private_data = root_holder.release();
	out_schema->release = ReleaseDuckDBArrowSchema;
}

} // namespace duckdb
