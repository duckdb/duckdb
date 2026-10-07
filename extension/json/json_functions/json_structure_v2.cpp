#include "duckdb/common/assert.hpp"
#include "duckdb/common/enum_util.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/common/optional_ptr.hpp"
#include "duckdb/common/typedefs.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/unique_ptr.hpp"
#include "duckdb/function/function_set.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "json_common.hpp"
#include "json_executors.hpp"
#include "json_functions.hpp"
#include "json_structure.hpp"
#include "yyjson.hpp"
#include <cstddef>
#include <optional>
#include <stack>

namespace duckdb {

namespace {

//===--------------------------------------------------------------------===//
// Extraction
//===--------------------------------------------------------------------===//
struct StructureNode {
	bool MergeType(LogicalTypeId type_p);

	LogicalTypeId type = LogicalTypeId::INVALID;
	bool incompatible = false;
	optional<string> key;
	json_key_map_t<idx_t> key_map;
	vector<reference<StructureNode>> children;
};

struct Frame {
	Frame(optional_ptr<yyjson_val> val_p, StructureNode &node_p) : val(val_p), node(node_p) {
	}

	optional_ptr<yyjson_val> val;
	reference<StructureNode> node;
	optional<yyjson_arr_iter> arr_it;
	optional<yyjson_obj_iter> obj_it;
};

using TreeStorage = vector<unique_ptr<StructureNode>>;

optional<Frame> HandleArrayFrame(TreeStorage &nodes, Frame &frame) {
	D_ASSERT(yyjson_is_arr(frame.val.get()));
	auto &node = frame.node.get();
	if (node.incompatible) {
		return nullopt;
	}

	if (!frame.arr_it) {
		if (!node.MergeType(LogicalTypeId::LIST)) {
			return nullopt;
		}

		frame.arr_it = yyjson_arr_iter_with(frame.val.get());
		if (node.children.empty()) {
			nodes.push_back(make_uniq<StructureNode>());
			node.children.emplace_back(*nodes.back());
		}
	}
	// All array elements contribute to one shared schema node.
	D_ASSERT(node.children.size() == 1);
	auto &child = node.children[0].get();
	if (child.incompatible) {
		return nullopt;
	}

	optional_ptr<yyjson_val> next = yyjson_arr_iter_next(&*frame.arr_it);
	if (!next) {
		return nullopt;
	}
	return Frame(next, child);
}

optional<Frame> HandleObjectFrame(TreeStorage &nodes, Frame &frame) {
	D_ASSERT(yyjson_is_obj(frame.val.get()));
	auto &node = frame.node.get();
	if (node.incompatible) {
		return nullopt;
	}

	if (!frame.obj_it) {
		if (!node.MergeType(LogicalTypeId::STRUCT)) {
			return nullopt;
		}

		frame.obj_it = yyjson_obj_iter_with(frame.val.get());
	}

	while (optional_ptr<yyjson_val> key = yyjson_obj_iter_next(&*frame.obj_it)) {
		const JSONKey lookup_key {unsafe_yyjson_get_str(key.get()), unsafe_yyjson_get_len(key.get())};
		auto entry = node.key_map.find(lookup_key);
		if (entry == node.key_map.end()) {
			nodes.push_back(make_uniq<StructureNode>());
			auto &child = *nodes.back();
			child.key.emplace(lookup_key.ptr, lookup_key.len);
			const JSONKey persistent_key {child.key->c_str(), child.key->length()};
			node.children.emplace_back(child);
			entry = node.key_map.emplace(persistent_key, node.children.size() - 1).first;
		}

		auto &child = node.children[entry->second].get();
		if (child.incompatible) {
			continue;
		}
		return Frame(yyjson_obj_iter_get_val(key.get()), child);
	}
	return nullopt;
}

bool IsNumeric(LogicalTypeId type) {
	return type == LogicalTypeId::DOUBLE || type == LogicalTypeId::UBIGINT || type == LogicalTypeId::BIGINT ||
	       type == LogicalTypeId::HUGEINT;
}

LogicalTypeId MaxNumericType(const LogicalTypeId &a, const LogicalTypeId &b) {
	D_ASSERT(a != b);
	if (a == LogicalTypeId::DOUBLE || b == LogicalTypeId::DOUBLE) {
		return LogicalTypeId::DOUBLE;
	}
	if (a == LogicalTypeId::HUGEINT || b == LogicalTypeId::HUGEINT) {
		return LogicalTypeId::HUGEINT;
	}
	// One is BIGINT and the other is UBIGINT - need HUGEINT to represent both ranges
	if ((a == LogicalTypeId::BIGINT && b == LogicalTypeId::UBIGINT) ||
	    (a == LogicalTypeId::UBIGINT && b == LogicalTypeId::BIGINT)) {
		return LogicalTypeId::HUGEINT;
	}
	return LogicalTypeId::BIGINT;
}

bool StructureNode::MergeType(LogicalTypeId type_p) {
	D_ASSERT(type_p != LogicalTypeId::INVALID);
	if (incompatible) {
		return false;
	}
	if (type == LogicalTypeId::INVALID || type == LogicalTypeId::SQLNULL) {
		type = type_p;
		return true;
	}
	if (type_p == LogicalTypeId::SQLNULL || type == type_p) {
		return true;
	}
	if (IsNumeric(type) && IsNumeric(type_p)) {
		type = MaxNumericType(type, type_p);
		return true;
	}

	incompatible = true;
	children.clear();
	key_map.clear();
	return false;
}

void HandleValueFrame(Frame &frame) {
	const auto val = frame.val.get();
	D_ASSERT(!yyjson_is_arr(val) && !yyjson_is_obj(val));
	frame.node.get().MergeType(JSONCommon::ValTypeToLogicalTypeId(val));
}

TreeStorage ExtractStructure(yyjson_val *val, const bool ignore_errors) {
	TreeStorage nodes;
	std::stack<Frame> stack;

	nodes.push_back(make_uniq<StructureNode>());
	stack.emplace(val, *nodes.back());

	while (!stack.empty()) {
		auto &frame = stack.top();

		switch (yyjson_get_tag(frame.val.get())) {
		case YYJSON_TYPE_ARR | YYJSON_SUBTYPE_NONE: {
			auto child = HandleArrayFrame(nodes, frame);
			if (child) {
				stack.push(std::move(*child));
			} else {
				stack.pop();
			}
			break;
		}
		case YYJSON_TYPE_OBJ | YYJSON_SUBTYPE_NONE: {
			auto child = HandleObjectFrame(nodes, frame);
			if (child) {
				stack.push(std::move(*child));
			} else {
				stack.pop();
			}
			break;
		}
		default: {
			HandleValueFrame(frame);
			stack.pop();
			break;
		}
		}
	}
	return nodes;
}

//===--------------------------------------------------------------------===//
// Convert
//===--------------------------------------------------------------------===//
yyjson_mut_val *ConvertStructure(const TreeStorage &nodes, yyjson_mut_doc *doc) {
	D_ASSERT(!nodes.empty());
	D_ASSERT(doc);
	struct ConversionFrame {
		explicit ConversionFrame(const StructureNode &node_p) : node(node_p) {
		}

		reference<const StructureNode> node;
		optional_ptr<yyjson_mut_val> value;
		idx_t child_index = 0;
	};

	optional_ptr<yyjson_mut_val> root;
	std::stack<ConversionFrame> stack;
	stack.emplace(*nodes.front());
	while (!stack.empty()) {
		auto &frame = stack.top();
		const auto &node = frame.node.get();
		if (!frame.value) {
			if (node.incompatible || (node.type == LogicalTypeId::STRUCT && node.children.empty())) {
				frame.value = yyjson_mut_str(doc, LogicalType::JSON_TYPE_NAME);
			} else if (node.type == LogicalTypeId::INVALID) {
				frame.value = yyjson_mut_str(doc, JSONCommon::TYPE_STRING_NULL);
			} else {
				switch (node.type) {
				case LogicalTypeId::LIST:
					D_ASSERT(node.children.size() == 1);
					frame.value = yyjson_mut_arr(doc);
					break;
				case LogicalTypeId::STRUCT:
					frame.value = yyjson_mut_obj(doc);
					break;
				default:
					frame.value = yyjson_mut_str(doc, EnumUtil::ToChars(node.type));
					break;
				}
			}
			if (!frame.value) {
				throw OutOfMemoryException("Failed to allocate JSON structure value");
			}
		}
		if (yyjson_mut_is_ctn(frame.value.get()) && frame.child_index < node.children.size()) {
			stack.emplace(node.children[frame.child_index++].get());
			continue;
		}

		auto value = frame.value;
		stack.pop();
		if (stack.empty()) {
			root = value;
			break;
		}
		auto &parent = stack.top();
		if (parent.node.get().type == LogicalTypeId::LIST) {
			yyjson_mut_arr_append(parent.value.get(), value.get());
		} else {
			D_ASSERT(parent.node.get().type == LogicalTypeId::STRUCT);
			D_ASSERT(node.key);
			optional_ptr<yyjson_mut_val> key = yyjson_mut_strncpy(doc, node.key->c_str(), node.key->length());
			if (!key) {
				throw OutOfMemoryException("Failed to allocate JSON structure key");
			}
			yyjson_mut_obj_add(parent.value.get(), key.get(), value.get());
		}
	}
	return root.get();
}

//===--------------------------------------------------------------------===//
// Function
//===--------------------------------------------------------------------===//
optional<string_t> JSONStructureFunction(yyjson_val *val, yyjson_alc *alc, Vector &) {
	const auto structure = ExtractStructure(val, true);
	auto doc = ConvertStructure(structure, yyjson_mut_doc_new(alc));
	return JSONCommon::WriteVal<yyjson_mut_val>(doc, alc);
}

void StructureFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	JSONExecutors::UnaryExecute<string_t>(args, state, result, JSONStructureFunction);
}

ScalarFunction GetStructureFunctionInternal(const LogicalType &input_type) {
	ScalarFunction fun({}, LogicalType::JSON(), nullptr, nullptr, nullptr);
	fun.GetSignature().AddParameter("json", input_type);
	return std::move(fun); // TODO: do we need move here?
}

} // namespace

ScalarFunctionSet JSONFunctions::GetStructureFunction() {
	ScalarFunctionSet fun_set("json_structure");

	fun_set.AddFunction(GetStructureFunctionInternal(LogicalType::JSON()));

	return fun_set;
}

} // namespace duckdb
