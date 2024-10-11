#include "duckdb/common/tree_renderer/json_tree_renderer.hpp"

#include "duckdb/common/pair.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/execution/operator/aggregate/physical_hash_aggregate.hpp"
#include "duckdb/execution/operator/join/physical_delim_join.hpp"
#include "duckdb/execution/operator/scan/physical_positional_scan.hpp"
#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/parallel/pipeline.hpp"
#include "duckdb/planner/logical_operator.hpp"
#include "utf8proc_wrapper.hpp"

#include "yyjson.hpp"

#include <sstream>

using namespace duckdb_yyjson; // NOLINT

namespace duckdb {

string JSONTreeRenderer::ToString(const LogicalOperator &op) {
	std::stringstream ss;
	Render(op, ss);
	return ss.str();
}

string JSONTreeRenderer::ToString(const PhysicalOperator &op) {
	std::stringstream ss;
	Render(op, ss);
	return ss.str();
}

string JSONTreeRenderer::ToString(const ProfilingNode &op) {
	std::stringstream ss;
	Render(op, ss);
	return ss.str();
}

string JSONTreeRenderer::ToString(const Pipeline &op) {
	std::stringstream ss;
	Render(op, ss);
	return ss.str();
}

void JSONTreeRenderer::Render(const LogicalOperator &op, std::ostream &ss) {
	auto tree = RenderTree::CreateRenderTree(op);
	ToStream(*tree, ss);
}

void JSONTreeRenderer::Render(const PhysicalOperator &op, std::ostream &ss) {
	auto tree = RenderTree::CreateRenderTree(op);
	ToStream(*tree, ss);
}

void JSONTreeRenderer::Render(const ProfilingNode &op, std::ostream &ss) {
	auto tree = RenderTree::CreateRenderTree(op);
	ToStream(*tree, ss);
}

void JSONTreeRenderer::Render(const Pipeline &op, std::ostream &ss) {
	auto tree = RenderTree::CreateRenderTree(op);
	ToStream(*tree, ss);
}

static yyjson_mut_val *RenderRecursive(yyjson_mut_doc *doc, RenderTree &tree, idx_t x, idx_t y) {
	auto node_p = tree.GetNode(x, y);
	D_ASSERT(node_p);
	auto &node = *node_p;

	auto object = yyjson_mut_obj(doc);
	auto children = yyjson_mut_arr(doc);
	for (auto &child_pos : node.child_positions) {
		auto child_object = RenderRecursive(doc, tree, child_pos.x, child_pos.y);
		yyjson_mut_arr_append(children, child_object);
	}
	yyjson_mut_obj_add_str(doc, object, "name", node.name.c_str());
	yyjson_mut_obj_add_val(doc, object, "children", children);
	auto extra_info = yyjson_mut_obj(doc);
	for (auto &it : node.extra_text) {
		auto &key = it.first;
		auto &value = it.second;
		auto splits = StringUtil::Split(value, "\n");
		if (splits.size() > 1) {
			auto list_items = yyjson_mut_arr(doc);
			for (auto &split : splits) {
				yyjson_mut_arr_add_strcpy(doc, list_items, split.c_str());
			}
			yyjson_mut_obj_add_val(doc, extra_info, key.c_str(), list_items);
		} else {
			yyjson_mut_obj_add_strcpy(doc, extra_info, key.c_str(), value.c_str());
		}
	}
	yyjson_mut_obj_add_val(doc, object, "extra_info", extra_info);
	return object;
}

void JSONTreeRenderer::ToStreamInternal(RenderTree &root, std::ostream &ss) {
	auto doc = yyjson_mut_doc_new(nullptr);
	auto result_obj = yyjson_mut_arr(doc);
	yyjson_mut_doc_set_root(doc, result_obj);

	auto plan = RenderRecursive(doc, root, 0, 0);
	yyjson_mut_arr_append(result_obj, plan);

	auto data = yyjson_mut_val_write_opts(result_obj, YYJSON_WRITE_ALLOW_INF_AND_NAN | YYJSON_WRITE_PRETTY, nullptr,
	                                      nullptr, nullptr);
	if (!data) {
		yyjson_mut_doc_free(doc);
		throw InternalException("The plan could not be rendered as JSON, yyjson failed");
	}
	ss << string(data);
	free(data);
	yyjson_mut_doc_free(doc);
}

} // namespace duckdb
