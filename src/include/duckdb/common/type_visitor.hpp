//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/type_visitor.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/types.hpp"

namespace duckdb {

struct TypeVisitor {
	static constexpr idx_t MAX_TYPE_RECURSION_DEPTH = 1000;
	template <class F>
	static bool Contains(const LogicalType &type, F &&predicate);

	static bool Contains(const LogicalType &type, LogicalTypeId type_id);

	template <class F>
	static LogicalType VisitReplace(const LogicalType &type, F &&func);

private:
	template <class F>
	static LogicalType VisitReplaceInternal(const LogicalType &type, F &&func);

	template <class F>
	static bool ContainsInternal(const LogicalType &type, F &&predicate);
};

struct VisitorFrame {
	LogicalType original_type;
	idx_t expected_children_count;
};

template <class F>
inline LogicalType TypeVisitor::VisitReplace(const LogicalType &type, F &&func) {
	return VisitReplaceInternal(type, func);
}

template <typename F>
inline LogicalType TypeVisitor::VisitReplaceInternal(const LogicalType &type, F &&func) {
	std::vector<VisitorFrame> postfix_stack;
	std::vector<LogicalType> worklist;
	worklist.push_back(type);

	// Phase 1: Post-order expansion via Worklist
	while (!worklist.empty()) {
		LogicalType current = std::move(worklist.back());
		worklist.pop_back();

		if (!current.AuxInfo()) {
			postfix_stack.push_back({std::move(current), 0});
			continue;
		}
		switch (current.id()) {
		case LogicalTypeId::STRUCT:
		case LogicalTypeId::TUPLE: {
			auto children = StructType::GetChildTypes(current);
			idx_t child_count = children.size();
			postfix_stack.push_back({std::move(current), child_count});
			for (auto &child : children) {
				worklist.push_back(std::move(child.second));
			}
			break;
		}
		case LogicalTypeId::UNION: {
			auto children = UnionType::CopyMemberTypes(current);
			idx_t child_count = children.size();

			for (auto &child : children) {
				worklist.push_back(std::move(child.second));
			}
			postfix_stack.push_back({std::move(current), child_count});
			break;
		}
		case LogicalTypeId::MAP: {
			worklist.push_back(MapType::KeyType(current));
			worklist.push_back(MapType::ValueType(current));
			postfix_stack.push_back({std::move(current), 2});
			break;
		}
		case LogicalTypeId::LIST: {
			worklist.push_back(ListType::GetChildType(current));
			postfix_stack.push_back({std::move(current), 1});
			break;
		}
		case LogicalTypeId::ARRAY: {
			worklist.push_back(ArrayType::GetChildType(current));
			postfix_stack.push_back({std::move(current), 1});
			break;
		}
		default:
			postfix_stack.push_back({std::move(current), 0});
			break;
		}
	}

	// Phase 2: Post-order evaluation & Reconstruction using result_stack
	std::vector<LogicalType> result_stack;

	while (!postfix_stack.empty()) {
		auto frame = std::move(postfix_stack.back());
		postfix_stack.pop_back();

		if (frame.expected_children_count == 0) {
			result_stack.push_back(func(frame.original_type));
			continue;
		}

		switch (frame.original_type.id()) {
		case LogicalTypeId::STRUCT: {
			auto children = StructType::GetChildTypes(frame.original_type);
			for (idx_t i = 0; i < frame.expected_children_count; ++i) {
				children[i].second = std::move(result_stack.back());
				result_stack.pop_back();
			}
			result_stack.push_back(func(LogicalType::STRUCT(std::move(children))));
			break;
		}
		case LogicalTypeId::TUPLE: {
			auto children = StructType::GetChildTypes(frame.original_type);
			for (idx_t i = 0; i < frame.expected_children_count; ++i) {
				children[i].second = std::move(result_stack.back());
				result_stack.pop_back();
			}
			result_stack.push_back(func(LogicalType::TUPLE(std::move(children))));
			break;
		}
		case LogicalTypeId::UNION: {
			auto children = UnionType::CopyMemberTypes(frame.original_type);
			for (idx_t i = 0; i < frame.expected_children_count; ++i) {
				children[i].second = std::move(result_stack.back());
				result_stack.pop_back();
			}
			result_stack.push_back(func(LogicalType::UNION(std::move(children))));
			break;
		}
		case LogicalTypeId::MAP: {
			auto value_type = std::move(result_stack.back());
			result_stack.pop_back();
			auto key_type = std::move(result_stack.back());
			result_stack.pop_back();
			result_stack.push_back(func(LogicalType::MAP(std::move(key_type), std::move(value_type))));
			break;
		}
		case LogicalTypeId::LIST: {
			auto child_type = std::move(result_stack.back());
			result_stack.pop_back();
			result_stack.push_back(func(LogicalType::LIST(std::move(child_type))));
			break;
		}
		case LogicalTypeId::ARRAY: {
			auto child_type = std::move(result_stack.back());
			result_stack.pop_back();
			idx_t array_size = ArrayType::GetSize(frame.original_type);
			result_stack.push_back(func(LogicalType::ARRAY(std::move(child_type), array_size)));
			break;
		}
		default:
			result_stack.push_back(func(std::move(frame.original_type)));
			break;
		}
	}
	D_ASSERT(result_stack.size() == 1);
	return result_stack.back();
}

template <class F>
inline bool TypeVisitor::Contains(const LogicalType &type, F &&predicate) {
	return ContainsInternal(type, predicate);
}

template <class F>
inline bool TypeVisitor::ContainsInternal(const LogicalType &root_type, F &&predicate) {
	std::vector<reference<const LogicalType>> worklist;
	worklist.push_back(root_type);

	while (!worklist.empty()) {
		const auto &type = worklist.back().get();
		worklist.pop_back();

		if (predicate(type)) {
			return true;
		}

		if (!type.AuxInfo()) {
			continue;
		}

		switch (type.id()) {
		case LogicalTypeId::STRUCT:
		case LogicalTypeId::TUPLE: {
			const auto &children = StructType::GetChildTypes(type);
			for (const auto &child : children) {
				worklist.push_back(child.second);
			}
			break;
		}
		case LogicalTypeId::UNION: {
			const auto member_count = UnionType::GetMemberCount(type);
			for (idx_t i = 0; i < member_count; ++i) {
				worklist.push_back(UnionType::GetMemberType(type, i));
			}
			break;
		}
		case LogicalTypeId::LIST: {
			worklist.push_back(ListType::GetChildType(type));
			break;
		}
		case LogicalTypeId::ARRAY: {
			worklist.push_back(ArrayType::GetChildType(type));
			break;
		}
		case LogicalTypeId::MAP: {
			worklist.push_back(MapType::KeyType(type));
			worklist.push_back(MapType::ValueType(type));
			break;
		}
		default:
			break;
		}
	}

	return false;
}

inline bool TypeVisitor::Contains(const LogicalType &type, LogicalTypeId type_id) {
	return Contains(type, [&](const LogicalType &ty) { return ty.id() == type_id; });
}

} // namespace duckdb
