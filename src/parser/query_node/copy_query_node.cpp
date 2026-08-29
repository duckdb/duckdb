#include "duckdb/parser/query_node/copy_query_node.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/parser/parsed_data/copy_info.hpp"

namespace duckdb {

CopyQueryNode::CopyQueryNode(unique_ptr<ParseInfo> info_p)
    : QueryNode(QueryNodeType::COPY_QUERY_NODE), info(unique_ptr_cast<ParseInfo, CopyInfo>(std::move(info_p))) {
}

CopyQueryNode::~CopyQueryNode() {
}

string CopyQueryNode::ToString() const {
	D_ASSERT(info);
	auto result = info->ToString();
	if (StringUtil::EndsWith(result, ";")) {
		result.pop_back();
	}
	return result;
}

bool CopyQueryNode::Equals(const QueryNode *other_p) const {
	if (this == other_p) {
		return true;
	}
	if (!QueryNode::Equals(other_p)) {
		return false;
	}
	auto &other = other_p->Cast<CopyQueryNode>();
	return info->Equals(*other.info);
}

unique_ptr<QueryNode> CopyQueryNode::Copy() const {
	auto result = make_uniq<CopyQueryNode>(info->Copy());
	CopyProperties(*result);
	return std::move(result);
}

} // namespace duckdb
