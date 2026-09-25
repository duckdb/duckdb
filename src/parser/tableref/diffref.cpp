#include "duckdb/parser/tableref/diffref.hpp"
#include "duckdb/parser/tableref/subqueryref.hpp"
#include "duckdb/parser/keyword_helper.hpp"
#include "duckdb/common/string_util.hpp"

namespace duckdb {

DiffRef::DiffRef() : TableRef(TableReferenceType::DIFF_REF) {
}

static string DiffSideToString(const TableRef &side) {
	if (side.type == TableReferenceType::SUBQUERY) {
		return "(" + side.Cast<SubqueryRef>().subquery->ToString() + ")";
	}
	return side.ToString();
}

string DiffRef::ToString() const {
	string result = "(DIFF " + DiffSideToString(*old_side) + ", " + DiffSideToString(*new_side);
	if (!key.empty()) {
		vector<string> quoted;
		for (auto &name : key) {
			quoted.push_back(KeywordHelper::WriteOptionallyQuoted(name));
		}
		result += " KEY (" + StringUtil::Join(quoted, ", ") + ")";
	}
	if (cells) {
		result += " CELLS";
	}
	return result + ")";
}

bool DiffRef::Equals(const TableRef &other_p) const {
	if (!TableRef::Equals(other_p)) {
		return false;
	}
	auto &other = other_p.Cast<DiffRef>();
	return old_side->Equals(*other.old_side) && new_side->Equals(*other.new_side) && key == other.key &&
	       cells == other.cells;
}

unique_ptr<TableRef> DiffRef::Copy() {
	auto copy = make_uniq<DiffRef>();
	copy->old_side = old_side->Copy();
	copy->new_side = new_side->Copy();
	copy->key = key;
	copy->cells = cells;
	CopyProperties(*copy);
	return std::move(copy);
}

} // namespace duckdb
