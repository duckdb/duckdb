#include "duckdb/parser/tableref/match_recognize_ref.hpp"

#include "duckdb/common/serializer/serializer.hpp"
#include "duckdb/common/serializer/deserializer.hpp"

namespace duckdb {

string MatchRecognizeRef::ToString() const {
	string result = "MATCH_RECOGNIZE( todo )";
	return BaseToString(result, column_name_alias);
}

MatchRecognizeRef::MatchRecognizeRef() : TableRef(TableReferenceType::MATCH_RECOGNIZE) {
}

bool MatchRecognizeRef::Equals(const TableRef &other_p) const {
	D_ASSERT(0); // TODO: is this correct? config should be equal?!
	if (!TableRef::Equals(other_p)) {
		return false;
	}
	auto &other = other_p.Cast<MatchRecognizeRef>();
	return input->Equals(*other.input); //&& config->Equals(*other.config);
}

unique_ptr<TableRef> MatchRecognizeRef::Copy() {
	D_ASSERT(0);
	// TODO: probably much more is needed here.
	auto copy = make_uniq<MatchRecognizeRef>(input->Copy(), make_uniq<MatchRecognizeConfig>(MatchRecognizeConfig()));
	copy->alias = alias;
	CopyProperties(*copy);
	return nullptr;
}

} // namespace duckdb
