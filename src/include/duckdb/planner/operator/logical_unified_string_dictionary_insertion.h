#pragma once

#include "duckdb/planner/logical_operator.hpp"

namespace duckdb {

class LogicalUnifiedStringDictionaryInsertion : public LogicalOperator {

public:
	static constexpr const LogicalOperatorType TYPE = LogicalOperatorType::LOGICAL_UNIFIED_STRING_DICTIONARY_INSERTION;

	explicit LogicalUnifiedStringDictionaryInsertion(vector<bool> cols_to_insert)
	    : LogicalOperator(LogicalOperatorType::LOGICAL_UNIFIED_STRING_DICTIONARY_INSERTION),
	      insert_to_usd(std::move(cols_to_insert)) {
	}

	vector<bool> insert_to_usd;

public:
	void Serialize(Serializer &serializer) const override;
	static unique_ptr<LogicalOperator> Deserialize(Deserializer &deserializer);

	vector<ColumnBinding> GetColumnBindings() override {
		return children[0]->GetColumnBindings();
	}

protected:
	void ResolveTypes() override;
};
} // namespace duckdb
