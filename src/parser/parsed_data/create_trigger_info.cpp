#include "duckdb/parser/parsed_data/create_trigger_info.hpp"
#include "duckdb/common/enum_util.hpp"
#include "duckdb/parser/keyword_helper.hpp"

namespace duckdb {

CreateTriggerInfo::CreateTriggerInfo()
    : CreateInfo(CatalogType::TRIGGER_ENTRY, INVALID_SCHEMA), timing(TriggerTiming::AFTER),
      event_type(TriggerEventType::INSERT_EVENT), for_each(TriggerForEach::STATEMENT) {
}

unique_ptr<CreateInfo> CreateTriggerInfo::Copy() const {
	auto result = make_uniq<CreateTriggerInfo>();
	CopyProperties(*result);
	result->trigger_name = trigger_name;
	result->base_table = unique_ptr_cast<TableRef, BaseTableRef>(base_table->Copy());
	result->timing = timing;
	result->event_type = event_type;
	result->columns = columns;
	result->for_each = for_each;
	result->trigger_action = trigger_action->Copy();
	return std::move(result);
}

string CreateTriggerInfo::ToString() const {
	duckdb::stringstream ss;
	ss << "CREATE";
	if (on_conflict == OnCreateConflict::REPLACE_ON_CONFLICT) {
		ss << " OR REPLACE";
	}
	ss << " TRIGGER ";
	if (on_conflict == OnCreateConflict::IGNORE_ON_CONFLICT) {
		ss << "IF NOT EXISTS ";
	}
	if (!IsInvalidSchema(schema)) {
		ss << SQLIdentifier(schema) << ".";
	}
	ss << SQLIdentifier(trigger_name);
	ss << " ";
	ss << EnumUtil::ToString(timing);
	ss << " ";
	ss << EnumUtil::ToString(event_type);
	if (event_type == TriggerEventType::UPDATE_EVENT && !columns.empty()) {
		ss << " OF ";
		for (idx_t i = 0; i < columns.size(); i++) {
			if (i > 0) {
				ss << ", ";
			}
			ss << SQLIdentifier(columns[i]);
		}
	}
	ss << " ON ";
	ss << base_table->ToString();
	ss << " FOR EACH " << EnumUtil::ToString(for_each);
	ss << " " << trigger_action->ToString();
	ss << ";";
	return ss.str();
}

} // namespace duckdb
