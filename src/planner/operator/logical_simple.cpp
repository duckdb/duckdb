#include "duckdb/planner/operator/logical_simple.hpp"
#include "duckdb/parser/parsed_data/alter_info.hpp"
#include "duckdb/parser/parsed_data/drop_info.hpp"
#include "duckdb/parser/parsed_data/load_info.hpp"

namespace duckdb {

void LogicalSimple::Serialize(FieldWriter &writer) const {
	writer.WriteField<LogicalOperatorType>(type);
	switch (type) {
	case LogicalOperatorType::LOGICAL_ALTER:
		info->Cast<AlterInfo>().Serialize(writer.GetSerializer());
		break;
	case LogicalOperatorType::LOGICAL_DROP:
		info->Cast<DropInfo>().Serialize(writer.GetSerializer());
		break;
	case LogicalOperatorType::LOGICAL_LOAD:
		info->Cast<LoadInfo>().Serialize(writer.GetSerializer());
		break;
	default:
		throw NotImplementedException(LogicalOperatorToString(type));
	}
}

unique_ptr<LogicalOperator> LogicalSimple::Deserialize(LogicalDeserializationState &state, FieldReader &reader) {
	auto type = reader.ReadRequired<LogicalOperatorType>();
	unique_ptr<ParseInfo> parse_info;
	switch (type) {
	case LogicalOperatorType::LOGICAL_ALTER:
		parse_info = AlterInfo::Deserialize(reader.GetSource());
		break;
	case LogicalOperatorType::LOGICAL_DROP:
		parse_info = DropInfo::Deserialize(reader.GetSource());
		break;
	case LogicalOperatorType::LOGICAL_LOAD:
		parse_info = LoadInfo::Deserialize(reader.GetSource());
		break;
	default:
		throw NotImplementedException(LogicalOperatorToString(state.type));
	}
	return make_uniq<LogicalSimple>(type, std::move(parse_info));
}

idx_t LogicalSimple::EstimateCardinality(ClientContext &context) {
	return 1;
}

} // namespace duckdb
