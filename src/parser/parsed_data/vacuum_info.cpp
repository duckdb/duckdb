#include "duckdb/parser/parsed_data/vacuum_info.hpp"
#include "duckdb/common/field_writer.hpp"

namespace duckdb {

VacuumInfo::VacuumInfo(VacuumOptions options) : ParseInfo(TYPE), options(options), has_table(false) {
}

unique_ptr<VacuumInfo> VacuumInfo::Copy() {
	auto result = make_uniq<VacuumInfo>(options);
	result->has_table = has_table;
	if (has_table) {
		result->ref = ref->Copy();
	}
	return result;
}

void VacuumInfo::Serialize(Serializer &serializer) const {
	FieldWriter writer(serializer);
	writer.WriteField(options.analyze);
	writer.WriteField(options.vacuum);
	writer.Finalize();
}

unique_ptr<ParseInfo> VacuumInfo::Deserialize(Deserializer &deserializer) {

	VacuumOptions options;
	FieldReader reader(deserializer);
	options.analyze = reader.ReadRequired<bool>();
	options.vacuum = reader.ReadRequired<bool>();
	reader.Finalize();

	auto vacuum_info = make_uniq<VacuumInfo>(options);
	return std::move(vacuum_info);
}

} // namespace duckdb
