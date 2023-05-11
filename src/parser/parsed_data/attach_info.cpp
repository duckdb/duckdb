#include "duckdb/parser/parsed_data/attach_info.hpp"
#include "duckdb/common/field_writer.hpp"

namespace duckdb {

unique_ptr<AttachInfo> AttachInfo::Copy() const {
	auto result = make_uniq<AttachInfo>();
	result->name = name;
	result->path = path;
	result->options = options;
	return result;
}

void AttachInfo::Serialize(Serializer &main_serializer) const {
	FieldWriter writer(main_serializer);
	writer.WriteString(name);
	writer.WriteString(path);
	writer.WriteField<uint32_t>(options.size());
	auto &serializer = writer.GetSerializer();
	for (auto &kv : options) {
		serializer.WriteString(kv.first);
		kv.second.Serialize(serializer);
	}
	writer.Finalize();
}

unique_ptr<ParseInfo> AttachInfo::Deserialize(Deserializer &deserializer) {
	FieldReader reader(deserializer);
	auto attach_info = make_uniq<AttachInfo>();
	attach_info->name = reader.ReadRequired<string>();
	attach_info->path = reader.ReadRequired<string>();
	auto default_attach_count = reader.ReadRequired<uint32_t>();
	auto &source = reader.GetSource();
	for (idx_t i = 0; i < default_attach_count; i++) {
		auto name = source.Read<string>();
		attach_info->options[name] = Value::Deserialize(source);
	}
	reader.Finalize();
	return std::move(attach_info);
}

} // namespace duckdb
