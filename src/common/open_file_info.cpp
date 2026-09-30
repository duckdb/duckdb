#include "duckdb/common/open_file_info.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"

namespace duckdb {

namespace {

struct OpenFileOption {
	const char *name;
	LogicalTypeId type;
};

//! The open options the core knows about, together with the type they are read back as. Options that are not
//! listed here are stored as-is - extensions are free to define options of their own.
const OpenFileOption OPEN_FILE_OPTIONS[] = {{"file_size", LogicalTypeId::UBIGINT},
                                            {"last_modified", LogicalTypeId::TIMESTAMP},
                                            {"etag", LogicalTypeId::VARCHAR},
                                            {"type", LogicalTypeId::VARCHAR},
                                            {"force_full_download", LogicalTypeId::BOOLEAN},
                                            {"validate_external_file_cache", LogicalTypeId::BOOLEAN},
                                            {"footer_size", LogicalTypeId::UBIGINT}};

optional_ptr<const OpenFileOption> FindOpenFileOption(const string &name) {
	for (auto &option : OPEN_FILE_OPTIONS) {
		if (StringUtil::CIEquals(name, option.name)) {
			return &option;
		}
	}
	return nullptr;
}

} // namespace

void ExtendedOpenFileInfo::SetUserOption(const string &name, const Value &value) {
	auto known_option = FindOpenFileOption(name);
	if (!known_option) {
		// not an option the core knows about - store it as-is for extensions to interpret
		options[name] = value;
		return;
	}
	LogicalType target_type(known_option->type);
	string error_message;
	auto casted_value = value.DefaultTryCastAs(target_type, &error_message);
	if (!casted_value) {
		throw InvalidInputException("Invalid value for file option \"%s\" - expected a value of type %s, but \"%s\" "
		                            "was provided",
		                            known_option->name, target_type.ToString(), value.ToString());
	}
	// store the option under its canonical name, so a lookup of the option always finds it
	options[known_option->name] = std::move(*casted_value);
}

template <>
bool ExtendedOpenFileInfo::TryGetOption(const string &name, bool &result) const {
	auto entry = options.find(name);
	if (entry == options.end()) {
		return false;
	}
	auto &value = entry->second;
	string error_message;
	auto bool_value = value.DefaultTryCastAs(LogicalType::BOOLEAN, &error_message);
	if (!bool_value || bool_value->IsNull()) {
		throw InvalidInputException(
		    "Invalid value for file option \"%s\" - expected a BOOLEAN, but \"%s\" was provided", name,
		    value.ToString());
	}
	result = BooleanValue::Get(*bool_value);
	return true;
}

template <>
bool ExtendedOpenFileInfo::TryGetOption(const string &name, string &result) const {
	auto entry = options.find(name);
	if (entry == options.end()) {
		return false;
	}
	auto &value = entry->second;
	// a string option can be stored as a VARCHAR or as a BLOB - both are read back with StringValue::Get,
	// and casting a BLOB to VARCHAR here would escape the bytes it holds
	if (value.IsNull() || value.type().InternalType() != PhysicalType::VARCHAR) {
		throw InvalidInputException(
		    "Invalid value for file option \"%s\" - expected a VARCHAR, but \"%s\" was provided", name,
		    value.ToString());
	}
	result = StringValue::Get(value);
	return true;
}

template <>
bool ExtendedOpenFileInfo::TryGetOption(const string &name, idx_t &result) const {
	auto entry = options.find(name);
	if (entry == options.end()) {
		return false;
	}
	auto &value = entry->second;
	string error_message;
	auto ubigint_value = value.DefaultTryCastAs(LogicalType::UBIGINT, &error_message);
	if (!ubigint_value || ubigint_value->IsNull()) {
		throw InvalidInputException(
		    "Invalid value for file option \"%s\" - expected a UBIGINT, but \"%s\" was provided", name,
		    value.ToString());
	}
	result = UBigIntValue::Get(*ubigint_value);
	return true;
}

} // namespace duckdb
