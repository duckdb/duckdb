#include "duckdb/common/open_file_info.hpp"

#include "duckdb/common/exception.hpp"

namespace duckdb {

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
	if (value.IsNull()) {
		throw InvalidInputException(
		    "Invalid value for file option \"%s\" - expected a VARCHAR, but \"%s\" was provided", name,
		    value.ToString());
	}
	// VARCHAR and BLOB are read back as-is - casting a BLOB to VARCHAR would escape the bytes it holds
	if (value.type().InternalType() == PhysicalType::VARCHAR) {
		result = StringValue::Get(value);
		return true;
	}
	result = value.ToString();
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

template <>
bool ExtendedOpenFileInfo::TryGetOption(const string &name, timestamp_t &result) const {
	auto entry = options.find(name);
	if (entry == options.end()) {
		return false;
	}
	auto &value = entry->second;
	string error_message;
	auto timestamp_value = value.DefaultTryCastAs(LogicalType::TIMESTAMP, &error_message);
	if (!timestamp_value || timestamp_value->IsNull()) {
		throw InvalidInputException(
		    "Invalid value for file option \"%s\" - expected a TIMESTAMP, but \"%s\" was provided", name,
		    value.ToString());
	}
	result = TimestampValue::Get(*timestamp_value);
	return true;
}

} // namespace duckdb
