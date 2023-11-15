#include "duckdb/execution/operator/scan/csv/csv_option.hpp"

namespace duckdb {
template <typename T>
std::string CSVOption<T>::FormatValueInternal(const std::string &val) const {
	return val;
}
template <typename T>
std::string CSVOption<T>::FormatValueInternal(const idx_t &val) const {
	return to_string(val);
}
template <typename T>
std::string CSVOption<T>::FormatValueInternal(const char &val) const {
	string char_val;
	char_val += val;
	return char_val;
}
template <typename T>
std::string CSVOption<T>::FormatValueInternal(const NewLineIdentifier &val) const {
	switch (val) {
	case NewLineIdentifier::SINGLE:
		return "\\n";
	case NewLineIdentifier::CARRY_ON:
		return "\\r\\n";
	case NewLineIdentifier::NOT_SET:
		return "One-line file";
	default:
		throw InternalException("Invalid Newline Detected.");
	}
}
template <typename T>
std::string CSVOption<T>::FormatValueInternal(const StrpTimeFormat &val) const {
	return val.format_specifier;
}
template <typename T>
std::string CSVOption<T>::FormatValueInternal(const bool &val) const {
	if (val) {
		return "true";
	}
	return "false";
}

template <typename T>
CSVOption<T> CSVOption<T>::Deserialize(Deserializer &deserializer) {
	bool set_by_user = deserializer.ReadProperty<bool>(100, "set_by_user");
	T value = deserializer.ReadProperty<T>(101, "value");
	return {value, set_by_user};
};
template <typename T>

void CSVOption<T>::Serialize(Serializer &serializer) const {
	serializer.WriteProperty(100, "set_by_user", set_by_user);
	serializer.WriteProperty(101, "value", value);
};

template <typename T>
void CSVOption<T>::Set(T value_p, bool by_user) {
	D_ASSERT(!(by_user && set_by_user));
	if (!set_by_user) {
		// If it's not set by user we can change the value
		value = value_p;
		set_by_user = by_user;
	}
}

template <typename T>
void CSVOption<T>::Set(CSVOption value_p, bool by_user) {
	D_ASSERT(!(by_user && set_by_user));
	if (!set_by_user) {
		// If it's not set by user we can change the value
		value = value_p;
		set_by_user = by_user;
	}
}
template <typename T>
bool CSVOption<T>::operator==(const CSVOption &other) const {
	return value == other.value;
}
template <typename T>
bool CSVOption<T>::operator!=(const CSVOption &other) const {
	return value != other.value;
}
template <typename T>
bool CSVOption<T>::operator==(const T &other) const {
	return value == other;
}
template <typename T>
bool CSVOption<T>::operator!=(const T &other) const {
	return value != other;
}
template <typename T>
const T CSVOption<T>::GetValue() const {
	return value;
}
template <typename T>
bool CSVOption<T>::IsSetByUser() const {
	return set_by_user;
}
template <typename T>
string CSVOption<T>::FormatSet() const {
	if (set_by_user) {
		return "(Set By User)";
	}
	return "(Auto-Detected)";
}
template <typename T>
string CSVOption<T>::FormatValue() const {
	return FormatValueInternal(value);
}
} // namespace duckdb
