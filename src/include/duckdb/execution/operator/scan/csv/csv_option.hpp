//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/scan/csv/csv_option.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/serializer/serializer.hpp"
#include "duckdb/common/serializer/deserializer.hpp"
#include "duckdb/function/scalar/strftime_format.hpp"

namespace duckdb {

enum class NewLineIdentifier : uint8_t {
	SINGLE = 1,   // Either \r or \n
	CARRY_ON = 2, // \r\n
	MIX = 3,      // Hippie-Land, can't run it multithreaded
	NOT_SET = 4
};

//! Wrapper for CSV Options that can be manually set by the user
//! It is important to make this difference for options that can be automatically sniffed AND manually set.
template <typename T>
struct CSVOption {
public:
	CSVOption(T value_p) : value(value_p) {};
	CSVOption(T value_p, bool set_by_user_p) : value(value_p), set_by_user(set_by_user_p) {};
	CSVOption() {};

	//! Sets value.
	//! If by user it also toggles the set_by user flag
	void Set(T value_p, bool by_user = true);

	//! Sets value.
	//! If by user it also toggles the set_by user flag
	void Set(CSVOption value_p, bool by_user = true);

	bool operator==(const CSVOption &other) const;

	bool operator!=(const CSVOption &other) const;

	bool operator==(const T &other) const;

	bool operator!=(const T &other) const;

	//! Returns CSV Option value
	const T GetValue() const;

	//! Returns if option was manually set by the user
	bool IsSetByUser() const;

	//! Returns a formatted string with information regarding how this option was set
	string FormatSet() const;

	//! Returns a formatted string with the actual value of this option
	string FormatValue() const;

	//! Serializes CSV Option
	DUCKDB_API void Serialize(Serializer &serializer) const;

	//! Deserializes CSV Option
	DUCKDB_API static CSVOption<T> Deserialize(Deserializer &deserializer);

private:
	//! If this option was manually set by the user
	bool set_by_user = false;
	T value;

	//! --------------------------------------------------- //
	//! Functions used to convert a value to a string
	//! --------------------------------------------------- //

	template <typename U>
	std::string FormatValueInternal(const U &val) const {
		throw InternalException("Type not accepted as CSV Option.");
	}

	std::string FormatValueInternal(const std::string &val) const;

	std::string FormatValueInternal(const idx_t &val) const;

	std::string FormatValueInternal(const char &val) const;

	std::string FormatValueInternal(const NewLineIdentifier &val) const;

	std::string FormatValueInternal(const StrpTimeFormat &val) const;

	std::string FormatValueInternal(const bool &val) const;
};

} // namespace duckdb
