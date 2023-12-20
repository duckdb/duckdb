//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/parsed_data/parse_info.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"

namespace duckdb {

enum class ParseInfoType : uint8_t {
	ALTER_INFO,
	ATTACH_INFO,
	COPY_INFO,
	CREATE_INFO,
	DETACH_INFO,
	DROP_INFO,
	BOUND_EXPORT_DATA,
	LOAD_INFO,
	PRAGMA_INFO,
	SHOW_SELECT_INFO,
	TRANSACTION_INFO,
	VACUUM_INFO
};

struct ParseInfo {
	explicit ParseInfo(ParseInfoType info_type) : info_type(info_type) {
	}
	virtual ~ParseInfo() {
	}

	ParseInfoType info_type;

public:
	template <class TARGET>
	TARGET &Cast() {
		D_ASSERT(dynamic_cast<TARGET *>(this));
		return reinterpret_cast<TARGET &>(*this);
	}

	template <class TARGET>
	const TARGET &Cast() const {
		D_ASSERT(dynamic_cast<const TARGET *>(this));
		return reinterpret_cast<const TARGET &>(*this);
	}

	virtual void Serialize(Serializer &serializer) const;
	static unique_ptr<ParseInfo> Deserialize(Deserializer &deserializer);
};

} // namespace duckdb
