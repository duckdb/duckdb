//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/main/setting_preset.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/optional_ptr.hpp"
#include "duckdb/common/types/value.hpp"

namespace duckdb {
class ClientContext;
class DBConfig;

//! A single setting within a preset.
struct PresetMember {
	//! The name of the setting to set
	const char *setting;
	//! Produces the value. Takes the config so that a preset can size itself to the machine it is
	//! applied on rather than baking in absolute values.
	Value (*value)(DBConfig &config);
	//! Whether the preset still applies if this setting does not exist - used for settings owned by
	//! an extension that may not be installed
	bool optional;
};

//! A named bundle of settings. Members are applied IN ORDER: settings constrain each other, so for
//! example `threads` must be set before `external_threads`, and `autoload_known_extensions` before
//! any setting owned by an extension.
struct PresetDefinition {
	const char *name;
	const char *description;
	const PresetMember *members;
	idx_t member_count;
};

//! The result of applying one member of a preset.
struct PresetMemberResult {
	string setting;
	Value old_value;
	Value new_value;
	//! "applied", or "skipped" when an optional member's setting was unavailable
	string status;
	//! Why the member was skipped; empty when applied
	string note;
};

struct PresetRegistry {
	//! The built-in presets
	DUCKDB_API static idx_t GetPresetCount();
	DUCKDB_API static const PresetDefinition &GetPresetByIndex(idx_t index);
	DUCKDB_API static optional_ptr<const PresetDefinition> Find(const string &name);

	//! Apply a preset, returning what each member did. Members are applied in order through the
	//! regular SET path, so locking, extension autoloading and scope resolution all behave as they
	//! do for an explicit SET.
	DUCKDB_API static vector<PresetMemberResult> Apply(ClientContext &context, const PresetDefinition &preset);
};

} // namespace duckdb
