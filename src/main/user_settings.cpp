#include "duckdb/main/user_settings.hpp"
#include "duckdb/main/settings.hpp"
#include "duckdb/common/type_visitor.hpp"
#include "duckdb/common/types/string.hpp"
#include "duckdb/common/types/interval.hpp"
#include "duckdb/common/types/uuid.hpp"

#include <cmath>
#ifdef __MVS__
#include <zos-tls.h>
#endif

namespace duckdb {

static bool HasUserSettingExtensionType(const LogicalType &type) {
	if (!type.IsNested()) {
		return type.HasExtensionInfo();
	}
	return TypeVisitor::Contains(type, [](const LogicalType &child) { return child.HasExtensionInfo(); });
}

static bool UserSettingValuesEqual(const Value &left, const Value &right);

static bool UserSettingChildrenEqual(const vector<Value> &left, const vector<Value> &right) {
	if (left.size() != right.size()) {
		return false;
	}
	for (idx_t i = 0; i < left.size(); i++) {
		if (!UserSettingValuesEqual(left[i], right[i])) {
			return false;
		}
	}
	return true;
}

static bool UserSettingValuesEqual(const Value &left, const Value &right) {
	// A no-op must preserve the type; NotDistinctFrom can cast values or equate differently typed NULLs.
	if (left.type() != right.type()) {
		return false;
	}
	// Ordinary type equality ignores collations, including those on nested child types.
	if ((left.type().IsNested() || left.type().id() == LogicalTypeId::VARCHAR) &&
	    !left.type().EqualsIncludingCollation(right.type())) {
		return false;
	}
	if (left.IsNull() || right.IsNull()) {
		return Value::NotDistinctFrom(left, right);
	}
	switch (left.type().InternalType()) {
	// NotDistinctFrom equates signed zeros and NaNs; preserve the sign and allow NaNs to miss the no-op.
	case PhysicalType::FLOAT: {
		const auto lvalue = left.GetValueUnsafe<float>();
		const auto rvalue = right.GetValueUnsafe<float>();
		return lvalue == rvalue && std::signbit(lvalue) == std::signbit(rvalue);
	}
	case PhysicalType::DOUBLE: {
		const auto lvalue = left.GetValueUnsafe<double>();
		const auto rvalue = right.GetValueUnsafe<double>();
		return lvalue == rvalue && std::signbit(lvalue) == std::signbit(rvalue);
	}
	// NotDistinctFrom equates 1 month with 30 days, but date arithmetic distinguishes them.
	case PhysicalType::INTERVAL:
		return left.GetValueUnsafe<interval_t>().SameFields(right.GetValueUnsafe<interval_t>());
	// SQL nested equality applies those same normalizations to children; recurse with these stricter rules.
	case PhysicalType::STRUCT:
		return UserSettingChildrenEqual(StructValue::GetChildren(left), StructValue::GetChildren(right));
	case PhysicalType::LIST:
		return UserSettingChildrenEqual(ListValue::GetChildren(left), ListValue::GetChildren(right));
	case PhysicalType::ARRAY:
		return UserSettingChildrenEqual(ArrayValue::GetChildren(left), ArrayValue::GetChildren(right));
	// NotDistinctFrom does not implement comparisons for these internal types.
	case PhysicalType::INVALID:
	case PhysicalType::UNKNOWN:
	case PhysicalType::BIT:
		return false;
	default:
		return Value::NotDistinctFrom(left, right);
	}
}

bool UserSettingsMap::SetUserSetting(idx_t setting_index, Value target_value) {
	if (setting_index >= settings.size()) {
		settings.resize(setting_index + 1);
	}
	auto &generic_setting = settings[setting_index];
	// Extension type equality can ignore missing modifiers and properties.
	if (generic_setting.is_set && !HasUserSettingExtensionType(generic_setting.value.type()) &&
	    !HasUserSettingExtensionType(target_value.type()) &&
	    UserSettingValuesEqual(generic_setting.value, target_value)) {
		return false;
	}
	generic_setting.is_set = true;
	generic_setting.value = std::move(target_value);
	return true;
}

void UserSettingsMap::ClearSetting(idx_t setting_index) {
	if (setting_index >= settings.size()) {
		// never set
		return;
	}
	auto &generic_setting = settings[setting_index];
	generic_setting.is_set = false;
	generic_setting.value = Value();
}

bool UserSettingsMap::IsSet(idx_t setting_index) const {
	if (setting_index >= settings.size()) {
		// never set
		return false;
	}
	return settings[setting_index].is_set;
}

bool UserSettingsMap::TryGetSetting(idx_t setting_index, Value &result_value) const {
	if (setting_index >= settings.size()) {
		// never set
		return false;
	}
	auto &generic_setting = settings[setting_index];
	if (!generic_setting.is_set) {
		return false;
	}
	result_value = generic_setting.value;
	return true;
}

//===--------------------------------------------------------------------===//
// GlobalUserSettings
//===--------------------------------------------------------------------===//
GlobalUserSettings::GlobalUserSettings() : settings_version(0), uuid(UUID::GenerateRandomUUID()) {
}

GlobalUserSettings::GlobalUserSettings(const GlobalUserSettings &other)
    : settings_map(other.settings_map), extension_parameters(other.extension_parameters),
      settings_version(other.settings_version.load()), uuid(UUID::GenerateRandomUUID()) {
}

GlobalUserSettings &GlobalUserSettings::operator=(const GlobalUserSettings &other) {
	settings_map = other.settings_map;
	extension_parameters = other.extension_parameters;
	settings_version = other.settings_version.load();
	return *this;
}

void GlobalUserSettings::SetUserSetting(idx_t setting_index, Value target_value) {
	lock_guard<mutex> guard(lock);
	if (settings_map.SetUserSetting(setting_index, std::move(target_value))) {
		++settings_version;
	}
}

void GlobalUserSettings::ClearSetting(idx_t setting_index) {
	lock_guard<mutex> guard(lock);
	settings_map.ClearSetting(setting_index);
	++settings_version;
}

bool GlobalUserSettings::IsSet(idx_t setting_index) const {
	lock_guard<mutex> guard(lock);
	return settings_map.IsSet(setting_index);
}

SettingLookupResult GlobalUserSettings::TryGetSetting(idx_t setting_index, Value &result_value) const {
#ifndef __MINGW32__
	// look-up in global settings
	const auto &cache = GetSettings();
	if (cache.settings.TryGetSetting(setting_index, result_value)) {
		return SettingLookupResult(SettingScope::GLOBAL);
	}
#else
	lock_guard<mutex> guard(lock);
	if (settings_map.TryGetSetting(setting_index, result_value)) {
		return SettingLookupResult(SettingScope::GLOBAL);
	}
#endif

	return SettingLookupResult();
}

bool GlobalUserSettings::HasExtensionOption(const Identifier &name) const {
	lock_guard<mutex> l(lock);
	return extension_parameters.find(name) != extension_parameters.end();
}

idx_t GlobalUserSettings::AddExtensionOption(const Identifier &name, ExtensionOption extension_option) {
	lock_guard<mutex> l(lock);
	const auto new_option = extension_parameters.emplace(make_pair(name, std::move(extension_option)));
	const auto did_insert = new_option.second;
	auto &option = new_option.first->second;

	if (!did_insert) {
		return option.setting_index.GetIndex();
	}

	auto setting_index = GeneratedSettingInfo::MaxSettingIndex + extension_parameters.size() - 1;
	option.setting_index = setting_index;
	++settings_version;
	return setting_index;
}

identifier_map_t<ExtensionOption> GlobalUserSettings::GetExtensionSettings() const {
	lock_guard<mutex> l(lock);
	return extension_parameters;
}

bool GlobalUserSettings::TryGetExtensionOption(const Identifier &name, ExtensionOption &result) const {
	lock_guard<mutex> l(lock);
	auto entry = extension_parameters.find(name);
	if (entry == extension_parameters.end()) {
		return false;
	}
	result = entry->second;
	return true;
}

optional_idx GlobalUserSettings::TryGetExtensionSettingIndex(const Identifier &name) const {
#ifndef __MINGW32__
	auto &extension_setting_indexes = GetSettings().extension_setting_indexes;
	auto entry = extension_setting_indexes.find(name);
	if (entry == extension_setting_indexes.end()) {
		return optional_idx();
	}
	return entry->second;
#else
	lock_guard<mutex> l(lock);
	auto entry = extension_parameters.find(name);
	if (entry == extension_parameters.end()) {
		return optional_idx();
	}
	return entry->second.setting_index;
#endif
}

#if !defined(__MINGW32__) && !defined(__MVS__)
//! The cache of the worker thread running on this thread, if any. A pointer needs no thread_local destructor
static thread_local CachedGlobalSettings *worker_settings_cache = nullptr; // NOLINT
#endif

WorkerSettingsCache::WorkerSettingsCache() {
}

WorkerSettingsCache::~WorkerSettingsCache() {
#if !defined(__MINGW32__) && !defined(__MVS__)
	if (cache) {
		worker_settings_cache = previous.get();
	}
#endif
}

bool WorkerSettingsCache::IsInitialized() const {
	return cache != nullptr;
}

void WorkerSettingsCache::Initialize() {
	cache = make_uniq<CachedGlobalSettings>();
#if !defined(__MINGW32__) && !defined(__MVS__)
	previous = worker_settings_cache;
	worker_settings_cache = cache.get();
#endif
}

#ifndef __MINGW32__
// Cache of global settings - used to allow lock-free access to global settings in a thread-safe manner
static CachedGlobalSettings &GetThreadSettingsCache() {
#ifdef __MVS__
	static __tlssim<CachedGlobalSettings> current_cache_impl;
	return *current_cache_impl.access();
#else
	if (worker_settings_cache) {
		return *worker_settings_cache;
	}
	// the first use on a thread registers the destructor, which allocates
	thread_local CachedGlobalSettings current_cache;
	return current_cache;
#endif
}

CachedGlobalSettings &GlobalUserSettings::GetSettings() const {
	auto &current_cache = GetThreadSettingsCache();
	const auto current_version = settings_version.load(std::memory_order_acquire);
	if (!current_cache.global_user_settings || this != current_cache.global_user_settings.get() ||
	    current_cache.uuid != uuid || current_cache.version != current_version) {
		// out-of-date, refresh the cache
		lock_guard<mutex> guard(lock);
		current_cache = CachedGlobalSettings(*this, settings_version, settings_map, extension_parameters);
	}
	return current_cache;
}
#endif

hugeint_t GlobalUserSettings::GetUUID() const {
	return uuid;
}

CachedGlobalSettings::CachedGlobalSettings() : version(0), uuid(0) {
}

CachedGlobalSettings::CachedGlobalSettings(const GlobalUserSettings &global_user_settings_p, idx_t version,
                                           UserSettingsMap settings_p,
                                           const identifier_map_t<ExtensionOption> &extension_parameters)
    : global_user_settings(global_user_settings_p), version(version), settings(std::move(settings_p)),
      uuid(global_user_settings_p.GetUUID()) {
	extension_setting_indexes.reserve(extension_parameters.size());
	for (auto &entry : extension_parameters) {
		extension_setting_indexes.emplace(entry.first, entry.second.setting_index.GetIndex());
	}
}

//===--------------------------------------------------------------------===//
// LocalUserSettings
//===--------------------------------------------------------------------===//
LocalUserSettings::~LocalUserSettings() {
}

void LocalUserSettings::SetUserSetting(idx_t setting_index, Value target_value) {
	settings_map.SetUserSetting(setting_index, std::move(target_value));
}

void LocalUserSettings::ClearSetting(idx_t setting_index) {
	settings_map.ClearSetting(setting_index);
}

bool LocalUserSettings::IsSet(idx_t setting_index) const {
	return settings_map.IsSet(setting_index);
}

SettingLookupResult LocalUserSettings::TryGetSetting(const GlobalUserSettings &global_settings, idx_t setting_index,
                                                     Value &result_value) const {
	if (settings_map.TryGetSetting(setting_index, result_value)) {
		return SettingLookupResult(SettingScope::LOCAL);
	}
	// look-up in global settings
	return global_settings.TryGetSetting(setting_index, result_value);
}

} // namespace duckdb
