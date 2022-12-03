#pragma once

namespace duckdb {

template <class T>
struct ConfigOption {
	//! Overloaded cast to T, for easy conversion
	operator T() const {
		return Get();
	}

	//! Overloaded assignation operator, for easy assignment
	void operator=(T setting) {
		Set(setting);
	}

	ConfigOption() : default_value(), current_value(default_value) {
	}
	ConfigOption(T setting) : default_value(setting), current_value(default_value) {
	}

	void Set(T setting) {
		current_value = setting;
	}
	const T &Get() const {
		return current_value;
	}

	void Unset() {
		current_value = default_value;
	}

	void OverrideDefault(T new_default) {
		if (current_value == default_value) {
			// Also update the current value if it was set to the default
			current_value = new_default;
		}
		default_value = new_default;
	}

private:
	// TODO: keep track of 'unset' to differentiate between a default value
	// and a user-set value that is EQUAL to the default value
	T default_value;
	T current_value;
};

} // namespace duckdb
