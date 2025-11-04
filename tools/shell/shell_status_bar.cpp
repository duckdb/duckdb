#include "shell_status_bar.hpp"
#include "duckdb/common/printer.hpp"
#include "shell_state.hpp"

namespace duckdb_shell {
using duckdb::OutputStream;
using duckdb::Printer;

enum class StatusBarPadding { NO_PADDING };

enum class StatusBarAlignment { LEFT, MIDDLE, RIGHT };

struct StatusBarPrompt : public Prompt {
public:
	StatusBarPrompt(StatusBar &status_bar_p) : status_bar(status_bar_p) {
	}

	StatusBar &status_bar;
	//! Alignment of the status bar relative to the total bar
	StatusBarAlignment alignment = StatusBarAlignment::LEFT;
	//! Padding of the content of the status bar
	StatusBarPadding padding_type = StatusBarPadding::NO_PADDING;
	//! In case there is padding (i.e. padding_type is not NO_PADDING) - where to position the content
	StatusBarAlignment content_alignment = StatusBarAlignment::LEFT;
	//! Display condition, if any
	string does_not_contain;
	//! Minimum render size
	optional_idx min_size;

public:
	void AddPaddingIfRequired(string &text, idx_t &render_length) {
		if (!min_size.IsValid() || render_length >= min_size.GetIndex()) {
			return;
		}
		auto padding_required = min_size.GetIndex() - render_length;
		// need to add at least one space of padding to each side
		idx_t left_padding;
		idx_t right_padding;
		switch (content_alignment) {
		case StatusBarAlignment::LEFT:
			left_padding = 1;
			right_padding = padding_required - 1;
			break;
		case StatusBarAlignment::MIDDLE:
			right_padding = padding_required / 2;
			left_padding = padding_required - right_padding;
			break;
		case StatusBarAlignment::RIGHT:
			left_padding = padding_required - 1;
			right_padding = 1;
			break;
		default:
			throw InternalException("Unsupported alignment type");
		}
		string result;
		result += string(left_padding, ' ');
		result += text;
		result += string(right_padding, ' ');
		text = result;
		render_length += padding_required;
	}

protected:
	vector<string> GetSupportedSettings() override {
		auto supported = Prompt::GetSupportedSettings();
		supported.push_back("progress_bar_percentage");
		supported.push_back("progress_bar");
		supported.push_back("eta");
		return supported;
	}

	bool ParseSetting(const string &bracket_type, const string &value) override {
		if (bracket_type == "align") {
			if (value == "right") {
				alignment = StatusBarAlignment::RIGHT;
			} else if (value == "left") {
				alignment = StatusBarAlignment::LEFT;
			} else {
				throw InvalidInputException("Unsupported type %s for align: expected left or right", value);
			}
			return true;
		}
		if (bracket_type == "content_align") {
			if (value == "right") {
				content_alignment = StatusBarAlignment::RIGHT;
			} else if (value == "left") {
				content_alignment = StatusBarAlignment::LEFT;
			} else if (value == "middle") {
				content_alignment = StatusBarAlignment::MIDDLE;
			} else {
				throw InvalidInputException("Unsupported type %s for content_align: expected left or right", value);
			}
			return true;
		}
		if (bracket_type == "hide_if_contains") {
			// parse condition
			does_not_contain = value;
			return true;
		}
		if (bracket_type == "min_size") {
			min_size = StringUtil::ToUnsigned(value);
			return true;
		}
		return false;
	}

	string HandleSetting(ShellState &state, const PromptComponent &component) override {
		if (component.literal == "progress_bar_percentage") {
			string result;
			if (status_bar.percentage < 100) {
				result += " ";
			}
			if (status_bar.percentage < 10) {
				result += " ";
			}
			result += to_string(status_bar.percentage) + "%";
			return result;
		}
		if (component.literal == "progress_bar") {
			return duckdb::TerminalProgressBarDisplay::FormatProgressBar(status_bar.display_info,
			                                                             status_bar.percentage);
		}
		if (component.literal == "eta") {
			return duckdb::TerminalProgressBarDisplay::FormatETA(status_bar.estimated_remaining_seconds);
		}
		return Prompt::HandleSetting(state, component);
	}

	duckdb::Connection &GetConnection(ShellState &state) override {
		if (!status_bar.connection) {
			status_bar.connection = make_uniq<duckdb::Connection>(*state.db);
		}
		return *status_bar.connection;
	}
};

StatusBar::StatusBar() {
}
StatusBar::~StatusBar() {
}

void StatusBar::AddComponent(const string &component_text) {
	auto component = make_uniq<StatusBarPrompt>(*this);
	component->ParsePrompt(component_text);
	components.push_back(std::move(component));
}

void StatusBar::ClearComponents() {
	components.clear();
}

string StatusBar::GenerateStatusBar(ShellState &state) {
	auto terminal_width = Printer::TerminalWidth();

	string lhs;
	string rhs;
	idx_t total_render_length = 0;
	for (auto &component : components) {
		auto text = component->GeneratePrompt(state);
		if (!component->does_not_contain.empty()) {
			if (StringUtil::Contains(text, component->does_not_contain)) {
				continue;
			}
		}
		auto render_length = state.RenderLength(text);
		component->AddPaddingIfRequired(text, render_length);

		if (total_render_length + render_length > terminal_width) {
			// exceeds terminal width - don't stop rendering
			break;
		}
		total_render_length += render_length;
		if (component->alignment == StatusBarAlignment::LEFT) {
			lhs += text;
		} else {
			rhs += text;
		}
	}
	string result;
	result = lhs;
	if (total_render_length < terminal_width) {
		result += string(terminal_width - total_render_length, ' ');
		result += rhs;
	}
	return result;
}

ShellStatusBarDisplay::ShellStatusBarDisplay() {
}

void ShellStatusBarDisplay::PrintProgressInternal(int32_t percentage, double estimated_remaining_seconds,
                                                  bool is_finished) {
	string result;
	// clear previous display
	result += "\r\x1b[0K";
	if (!is_finished) {
		auto &state = ShellState::Get();
		try {
			state.status_bar->percentage = percentage;
			state.status_bar->estimated_remaining_seconds = estimated_remaining_seconds;
			result += state.status_bar->GenerateStatusBar(state);
		} catch (std::exception &ex) {
			ErrorData error(ex);
			result += error.Message();
		}
		state.status_bar->connection.reset();
	}
	Printer::RawPrint(OutputStream::STREAM_STDOUT, result);
}

} // namespace duckdb_shell
