#include "shell_status_bar.hpp"
#include "duckdb/common/printer.hpp"
#include "shell_state.hpp"

namespace duckdb_shell {
using duckdb::OutputStream;
using duckdb::Printer;

struct StatusBarPrompt : public Prompt {
public:
	StatusBarPrompt(StatusBar &status_bar_p) : status_bar(status_bar_p) {
	}

	StatusBar &status_bar;

protected:
	vector<string> GetSupportedSettings() override {
		auto supported = Prompt::GetSupportedSettings();
		supported.push_back("progress_bar_percentage");
		supported.push_back("progress_bar");
		supported.push_back("eta");
		return supported;
	}

	string HandleSetting(ShellState &state, const PromptComponent &component) override {
		if (component.literal == "progress_bar_percentage") {
			return to_string(status_bar.percentage) + "%";
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
};

StatusBarComponent::~StatusBarComponent() {
}

StatusBar::StatusBar() {
}
StatusBar::~StatusBar() {
}

void StatusBar::ParseStatusBar(const string &text) {
	auto component = make_uniq<StatusBarComponent>();
	component->prompt = make_uniq<StatusBarPrompt>(*this);
	component->prompt->ParsePrompt(text);
	components.push_back(std::move(component));
}

string StatusBar::GenerateStatusBar(ShellState &state) {
	string result;
	for (auto &component : components) {
		result += component->prompt->GeneratePrompt(state);
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
		try {
			auto &state = ShellState::Get();
			state.status_bar->percentage = percentage;
			state.status_bar->estimated_remaining_seconds = estimated_remaining_seconds;
			result += state.status_bar->GenerateStatusBar(state);
		} catch (std::exception &ex) {
		duckdb:
			ErrorData error(ex);
			result += error.Message();
		}
	}
	Printer::RawPrint(OutputStream::STREAM_STDOUT, result);
}

} // namespace duckdb_shell
