#include "shell_renderer.hpp"
#include "shell_state.hpp"

namespace duckdb_shell {

bool ShellRenderer::IsColumnar(RenderMode mode) {
	switch(mode) {
	case RenderMode::COLUMN:
	case RenderMode::TABLE:
	case RenderMode::BOX:
	case RenderMode::MARKDOWN:
	case RenderMode::LATEX:
		return true;
	default:
		return false;
	}
}

}
