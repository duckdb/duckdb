#pragma once

/*
** Render output like fprintf().  Except, if the output is going to the
** console and if this is running on a Windows machine, translate the
** output from UTF-8 into MBCS.
*/
#if defined(_WIN32) || defined(WIN32)
static int win_utf8_mode = 0;

void utf8_printf(FILE *out, const char *zFormat, ...) {
	va_list ap;
	va_start(ap, zFormat);
	if (stdout_is_console && (out == stdout || out == stderr)) {
		char *z1 = sqlite3_vmprintf(zFormat, ap);
		if (win_utf8_mode && SetConsoleOutputCP(CP_UTF8)) {
			// we can write UTF8 directly
			fputs(z1, out);
		} else {
			// fallback to writing old style windows unicode
			char *z2 = sqlite3_win32_utf8_to_mbcs_v2(z1, 0);
			fputs(z2, out);
			sqlite3_free(z2);
		}
		sqlite3_free(z1);
	} else {
		vfprintf(out, zFormat, ap);
	}
	va_end(ap);
}
#elif !defined(utf8_printf)
#define utf8_printf fprintf
#endif
