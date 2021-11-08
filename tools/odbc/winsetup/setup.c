/* Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
 * All Rights Reserved. Use of this source code is governed by a MPL/2.0 license that can be found in the LICENSE file.
 */

#ifndef _CRT_SECURE_NO_DEPRECATE
#define _CRT_SECURE_NO_DEPRECATE 1
#endif

#include <windows.h>
/* indicate to sqltypes.h that windows.h has already been included and
   that it doesn't have to define Windows constants */
#define ALREADY_HAVE_WINDOWS_TYPE 1
#include <sql.h>
#include <sqlext.h>
#include <stdio.h>
#include <inttypes.h>

#ifdef EXPORT
#undef EXPORT
#endif
#define EXPORT __declspec(dllexport)
#include <odbcinst.h>
#include "resource.h"

static char *driver_name = "DuckDB Driver";
static HINSTANCE instance;

static void ODBCLOG(const char *fmt, ...) {
	va_list ap;
	char *s = getenv("ODBCDEBUG");

	va_start(ap, fmt);
	if (s && *s) {
		FILE *f;

		f = fopen(s, "a");
		if (f) {
			vfprintf(f, fmt, ap);
			fclose(f);
		} else {
			vfprintf(stderr, fmt, ap);
		}
	}
	va_end(ap);
}

BOOL INSTAPI ConfigDriver(HWND hwnd, WORD request, LPCSTR driver, LPCSTR args, LPSTR msg, WORD msgmax, WORD *msgout) {
	(void)hwnd;
	ODBCLOG("ConfigDriver %d %s %s\n", request, driver ? driver : "(null)", args ? args : "(null)");

	if (msgout)
		*msgout = 0;
	if (msg && msgmax > 0)
		*msg = 0;
	switch (request) {
	case ODBC_INSTALL_DRIVER:
	case ODBC_REMOVE_DRIVER:
		break;
	case ODBC_CONFIG_DRIVER:
		break;
	default:
		SQLPostInstallerError(ODBC_ERROR_INVALID_REQUEST_TYPE, "Invalid request");
		return FALSE;
	}
	if (strcmp(driver, driver_name) != 0) {
		SQLPostInstallerError(ODBC_ERROR_INVALID_NAME, "Invalid driver name");
		return FALSE;
	}
	return TRUE;
}

struct odbc_data_t {
	char *dsn;
	char *database;
	HWND parent;
	WORD request;
};

static void MergeFromProfileString(const char *dsn, char **datap, const char *entry, const char *defval) {
	char buf[256];

	if (*datap != NULL) {
		return;
	}
	if (dsn == NULL || *dsn == 0) {
		*datap = strdup(defval);
		return;
	}
	if (SQLGetPrivateProfileString(dsn, entry, defval, buf, sizeof(buf), "odbc.ini") == 0)
		return;
	*datap = strdup(buf);
}

static INT_PTR CALLBACK DialogProc(HWND hwndDlg, UINT uMsg, WPARAM wParam, LPARAM lParam) {
	static struct odbc_data_t *datap;
	char buf[128];
	RECT rcDlg, rcOwner;

	switch (uMsg) {
	case WM_INITDIALOG:
		ODBCLOG("DialogProc WM_INITDIALOG 0x%x 0x%x\n", (unsigned)wParam, (unsigned)lParam);

		datap = (struct odbc_data_t *)lParam;
		/* center dialog on parent */
		GetWindowRect(datap->parent, &rcOwner);
		GetWindowRect(hwndDlg, &rcDlg);
		SetWindowPos(hwndDlg, 0, rcOwner.left + (rcOwner.right - rcOwner.left - (rcDlg.right - rcDlg.left)) / 2,
		             rcOwner.top + (rcOwner.bottom - rcOwner.top - (rcDlg.bottom - rcDlg.top)) / 2, 0,
		             0, /* ignores size arguments */
		             SWP_NOSIZE | SWP_NOZORDER);

		/* fill in text fields */
		SetDlgItemText(hwndDlg, IDC_EDIT_DSN, datap->dsn ? datap->dsn : "");
		SetDlgItemText(hwndDlg, IDC_EDIT_DATABASE, datap->database ? datap->database : "");
		if (datap->request == ODBC_ADD_DSN && datap->dsn && *datap->dsn)
			EnableWindow(GetDlgItem(hwndDlg, IDC_EDIT_DSN), FALSE);
		return TRUE;
	case WM_COMMAND:
		ODBCLOG("DialogProc WM_COMMAND 0x%x 0x%x\n", (unsigned)wParam, (unsigned)lParam);

		switch (LOWORD(wParam)) {
		case IDOK:
			if (datap->request != ODBC_ADD_DSN || datap->dsn == NULL || *datap->dsn == 0) {
				GetDlgItemText(hwndDlg, IDC_EDIT_DSN, buf, sizeof(buf));
				if (!SQLValidDSN(buf)) {
					MessageBox(hwndDlg, "Invalid Datasource Name", NULL, MB_ICONERROR);
					return TRUE;
				}
				if (datap->dsn)
					free(datap->dsn);
				datap->dsn = strdup(buf);
			}
			GetDlgItemText(hwndDlg, IDC_EDIT_DATABASE, buf, sizeof(buf));
			if (datap->database)
				free(datap->database);
			datap->database = strdup(buf);
			/* fall through */
		case IDCANCEL:
			EndDialog(hwndDlg, LOWORD(wParam));
			return TRUE;
		}
	default:
		ODBCLOG("DialogProc 0x%x 0x%x 0x%x\n", uMsg, (unsigned)wParam, (unsigned)lParam);
		break;
	}
	return FALSE;
}

inline int strncasecmp(const char *s1, const char *s2, int n) {
	int cmp_val = 0;
	while (n != 0 && *s1 != '\0') {
		cmp_val = tolower(*s1++) - tolower(*s2++);
		if (cmp_val != 0) {
			return cmp_val;
		}
		--n;
	}
	return cmp_val;
}

BOOL INSTAPI ConfigDSN(HWND parent, WORD request, LPCSTR driver, LPCSTR attributes) {
	struct odbc_data_t data;
	char *dsn = NULL;
	BOOL rc;

	ODBCLOG("ConfigDSN %d %s %s 0x%" PRIxPTR "\n", request, driver ? driver : "(null)",
	        attributes ? attributes : "(null)", (uintptr_t)&data);

	if (strcmp(driver, driver_name) != 0) {
		SQLPostInstallerError(ODBC_ERROR_INVALID_NAME, "Invalid driver name");
		return FALSE;
	}
	switch (request) {
	case ODBC_ADD_DSN:
	case ODBC_CONFIG_DSN:
	case ODBC_REMOVE_DSN:
		break;
	default:
		SQLPostInstallerError(ODBC_ERROR_INVALID_REQUEST_TYPE, "Invalid request");
		return FALSE;
	}

	data.dsn = NULL;
	data.database = NULL;
	data.parent = parent;
	data.request = request;

	while (*attributes) {
		char *value = strchr(attributes, '=');

		if (value == NULL) {
			SQLPostInstallerError(ODBC_ERROR_INVALID_KEYWORD_VALUE, "Invalid attributes string");
			return FALSE;
		}
		value++;
		if (strncasecmp("dsn=", attributes, value - attributes) == 0) {
			dsn = value;
			data.dsn = strdup(value);
		} else if (strncasecmp("database=", attributes, value - attributes) == 0)
			data.database = strdup(value);
		attributes = value + strlen(value) + 1;
	}

	if (request == ODBC_REMOVE_DSN) {
		if (data.dsn == NULL) {
			SQLPostInstallerError(ODBC_ERROR_INVALID_KEYWORD_VALUE, "No DSN specified");
			return FALSE;
		}
		rc = SQLRemoveDSNFromIni(data.dsn);

		goto finish;
	}

	MergeFromProfileString(data.dsn, &data.database, "database", ":memory:");

	ODBCLOG("ConfigDSN values: dsn=%s database=%s\n", data.dsn ? data.dsn : "(null)",
	        data.database ? data.database : "(null)");

	/* we're optimistic: default return value */
	rc = TRUE;

	if (parent) {
		switch (DialogBoxParam(instance, MAKEINTRESOURCE(IDD_SETUP_DIALOG), parent, DialogProc, (LPARAM)&data)) {
		case IDOK:
			break;
		default:
			rc = FALSE;
			SQLPostInstallerError(ODBC_ERROR_REQUEST_FAILED, "Error creating configuration dialog");
			/* fall through */
		case IDCANCEL:
			goto finish;
		}
	}

	if (request == ODBC_ADD_DSN || strcmp(dsn, data.dsn) != 0) {
		if (!SQLValidDSN(data.dsn)) {
			rc = FALSE;
			if (parent)
				MessageBox(parent, "Invalid Datasource Name", NULL, MB_ICONERROR);
			SQLPostInstallerError(ODBC_ERROR_INVALID_NAME, "Invalid driver name");
			goto finish;
		}
		if (dsn == NULL || strcmp(dsn, data.dsn) != 0) {
			char *drv = NULL;

			/* figure out whether the new dsn already exists */
			MergeFromProfileString(data.dsn, &drv, "driver", "");
			if (drv && *drv) {
				free(drv);
				if (parent && MessageBox(parent, "Replace existing Datasource Name?", NULL,
				                         MB_OKCANCEL | MB_ICONQUESTION) != IDOK) {
					goto finish;
				}
				ODBCLOG("ConfigDSN removing dsn %s\n", data.dsn);
				if (!SQLRemoveDSNFromIni(data.dsn)) {
					rc = FALSE;
					MessageBox(parent, "Failed to remove old Datasource Name", NULL, MB_ICONERROR);
					SQLPostInstallerError(ODBC_ERROR_REQUEST_FAILED, "Failed to remove old Datasource Name");
					goto finish;
				}
			} else if (drv)
				free(drv);
		}
		if (dsn && !SQLRemoveDSNFromIni(dsn)) {
			rc = FALSE;
			if (parent)
				MessageBox(parent, "Failed to remove old Datasource Name", NULL, MB_ICONERROR);
			SQLPostInstallerError(ODBC_ERROR_REQUEST_FAILED, "Failed to remove old Datasource Name");
			goto finish;
		}
		if (!SQLWriteDSNToIni(data.dsn, driver)) {
			rc = FALSE;
			if (parent)
				MessageBox(parent, "Failed to add new Datasource Name", NULL, MB_ICONERROR);
			SQLPostInstallerError(ODBC_ERROR_REQUEST_FAILED, "Failed to add new Datasource Name");
			goto finish;
		}
	}
	ODBCLOG("ConfigDSN writing values: dsn=%s database=%s\n", data.dsn ? data.dsn : "(null)",
	        data.database ? data.database : "(null)");

	if (!SQLWritePrivateProfileString(data.dsn, "database", data.database, "odbc.ini")) {
		rc = FALSE;
		if (parent)
			MessageBox(parent, "Error writing configuration data to registry", NULL, MB_ICONERROR);
		SQLPostInstallerError(ODBC_ERROR_REQUEST_FAILED, "Error writing configuration data to registry");
		goto finish;
	}

finish:
	if (data.dsn)
		free(data.dsn);
	if (data.database)
		free(data.database);
	ODBCLOG("ConfigDSN returning %s\n", rc ? "TRUE" : "FALSE");
	return rc;
}

BOOL WINAPI DllMain(HINSTANCE hinstDLL, DWORD reason, LPVOID reserved) {
	instance = hinstDLL;
	(void)reserved;
	ODBCLOG("DllMain %d\n", reason);

	return TRUE;
}
