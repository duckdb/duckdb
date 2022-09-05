/* Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
 * All Rights Reserved. Use of this source code is governed by a MPL/2.0 license that can be found in the LICENSE file.
 */

#include <windows.h>
#include <winver.h>
#include <shlwapi.h>
#include <sqlext.h>
#include <sql.h>
#include <odbcinst.h>
#include <stdio.h>
#include <string.h>
#include <stdbool.h>
#include <shlobj_core.h>

static const char *driver_name = "DuckDB Driver";
static const char *data_source_name = "DuckDB";
static const char *driver_dll = "duckdb_odbc.dll";
static const char *driver_dl_ls = "duckdb_odbc_setup.dll";
static const char *duckdb_odbc_ver = "3.0";

// global option do show or not message box, useful on the CI
static bool show_msg_box = true;

void PrintInfoMsg(const char *func, const char *msg, int errnr) {
	if (show_msg_box) {
		MessageBox(NULL, msg, func, MB_ICONINFORMATION | MB_TASKMODAL | MB_SETFOREGROUND);
	} else {
		printf("%d - %s: %s\n", errnr, func, msg);
	}
}

void PrintMsg(const char *func, const char *msg, int errnr) {
	if (show_msg_box) {
		MessageBox(NULL, msg, func, MB_ICONSTOP | MB_OK | MB_TASKMODAL | MB_SETFOREGROUND);
	} else {
		printf("%d - %s: %s\n", errnr, func, msg);
	}
}

static BOOL ProcessSQLErrorMessages(const char *func) {
	WORD errnr = 1;
	DWORD errcode;
	char errmsg[300];
	WORD errmsglen;
	int rc;
	BOOL func_rc = FALSE;

	do {
		errmsg[0] = '\0';
		rc = SQLInstallerError(errnr, &errcode, errmsg, sizeof(errmsg), &errmsglen);
		if (rc == SQL_SUCCESS || rc == SQL_SUCCESS_WITH_INFO) {
			PrintMsg(func, errmsg, errnr);
			func_rc = TRUE;
		}
		errnr++;
	} while (rc != SQL_NO_DATA);
	return func_rc;
}

static void ProcessSysErrorMessage(DWORD err, const char *func) {
	char *lp_msg_buf;

	FormatMessage(FORMAT_MESSAGE_ALLOCATE_BUFFER | FORMAT_MESSAGE_FROM_SYSTEM | FORMAT_MESSAGE_IGNORE_INSERTS, NULL,
	              err, MAKELANGID(LANG_NEUTRAL, SUBLANG_DEFAULT), (LPTSTR)&lp_msg_buf, 0, NULL);
	PrintMsg(func, lp_msg_buf, 0);
	LocalFree(lp_msg_buf);
}

int CheckIfFileExists(const char *filepath, const char *filename) {
	char buf[300];
	LPTSTR b;

	return SearchPath(filepath, filename, NULL, sizeof(buf), buf, &b) > 0;
}

static BOOL InstallMyDriver(const char *driverpath, const char *drivername) {
	char outpath[301];
	WORD outpathlen;
	DWORD usagecount;
	char *p;

	/* the correct format of driver keywords are
	 * "DriverName\0Driver=...\xxxxxx.DLL\0Setup=...\xxxxxx.DLL\0\0" */
	size_t driverlen = strlen(drivername) + 2 * strlen(driverpath) + strlen(driver_dll) + strlen(driver_dl_ls) + 90;
	char *driver = (char *)malloc(driverlen);
	snprintf(driver, driverlen,
	         "%s;Driver=%s\\%s;Setup=%s\\%s;APILevel=1;"
	         "ConnectFunctions=YYN;DriverODBCVer=%s;SQLLevel=3;",
	         drivername, driverpath, driver_dll, driverpath, driver_dl_ls, duckdb_odbc_ver);

	for (p = driver; *p; p++) {
		if (*p == ';') {
			*p = '\0';
		}
	}

	/* call SQLInstallDriverEx to install the driver in the
	 * registry */
	if (!SQLInstallDriverEx(driver, driverpath, outpath, sizeof(outpath), &outpathlen, ODBC_INSTALL_COMPLETE,
	                        &usagecount) &&
	    ProcessSQLErrorMessages("SQLInstallDriverEx")) {
		free(driver);
		return FALSE;
	}
	free(driver);

	return TRUE;
}

static BOOL RemoveMyDriver(const char *drivername) {
	char buf[300];
	DWORD usagecount;
	DWORD valtype, valsize, rc;

	/* most of this is equivalent to what SQLRemoveDriver is
	   suppposed to do, except that it consistently causes a
	   crash, so we do it ourselves */
	snprintf(buf, sizeof(buf), "SOFTWARE\\ODBC\\ODBCINST.INI\\%s", drivername);
	valsize = sizeof(usagecount);
	usagecount = 0;
	valtype = REG_DWORD;
	rc = SHGetValue(HKEY_LOCAL_MACHINE, buf, "UsageCount", &valtype, &usagecount, &valsize);
	if (rc == ERROR_FILE_NOT_FOUND) {
		/* not installed, do nothing */
		PrintInfoMsg(drivername, "Driver removed already.", 0);
		exit(0);
	}
	if (rc != ERROR_SUCCESS) {
		ProcessSysErrorMessage(rc, "one");
		return FALSE;
	}
	if (usagecount > 1) {
		usagecount--;
		rc = SHSetValue(HKEY_LOCAL_MACHINE, buf, "UsageCount", REG_DWORD, &usagecount, sizeof(usagecount));
		if (rc != ERROR_SUCCESS) {
			ProcessSysErrorMessage(rc, "two");
			return FALSE;
		}
		return TRUE;
	}
	rc = SHDeleteKey(HKEY_LOCAL_MACHINE, buf);
	if (rc != ERROR_SUCCESS) {
		ProcessSysErrorMessage(rc, "three");
		return FALSE;
	}
	rc = SHDeleteValue(HKEY_LOCAL_MACHINE, "SOFTWARE\\ODBC\\ODBCINST.INI\\ODBC Drivers", drivername);
	if (rc != ERROR_SUCCESS) {
		ProcessSysErrorMessage(rc, "four");
		return FALSE;
	}

	return TRUE;
}

static void CreateAttributeString(char *attrs, size_t len, const char *dsn) {
	snprintf(attrs, len, "DSN=%s;Database=:memory:;", dsn);

	for (; *attrs; attrs++) {
		if (*attrs == ';') {
			*attrs = '\0';
		}
	}
}

static BOOL AddMyDSN(const char *dsn, const char *drivername) {
	char attrs[200];

	CreateAttributeString(attrs, sizeof(attrs), dsn);

	/* I choose to remove the DSN if it already existed */
	SQLConfigDataSource(NULL, ODBC_REMOVE_SYS_DSN, drivername, attrs);

	/* then create a new DSN */
	if (!SQLConfigDataSource(NULL, ODBC_ADD_SYS_DSN, drivername, attrs) &&
	    ProcessSQLErrorMessages("SQLConfigDataSource")) {
		return FALSE;
	}

	return TRUE;
}

static BOOL RemoveMyDSN(const char *dsn, const char *drivername) {
	char buf[200];
	char *p;

	snprintf(buf, sizeof(buf), "DSN=%s;", dsn);
	for (p = buf; *p; p++) {
		if (*p == ';') {
			*p = 0;
		}
	}
	SQLConfigDataSource(NULL, ODBC_REMOVE_SYS_DSN, drivername, buf);
	return TRUE;
}

static BOOL Install(const char *driverpath, const char *dsn, const char *drivername) {
	char path[300];
	WORD pathlen;
	BOOL rc;
	DWORD usagecount;

	/* first, retrieve the path the driver should be installed to
	 * in path */
	if (!SQLInstallDriverManager(path, sizeof(path), &pathlen) && ProcessSQLErrorMessages("SQLInstallDriverManager")) {
		return FALSE;
	}

	if (!CheckIfFileExists(path, "odbc32.dll")) {
		PrintMsg("Install", "You must install MDAC before you can use the ODBC driver", 0);
		SQLRemoveDriverManager(&usagecount);
		return FALSE;
	}

	rc = InstallMyDriver(driverpath, drivername);

	if (rc) {
		/* after the driver is installed create the new DSN */
		rc = AddMyDSN(dsn, drivername);
	}

	if (!rc) {
		SQLRemoveDriverManager(&usagecount);
	}

	return rc;
}

static BOOL Uninstall(const char *dsn, const char *drivername) {
	DWORD usagecount;

	RemoveMyDSN(dsn, drivername);
	RemoveMyDriver(drivername);
	SQLRemoveDriverManager(&usagecount);
	return TRUE;
}

void ElevatePrivilegesAsAdmin(char **parameters) {
	char szPath[MAX_PATH];
	if (GetModuleFileName(NULL, szPath, ARRAYSIZE(szPath))) {
		// Launch itself as admin
		SHELLEXECUTEINFO sei = {sizeof(sei)};
		sei.lpVerb = "runas";
		sei.lpFile = szPath;
		sei.lpParameters = *parameters;
		sei.hwnd = NULL;
		sei.nShow = SW_NORMAL;
		if (!ShellExecuteEx(&sei)) {
			DWORD dwError = GetLastError();
			if (dwError == ERROR_CANCELLED) {
				PrintMsg("Admin Privileges", "User did not allow privileges elevation", 0);
			} else {
				PrintMsg("Admin Privileges", "Error during privileges elevation", 0);
			}
			exit(1);
		} else {
			_exit(1);
		}
	}
}

void CopyParameters(int argc, char **argv, char **parameters) {
	size_t alloc_size, last_alloc_pos = 0;
	for (int i = 1; i < argc; i++) {
		alloc_size = strlen(argv[i]) + 1;
		*parameters = (char *)realloc(*parameters, alloc_size);
		strcat(*parameters, argv[i]);
		if ((i + 1) != argc) {
			strcat(*parameters, " ");
		}
	}
}

int main(int argc, char **argv) {
	if (argc > 5) {
		PrintMsg(argv[0], "/CI [/Install | /Uninstall]", 0);
		exit(1);
	}

	char *parameters = NULL;
	if (!IsUserAnAdmin()) {
		CopyParameters(argc, argv, &parameters);
		ElevatePrivilegesAsAdmin(&parameters);
	}

	char *install_cmd = "/Install";
	char *cmd;
	bool is_ci;
	// Default mode is Install to allow double click
	if (argc == 1) {
		cmd = install_cmd;
	} else {
		is_ci = (strcmp("/CI", argv[1]) == 0) ? true : false;
		cmd = is_ci ? argv[2] : argv[1];
		if (is_ci) {
			show_msg_box = false;
		}
	}

	/* after /Install or /Uninstall we optionally accept the DSN and the driver name */
	const char *dsn;
	if (argc > 2) {
		dsn = argv[2];
		// /CI option was provided
		if (is_ci) {
			dsn = (argc == 3) ? data_source_name : argv[3];
		}
	} else {
		dsn = data_source_name;
	}

	const char *drivername;
	if (argc > 3) {
		drivername = argv[3];
		// /CI option was provided
		if (is_ci) {
			drivername = (argc == 4) ? driver_name : argv[4];
		}
	} else {
		drivername = driver_name;
	}

	char buf[MAX_PATH];
	if (GetModuleFileName(NULL, buf, (DWORD)sizeof(buf)) == 0) {
		PrintMsg(argv[0], "Cannot retrieve file location", 0);
		exit(1);
	}

	char *p = strrchr(buf, '\\');
	if (p != NULL) {
		// remove last component
		*p = '\0';
	}

	if (strcmp("/Install", cmd) == 0) {
		if (!Install(buf, dsn, drivername)) {
			PrintMsg(argv[0], "ODBC Install Failed", 0);
			exit(1);
		} else {
			PrintInfoMsg(argv[0], "ODBC Installation completed successfully", 0);
		}
	} else if (strcmp("/Uninstall", cmd) == 0) {
		/* remove file we've installed in previous versions of this program */
		strcat_s(buf, sizeof(buf), "\\ODBCDriverInstalled.txt");
		(void)DeleteFile(buf);

		if (!Uninstall(dsn, drivername)) {
			PrintMsg(argv[0], "ODBC Uninstall Failed", 0);
			exit(1);
		} else {
			PrintInfoMsg(argv[0], "ODBC Uninstall completed successfully", 0);
		}
	} else {
		PrintMsg(argv[0], "[/Install | /Uninstall]", 0);
		exit(1);
	}
	if (parameters) {
		free(parameters);
	}
	return 0;
}
