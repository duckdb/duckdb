#include <windows.h>
#include <winver.h>
#include <shlwapi.h>
#include <sqlext.h>
#include <sql.h>
#include <odbcinst.h>
#include <stdio.h>
#include <string.h>

static const char *DriverName = "DuckDB Driver";
static const char *DataSourceName = "DuckDB";
static const char *DriverDLL = "duckdb_odbc.dll";
static const char *DriverDLLs = "duckdb_odbc_setup.dll";
static const char *DUCKDB_ODBC_VER = "3.0";

/* General error handler for installer functions */

static BOOL
ProcessSQLErrorMessages(const char *func)
{
	WORD errnr = 1;
	DWORD errcode;
	char errmsg[300];
	WORD errmsglen;
	int rc;
	BOOL func_rc = FALSE;

	do {
		errmsg[0] = '\0';
		rc = SQLInstallerError(errnr, &errcode,
							   errmsg, sizeof(errmsg), &errmsglen);
		if (rc == SQL_SUCCESS || rc == SQL_SUCCESS_WITH_INFO) {
			MessageBox(NULL, errmsg, func,
					   MB_ICONSTOP | MB_OK | MB_TASKMODAL | MB_SETFOREGROUND);
			func_rc = TRUE;
		}
		errnr++;
	} while (rc != SQL_NO_DATA);
	return func_rc;
}

static void
ProcessSysErrorMessage(DWORD err, const char *func)
{
	char *lpMsgBuf;

	FormatMessage(FORMAT_MESSAGE_ALLOCATE_BUFFER | FORMAT_MESSAGE_FROM_SYSTEM | FORMAT_MESSAGE_IGNORE_INSERTS,
				  NULL, err, MAKELANGID(LANG_NEUTRAL, SUBLANG_DEFAULT),
				  (LPTSTR) & lpMsgBuf, 0, NULL);
	MessageBox(NULL, (LPCTSTR) lpMsgBuf, func, MB_OK | MB_ICONINFORMATION);
	LocalFree(lpMsgBuf);
}

int
CheckIfFileExists(const char *filepath, const char *filename)
{
	char buf[300];
	LPTSTR b;

	return SearchPath(filepath, filename, NULL, sizeof(buf), buf, &b) > 0;
}

static BOOL
InstallMyDriver(const char *driverpath, const char *drivername)
{
	char outpath[301];
	WORD outpathlen;
	DWORD usagecount;
	char *p;

	/* the correct format of driver keywords are
	 * "DriverName\0Driver=...\xxxxxx.DLL\0Setup=...\xxxxxx.DLL\0\0" */
	size_t driverlen = strlen(drivername) + 2 * strlen(driverpath) + strlen(DriverDLL) + strlen(DriverDLLs) + 90;
	char *driver = (char *)malloc(driverlen);
	snprintf(driver, driverlen,
			 "%s;Driver=%s\\%s;Setup=%s\\%s;APILevel=1;"
			 "ConnectFunctions=YYN;DriverODBCVer=%s;SQLLevel=3;",
			 drivername, driverpath, DriverDLL, driverpath, DriverDLLs,
			 DUCKDB_ODBC_VER);

	for (p = driver; *p; p++) {
		if (*p == ';') {
			*p = '\0';
		}
	}

	/* call SQLInstallDriverEx to install the driver in the
	 * registry */
	if (!SQLInstallDriverEx(driver, driverpath,
							outpath, sizeof(outpath), &outpathlen,
							ODBC_INSTALL_COMPLETE, &usagecount) &&
	    ProcessSQLErrorMessages("SQLInstallDriverEx")) {
		free(driver);
		return FALSE;
	}
	free(driver);

	return TRUE;
}

static BOOL
RemoveMyDriver(const char *drivername)
{
	char buf[300];
	DWORD usagecount;
	DWORD valtype, valsize, rc;

	/* most of this is equivalent to what SQLRemoveDriver is
	   suppposed to do, except that it consistently causes a
	   crash, so we do it ourselves */
	snprintf(buf, sizeof(buf), "SOFTWARE\\ODBC\\ODBCINST.INI\\%s",
			 drivername);
	valsize = sizeof(usagecount);
	usagecount = 0;
	valtype = REG_DWORD;
	rc = SHGetValue(HKEY_LOCAL_MACHINE, buf, "UsageCount",
					&valtype, &usagecount, &valsize);
	if (rc == ERROR_FILE_NOT_FOUND) {
		/* not installed, do nothing */
		exit(0);
	}
	if (rc != ERROR_SUCCESS) {
		ProcessSysErrorMessage(rc, "one");
		return FALSE;
	}
	if (usagecount > 1) {
		usagecount--;
		rc = SHSetValue(HKEY_LOCAL_MACHINE, buf, "UsageCount",
						REG_DWORD, &usagecount, sizeof(usagecount));
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
	rc = SHDeleteValue(HKEY_LOCAL_MACHINE,
					   "SOFTWARE\\ODBC\\ODBCINST.INI\\ODBC Drivers",
					   drivername);
	if (rc != ERROR_SUCCESS) {
		ProcessSysErrorMessage(rc, "four");
		return FALSE;
	}

	return TRUE;
}

static void
CreateAttributeString(char *attrs, size_t len, const char *dsn)
{
	snprintf(attrs, len,
			 "DSN=%s;Database=:memory:;",
			 dsn);

	for (; *attrs; attrs++)
		if (*attrs == ';')
			*attrs = '\0';
}

static BOOL
AddMyDSN(const char *dsn, const char *drivername)
{
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

static BOOL
RemoveMyDSN(const char *dsn, const char *drivername)
{
	char buf[200];
	char *p;

	snprintf(buf, sizeof(buf), "DSN=%s;", dsn);
	for (p = buf; *p; p++)
		if (*p == ';')
			*p = 0;
	SQLConfigDataSource(NULL, ODBC_REMOVE_SYS_DSN, drivername, buf);
	return TRUE;
}

static BOOL
Install(const char *driverpath, const char *dsn, const char *drivername)
{
	char path[300];
	WORD pathlen;
	BOOL rc;
	DWORD usagecount;

	/* first, retrieve the path the driver should be installed to
	 * in path */
	if (!SQLInstallDriverManager(path, sizeof(path), &pathlen) &&
	    ProcessSQLErrorMessages("SQLInstallDriverManager"))
		return FALSE;

	if (!CheckIfFileExists(path, "odbc32.dll")) {
		MessageBox(NULL,
				   "You must install MDAC before you can use the ODBC driver",
				   "Install",
				   MB_ICONSTOP | MB_OK | MB_TASKMODAL | MB_SETFOREGROUND);
		SQLRemoveDriverManager(&usagecount);
		return FALSE;
	}

	rc = InstallMyDriver(driverpath, drivername);

	if (rc) {
		/* after the driver is installed create the new DSN */
		rc = AddMyDSN(dsn, drivername);
	}

	if (!rc)
		SQLRemoveDriverManager(&usagecount);

	return rc;
}

static BOOL
Uninstall(const char *dsn, const char *drivername)
{
	DWORD usagecount;

	RemoveMyDSN(dsn, drivername);
	RemoveMyDriver(drivername);
	SQLRemoveDriverManager(&usagecount);
	return TRUE;
}

int main(int argc, char **argv)
{
	char buf[MAX_PATH];

	if (GetModuleFileName(NULL, buf, (DWORD) sizeof(buf)) == 0) {
		MessageBox(NULL, "Cannot retrieve file location",
				   argv[0],
				   MB_ICONSTOP | MB_OK | MB_TASKMODAL | MB_SETFOREGROUND);
		exit(1);
	}

	char *p = strrchr(buf, '\\');
	if (p != NULL) {
		// remove last component
		*p = '\0';
		if (p > buf + 4 && strcmp(p - 4, "\\bin") == 0) {
			// also remove \bin directory
			p -= 4;
			*p = '\0';
		}
	}
	/* else {
		strcpy(buf, ".");
		p = buf + 1;
	} */


	if (argc < 2 || argc > 4) {
		MessageBox(NULL, "/Install or /Uninstall argument expected",
				   argv[0],
				   MB_ICONSTOP | MB_OK | MB_TASKMODAL | MB_SETFOREGROUND);
		exit(1);
	}

	/* after /Install or /Uninstall we optionally accept the DSN and
	 * the driver name */
	const char *dsn = argc >= 3 ? argv[2] : DataSourceName;
	const char *drivername = argc >= 4 ? argv[3] : DriverName;

	if (strcmp("/Install", argv[1]) == 0) {
		if (!Install(buf, dsn, drivername)) {
			MessageBox(NULL, "ODBC Install Failed", argv[0],
					   MB_ICONSTOP | MB_OK | MB_TASKMODAL | MB_SETFOREGROUND);
			exit(1);
		}
	} else if (strcmp("/Uninstall", argv[1]) == 0) {
		/* remove file we've installed in previous versions of this program */
		strcat_s(buf, sizeof(buf), "\\ODBCDriverInstalled.txt");
		(void) DeleteFile(buf);

		if (!Uninstall(dsn, drivername)) {
			MessageBox(NULL, "ODBC Uninstall Failed", argv[0],
					   MB_ICONSTOP | MB_OK | MB_TASKMODAL | MB_SETFOREGROUND);
			exit(1);
		}
	} else {
		MessageBox(NULL, "/Install or /Uninstall argument expected",
				   argv[0],
				   MB_ICONSTOP | MB_OK | MB_TASKMODAL | MB_SETFOREGROUND);
		exit(1);
	}
	return 0;
}
