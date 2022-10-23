package org.duckdb;

import javax.sql.rowset.CachedRowSet;
import javax.sql.rowset.RowSetProvider;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.RowIdLifetime;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Statement;

public class DuckDBDatabaseMetaData implements DatabaseMetaData {
	DuckDBConnection conn;

	public DuckDBDatabaseMetaData(DuckDBConnection conn) {
		this.conn = conn;
	}

	@Override
	public <T> T unwrap(Class<T> iface) throws SQLException {
		throw new SQLFeatureNotSupportedException("unwrap");
	}

	@Override
	public boolean isWrapperFor(Class<?> iface) throws SQLException {
		throw new SQLFeatureNotSupportedException("isWrapperFor");
	}

	@Override
	public boolean allProceduresAreCallable() throws SQLException {
		return false;
	}

	@Override
	public boolean allTablesAreSelectable() throws SQLException {
		return true;
	}

	@Override
	public String getURL() throws SQLException {
		return conn.db.url;
	}

	@Override
	public String getUserName() throws SQLException {
		return "";
	}

	@Override
	public boolean isReadOnly() throws SQLException {
		return false;
	}

	@Override
	public boolean nullsAreSortedHigh() throws SQLException {
		return true;
	}

	@Override
	public boolean nullsAreSortedLow() throws SQLException {
		return false;
	}

	@Override
	public boolean nullsAreSortedAtStart() throws SQLException {
		return true;
	}

	@Override
	public boolean nullsAreSortedAtEnd() throws SQLException {
		return false;
	}

	@Override
	public String getDatabaseProductName() throws SQLException {
		return "DuckDB";
	}

	@Override
	public String getDatabaseProductVersion() throws SQLException {
		Statement s = conn.createStatement();
		ResultSet rs = s.executeQuery("PRAGMA version");
		rs.next();
		String result = rs.getString(1);
		rs.close();
		s.close();
		return result;
	}

	@Override
	public String getDriverName() throws SQLException {
		return "DuckDBJ";
	}

	@Override
	public String getDriverVersion() throws SQLException {
		return "1.0";
	}

	@Override
	public int getDriverMajorVersion() {
		return 1;
	}

	@Override
	public int getDriverMinorVersion() {
		return 0;
	}

	@Override
	public boolean usesLocalFiles() throws SQLException {
		return true;
	}

	@Override
	public boolean usesLocalFilePerTable() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsMixedCaseIdentifiers() throws SQLException {
		return true;
	}

	@Override
	public boolean storesUpperCaseIdentifiers() throws SQLException {
		return false;
	}

	@Override
	public boolean storesLowerCaseIdentifiers() throws SQLException {
		return false;
	}

	@Override
	public boolean storesMixedCaseIdentifiers() throws SQLException {
		return true;
	}

	@Override
	public boolean supportsMixedCaseQuotedIdentifiers() throws SQLException {
		return true;
	}

	@Override
	public boolean storesUpperCaseQuotedIdentifiers() throws SQLException {
		return false;
	}

	@Override
	public boolean storesLowerCaseQuotedIdentifiers() throws SQLException {
		return false;
	}

	@Override
	public boolean storesMixedCaseQuotedIdentifiers() throws SQLException {
		return true;
	}

	@Override
	public String getIdentifierQuoteString() throws SQLException {
		return "\"";
	}

	@Override
	public String getSQLKeywords() throws SQLException {
		throw new SQLFeatureNotSupportedException("getSQLKeywords");
	}

	@Override
	public String getNumericFunctions() throws SQLException {
		throw new SQLFeatureNotSupportedException("getNumericFunctions");
	}

	@Override
	public String getStringFunctions() throws SQLException {
		throw new SQLFeatureNotSupportedException("getStringFunctions");
	}

	@Override
	public String getSystemFunctions() throws SQLException {
		throw new SQLFeatureNotSupportedException("getSystemFunctions");
	}

	@Override
	public String getTimeDateFunctions() throws SQLException {
		throw new SQLFeatureNotSupportedException("getTimeDateFunctions");
	}

	@Override
	public String getSearchStringEscape() throws SQLException {
		return null;
	}

	@Override
	public String getExtraNameCharacters() throws SQLException {
		return "";
	}

	@Override
	public boolean supportsAlterTableWithAddColumn() throws SQLException {
		return true;
	}

	@Override
	public boolean supportsAlterTableWithDropColumn() throws SQLException {
		return true;
	}

	@Override
	public boolean supportsColumnAliasing() throws SQLException {
		return true;
	}

	@Override
	public boolean nullPlusNonNullIsNull() throws SQLException {
		return true;
	}

	@Override
	public boolean supportsConvert() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsConvert(int fromType, int toType) throws SQLException {
		throw new SQLFeatureNotSupportedException("supportsConvert");
	}

	@Override
	public boolean supportsTableCorrelationNames() throws SQLException {
		throw new SQLFeatureNotSupportedException("supportsTableCorrelationNames");
	}

	@Override
	public boolean supportsDifferentTableCorrelationNames() throws SQLException {
		throw new SQLFeatureNotSupportedException("supportsDifferentTableCorrelationNames");
	}

	@Override
	public boolean supportsExpressionsInOrderBy() throws SQLException {
		return true;
	}

	@Override
	public boolean supportsOrderByUnrelated() throws SQLException {
		return true;
	}

	@Override
	public boolean supportsGroupBy() throws SQLException {
		return true;
	}

	@Override
	public boolean supportsGroupByUnrelated() throws SQLException {
		return true;
	}

	@Override
	public boolean supportsGroupByBeyondSelect() throws SQLException {
		return true;
	}

	@Override
	public boolean supportsLikeEscapeClause() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsMultipleResultSets() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsMultipleTransactions() throws SQLException {
		return true;
	}

	@Override
	public boolean supportsNonNullableColumns() throws SQLException {
		return true;
	}

	@Override
	public boolean supportsMinimumSQLGrammar() throws SQLException {
		return true;
	}

	@Override
	public boolean supportsCoreSQLGrammar() throws SQLException {
		return true;
	}

	@Override
	public boolean supportsExtendedSQLGrammar() throws SQLException {
		return true;
	}

	@Override
	public boolean supportsANSI92EntryLevelSQL() throws SQLException {
		return true;
	}

	@Override
	public boolean supportsANSI92IntermediateSQL() throws SQLException {
		return true;
	}

	@Override
	public boolean supportsANSI92FullSQL() throws SQLException {
		return true;
	}

	@Override
	public boolean supportsIntegrityEnhancementFacility() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsOuterJoins() throws SQLException {
		return true;
	}

	@Override
	public boolean supportsFullOuterJoins() throws SQLException {
		return true;
	}

	@Override
	public boolean supportsLimitedOuterJoins() throws SQLException {
		return true;
	}

	@Override
	public String getSchemaTerm() throws SQLException {
		return "schema";
	}

	@Override
	public String getProcedureTerm() throws SQLException {
		return "procedure";
	}

	@Override
	public String getCatalogTerm() throws SQLException {
		return "catalog";
	}

	@Override
	public boolean isCatalogAtStart() throws SQLException {
		return true;
	}

	@Override
	public String getCatalogSeparator() throws SQLException {
		return ".";
	}

	@Override
	public boolean supportsSchemasInDataManipulation() throws SQLException {
		return true;
	}

	@Override
	public boolean supportsSchemasInProcedureCalls() throws SQLException {
		return true;
	}

	@Override
	public boolean supportsSchemasInTableDefinitions() throws SQLException {
		return true;
	}

	@Override
	public boolean supportsSchemasInIndexDefinitions() throws SQLException {
		return true;
	}

	@Override
	public boolean supportsSchemasInPrivilegeDefinitions() throws SQLException {
		throw new SQLFeatureNotSupportedException("supportsSchemasInPrivilegeDefinitions");
	}

	@Override
	public boolean supportsCatalogsInDataManipulation() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsCatalogsInProcedureCalls() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsCatalogsInTableDefinitions() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsCatalogsInIndexDefinitions() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsCatalogsInPrivilegeDefinitions() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsPositionedDelete() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsPositionedUpdate() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsSelectForUpdate() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsStoredProcedures() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsSubqueriesInComparisons() throws SQLException {
		return true;
	}

	@Override
	public boolean supportsSubqueriesInExists() throws SQLException {
		return true;
	}

	@Override
	public boolean supportsSubqueriesInIns() throws SQLException {
		return true;
	}

	@Override
	public boolean supportsSubqueriesInQuantifieds() throws SQLException {
		return true;
	}

	@Override
	public boolean supportsCorrelatedSubqueries() throws SQLException {
		return true;
	}

	@Override
	public boolean supportsUnion() throws SQLException {
		return true;
	}

	@Override
	public boolean supportsUnionAll() throws SQLException {
		return true;
	}

	@Override
	public boolean supportsOpenCursorsAcrossCommit() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsOpenCursorsAcrossRollback() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsOpenStatementsAcrossCommit() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsOpenStatementsAcrossRollback() throws SQLException {
		return false;
	}

	@Override
	public int getMaxBinaryLiteralLength() throws SQLException {

		return 0;
	}

	@Override
	public int getMaxCharLiteralLength() throws SQLException {
		return 0;
	}

	@Override
	public int getMaxColumnNameLength() throws SQLException {
		return 0;
	}

	@Override
	public int getMaxColumnsInGroupBy() throws SQLException {
		return 0;
	}

	@Override
	public int getMaxColumnsInIndex() throws SQLException {
		return 0;
	}

	@Override
	public int getMaxColumnsInOrderBy() throws SQLException {
		return 0;
	}

	@Override
	public int getMaxColumnsInSelect() throws SQLException {
		return 0;
	}

	@Override
	public int getMaxColumnsInTable() throws SQLException {
		return 0;
	}

	@Override
	public int getMaxConnections() throws SQLException {
		return 0;
	}

	@Override
	public int getMaxCursorNameLength() throws SQLException {
		return 0;
	}

	@Override
	public int getMaxIndexLength() throws SQLException {
		return 0;
	}

	@Override
	public int getMaxSchemaNameLength() throws SQLException {
		return 0;
	}

	@Override
	public int getMaxProcedureNameLength() throws SQLException {
		return 0;
	}

	@Override
	public int getMaxCatalogNameLength() throws SQLException {
		return 0;
	}

	@Override
	public int getMaxRowSize() throws SQLException {
		return 0;
	}

	@Override
	public boolean doesMaxRowSizeIncludeBlobs() throws SQLException {
		return false;
	}

	@Override
	public int getMaxStatementLength() throws SQLException {
		return 0;
	}

	@Override
	public int getMaxStatements() throws SQLException {
		return 0;
	}

	@Override
	public int getMaxTableNameLength() throws SQLException {
		return 0;
	}

	@Override
	public int getMaxTablesInSelect() throws SQLException {
		return 0;
	}

	@Override
	public int getMaxUserNameLength() throws SQLException {
		return 0;
	}

	@Override
	public int getDefaultTransactionIsolation() throws SQLException {
		return Connection.TRANSACTION_REPEATABLE_READ;
	}

	@Override
	public boolean supportsTransactions() throws SQLException {
		return true;
	}

	@Override
	public boolean supportsTransactionIsolationLevel(int level) throws SQLException {
		return level < Connection.TRANSACTION_SERIALIZABLE;
	}

	@Override
	public boolean supportsDataDefinitionAndDataManipulationTransactions() throws SQLException {
		return true;
	}

	@Override
	public boolean supportsDataManipulationTransactionsOnly() throws SQLException {
		return false;
	}

	@Override
	public boolean dataDefinitionCausesTransactionCommit() throws SQLException {
		return false;
	}

	@Override
	public boolean dataDefinitionIgnoredInTransactions() throws SQLException {
		return false;
	}

	@Override
	public ResultSet getCatalogs() throws SQLException {
		return conn.createStatement().executeQuery(
				"SELECT DISTINCT catalog_name AS 'TABLE_CAT' FROM information_schema.schemata ORDER BY \"TABLE_CAT\"");
	}

	@Override
	public ResultSet getSchemas() throws SQLException {
		return conn.createStatement().executeQuery(
				"SELECT schema_name AS 'TABLE_SCHEM', catalog_name AS 'TABLE_CATALOG' FROM information_schema.schemata ORDER BY \"TABLE_CATALOG\", \"TABLE_SCHEM\"");
	}

	@Override
	public ResultSet getSchemas(String catalog, String schemaPattern) throws SQLException {
		if (catalog != null && !catalog.isEmpty()) {
			throw new SQLException("catalog argument is not supported");
		}
		PreparedStatement ps = conn.prepareStatement(
				"SELECT schema_name AS 'TABLE_SCHEM', catalog_name AS 'TABLE_CATALOG' FROM information_schema.schemata WHERE schema_name LIKE ? ORDER BY \"TABLE_CATALOG\", \"TABLE_SCHEM\"");
		ps.setString(1, schemaPattern);
		return ps.executeQuery();
	}

	@Override
	public ResultSet getTableTypes() throws SQLException {
		return conn.createStatement().executeQuery(
				"SELECT DISTINCT table_type AS 'TABLE_TYPE' FROM information_schema.tables ORDER BY \"TABLE_TYPE\"");
	}

	@Override
	public ResultSet getTables(String catalog, String schemaPattern, String tableNamePattern, String[] types)
			throws SQLException {
		if (catalog != null && !catalog.isEmpty()) {
			throw new SQLException("Actual catalog argument is not supported, got " + catalog);
		}
		String table_type_str = "";
		if (types != null && types.length > 0) {
			table_type_str = "table_type IN (";
			for (int i = 0; i < types.length; i++) {
				table_type_str += "'" + types[i] + "'";
				if (i < types.length - 1) {
					table_type_str += ',';
				}
			}
			table_type_str += ") AND ";
		}
		if (schemaPattern == null) {
			schemaPattern = "%";
		}
		if (tableNamePattern == null) {
			tableNamePattern = "%";
		}
		PreparedStatement ps = conn.prepareStatement(
				"SELECT table_catalog AS 'TABLE_CAT', table_schema AS 'TABLE_SCHEM', table_name AS 'TABLE_NAME', table_type as 'TABLE_TYPE', NULL AS 'REMARKS', NULL AS 'TYPE_CAT', NULL AS 'TYPE_SCHEM', NULL AS 'TYPE_NAME', NULL as 'SELF_REFERENCING_COL_NAME', NULL as 'REF_GENERATION' FROM information_schema.tables WHERE "
						+ table_type_str
						+ " table_schema LIKE ? AND table_name LIKE ? ORDER BY \"TABLE_TYPE\", \"TABLE_CAT\", \"TABLE_SCHEM\", \"TABLE_NAME\"");
		ps.setString(1, schemaPattern);
		ps.setString(2, tableNamePattern);
		return ps.executeQuery();

	}

	@Override
	public ResultSet getColumns(String catalog, String schemaPattern, String tableNamePattern, String columnNamePattern)
			throws SQLException {
		if (catalog != null && !catalog.isEmpty()) {
			throw new SQLException("catalog argument is not supported");
		}
		if (schemaPattern == null) {
			schemaPattern = "%";
		}
		if (tableNamePattern == null) {
			tableNamePattern = "%";
		}
		if (columnNamePattern == null) {
			columnNamePattern = "%";
		}

		// need to figure out the java types for the sql types :/
		String values_str = "VALUES(NULL::STRING, NULL::INTEGER)";
		Statement gunky_statement = conn.createStatement();
		// TODO this could get slow with many many columns and we really only need the
		// types :/
		ResultSet rs = gunky_statement
				.executeQuery("SELECT DISTINCT data_type FROM information_schema.columns ORDER BY data_type");
		while (rs.next()) {
			values_str += ", ('" + rs.getString(1) + "', " + Integer.toString(
					DuckDBResultSetMetaData.type_to_int(DuckDBResultSetMetaData.TypeNameToType(rs.getString(1)))) + ")";
		}
		rs.close();
		gunky_statement.close();

		PreparedStatement ps = conn.prepareStatement(
				"SELECT table_catalog AS 'TABLE_CAT', table_schema AS 'TABLE_SCHEM', table_name AS 'TABLE_NAME', column_name as 'COLUMN_NAME', type_id AS 'DATA_TYPE', c.data_type AS 'TYPE_NAME', NULL AS 'COLUMN_SIZE', NULL AS 'BUFFER_LENGTH', numeric_precision AS 'DECIMAL_DIGITS', 10 AS 'NUM_PREC_RADIX', CASE WHEN is_nullable = 'YES' THEN 1 else 0 END AS 'NULLABLE', NULL as 'REMARKS', column_default AS 'COLUMN_DEF', NULL AS 'SQL_DATA_TYPE', NULL AS 'SQL_DATETIME_SUB', character_octet_length AS 'CHAR_OCTET_LENGTH', ordinal_position AS 'ORDINAL_POSITION', is_nullable AS 'IS_NULLABLE', NULL AS 'SCOPE_CATALOG', NULL AS 'SCOPE_SCHEMA', NULL AS 'SCOPE_TABLE', NULL AS 'SOURCE_DATA_TYPE', '' AS 'IS_AUTOINCREMENT', '' AS 'IS_GENERATEDCOLUMN'  FROM information_schema.columns c JOIN ("
						+ values_str
						+ ") t(type_name, type_id) ON c.data_type = t.type_name WHERE table_schema LIKE ? AND table_name LIKE ? AND column_name LIKE ? ORDER BY \"TABLE_CAT\",\"TABLE_SCHEM\", \"TABLE_NAME\", \"ORDINAL_POSITION\"");
		ps.setString(1, schemaPattern);
		ps.setString(2, tableNamePattern);
		ps.setString(3, columnNamePattern);
		return ps.executeQuery();

	}

	@Override
	public ResultSet getColumnPrivileges(String catalog, String schema, String table, String columnNamePattern)
			throws SQLException {
		throw new SQLFeatureNotSupportedException("getColumnPrivileges");
	}

	@Override
	public ResultSet getTablePrivileges(String catalog, String schemaPattern, String tableNamePattern)
			throws SQLException {
		throw new SQLFeatureNotSupportedException("getTablePrivileges");
	}

	@Override
	public ResultSet getProcedures(String catalog, String schemaPattern, String procedureNamePattern)
			throws SQLException {
		return conn.createStatement().executeQuery("SELECT NULL WHERE FALSE");
	}

	@Override
	public ResultSet getProcedureColumns(String catalog, String schemaPattern, String procedureNamePattern,
			String columnNamePattern) throws SQLException {
		return conn.createStatement().executeQuery("SELECT NULL WHERE FALSE");
	}

	@Override
	public ResultSet getBestRowIdentifier(String catalog, String schema, String table, int scope, boolean nullable)
			throws SQLException {
		throw new SQLFeatureNotSupportedException("getBestRowIdentifier");
	}

	@Override
	public ResultSet getVersionColumns(String catalog, String schema, String table) throws SQLException {
		throw new SQLFeatureNotSupportedException("getVersionColumns");
	}

	@Override
	public ResultSet getPrimaryKeys(String catalog, String schema, String table) throws SQLException {
		throw new SQLFeatureNotSupportedException("getPrimaryKeys");
	}

	@Override
	public ResultSet getImportedKeys(String catalog, String schema, String table) throws SQLException {
		throw new SQLFeatureNotSupportedException("getImportedKeys");
	}

	@Override
	public ResultSet getExportedKeys(String catalog, String schema, String table) throws SQLException {
		throw new SQLFeatureNotSupportedException("getExportedKeys");
	}

	@Override
	public ResultSet getCrossReference(String parentCatalog, String parentSchema, String parentTable,
			String foreignCatalog, String foreignSchema, String foreignTable) throws SQLException {
		throw new SQLFeatureNotSupportedException("getCrossReference");
	}

	@Override
	public ResultSet getTypeInfo() throws SQLException {
		throw new SQLFeatureNotSupportedException("getTypeInfo");
	}

	@Override
	public ResultSet getIndexInfo(String catalog, String schema, String table, boolean unique, boolean approximate)
			throws SQLException {
		throw new SQLFeatureNotSupportedException("getIndexInfo(");
	}

	@Override
	public boolean supportsResultSetType(int type) throws SQLException {
		return type == ResultSet.TYPE_FORWARD_ONLY;
	}

	@Override
	public boolean supportsResultSetConcurrency(int type, int concurrency) throws SQLException {
		return type == ResultSet.TYPE_FORWARD_ONLY && concurrency == ResultSet.CONCUR_READ_ONLY;
	}

	@Override
	public boolean ownUpdatesAreVisible(int type) throws SQLException {
		throw new SQLFeatureNotSupportedException("ownUpdatesAreVisible");
	}

	@Override
	public boolean ownDeletesAreVisible(int type) throws SQLException {
		throw new SQLFeatureNotSupportedException("ownDeletesAreVisible");
	}

	@Override
	public boolean ownInsertsAreVisible(int type) throws SQLException {
		throw new SQLFeatureNotSupportedException("ownInsertsAreVisible");
	}

	@Override
	public boolean othersUpdatesAreVisible(int type) throws SQLException {
		throw new SQLFeatureNotSupportedException("othersUpdatesAreVisible");
	}

	@Override
	public boolean othersDeletesAreVisible(int type) throws SQLException {
		throw new SQLFeatureNotSupportedException("othersDeletesAreVisible");
	}

	@Override
	public boolean othersInsertsAreVisible(int type) throws SQLException {
		throw new SQLFeatureNotSupportedException("othersInsertsAreVisible");
	}

	@Override
	public boolean updatesAreDetected(int type) throws SQLException {
		throw new SQLFeatureNotSupportedException("updatesAreDetected");
	}

	@Override
	public boolean deletesAreDetected(int type) throws SQLException {
		throw new SQLFeatureNotSupportedException("deletesAreDetected");
	}

	@Override
	public boolean insertsAreDetected(int type) throws SQLException {
		throw new SQLFeatureNotSupportedException("insertsAreDetected");
	}

	@Override
	public boolean supportsBatchUpdates() throws SQLException {
		throw new SQLFeatureNotSupportedException("supportsBatchUpdates");
	}

	@Override
	public ResultSet getUDTs(String catalog, String schemaPattern, String typeNamePattern, int[] types)
			throws SQLException {
		throw new SQLFeatureNotSupportedException("getUDTs");
	}

	@Override
	public Connection getConnection() throws SQLException {
		return conn;
	}

	@Override
	public boolean supportsSavepoints() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsNamedParameters() throws SQLException {
		return false;
	}

	@Override
	public boolean supportsMultipleOpenResults() throws SQLException {
		return true;
	}

	@Override
	public boolean supportsGetGeneratedKeys() throws SQLException {
		return false;
	}

	@Override
	public ResultSet getSuperTypes(String catalog, String schemaPattern, String typeNamePattern) throws SQLException {
		throw new SQLFeatureNotSupportedException("getSuperTypes");
	}

	@Override
	public ResultSet getSuperTables(String catalog, String schemaPattern, String tableNamePattern) throws SQLException {
		throw new SQLFeatureNotSupportedException("getSuperTables");
	}

	@Override
	public ResultSet getAttributes(String catalog, String schemaPattern, String typeNamePattern,
			String attributeNamePattern) throws SQLException {
		throw new SQLFeatureNotSupportedException("getAttributes");
	}

	@Override
	public boolean supportsResultSetHoldability(int holdability) throws SQLException {
		throw new SQLFeatureNotSupportedException("supportsResultSetHoldability");
	}

	@Override
	public int getResultSetHoldability() throws SQLException {
		throw new SQLFeatureNotSupportedException("getResultSetHoldability");
	}

	@Override
	public int getDatabaseMajorVersion() throws SQLException {
		return 1;
	}

	@Override
	public int getDatabaseMinorVersion() throws SQLException {
		return 0;
	}

	@Override
	public int getJDBCMajorVersion() throws SQLException {
		return 1;
	}

	@Override
	public int getJDBCMinorVersion() throws SQLException {
		return 0;
	}

	@Override
	public int getSQLStateType() throws SQLException {
		throw new SQLFeatureNotSupportedException("getSQLStateType");
	}

	@Override
	public boolean locatorsUpdateCopy() throws SQLException {
		throw new SQLFeatureNotSupportedException("locatorsUpdateCopy");
	}

	@Override
	public boolean supportsStatementPooling() throws SQLException {
		throw new SQLFeatureNotSupportedException("supportsStatementPooling");
	}

	@Override
	public RowIdLifetime getRowIdLifetime() throws SQLException {
		throw new SQLFeatureNotSupportedException("getRowIdLifetime");
	}

	@Override
	public boolean supportsStoredFunctionsUsingCallSyntax() throws SQLException {
		throw new SQLFeatureNotSupportedException("supportsStoredFunctionsUsingCallSyntax");
	}

	@Override
	public boolean autoCommitFailureClosesAllResultSets() throws SQLException {
		throw new SQLFeatureNotSupportedException("autoCommitFailureClosesAllResultSets");
	}

	@Override
	public ResultSet getClientInfoProperties() throws SQLException {
		throw new SQLFeatureNotSupportedException("getClientInfoProperties");
	}

	/**
	 *
	 * @param catalog a catalog name; must match the catalog name as it
	 *        is stored in the database; "" retrieves those without a catalog;
	 *        <code>null</code> means that the catalog name should not be used to narrow
	 *        the search
	 * @param schemaPattern a schema name pattern; must match the schema name
	 *        as it is stored in the database; "" retrieves those without a schema;
	 *        <code>null</code> means that the schema name should not be used to narrow
	 *        the search
	 * @param functionNamePattern a function name pattern; must match the
	 *        function name as it is stored in the database
	 * FUNCTION_CAT String => function catalog (may be null)
	 * FUNCTION_SCHEM String => function schema (may be null)
	 * FUNCTION_NAME String => function name. This is the name used to invoke the function
	 * REMARKS String => explanatory comment on the function
	 * FUNCTION_TYPE short => kind of function:
	 *  - functionResultUnknown - Cannot determine if a return value or table will be returned
	 *  - functionNoTable- Does not return a table
	 *  - functionReturnsTable - Returns a table
	 * SPECIFIC_NAME String => the name which uniquely identifies this function within its schema. This is a user specified, or DBMS generated, name that may be different then the FUNCTION_NAME for example with overload functions
	 */
	@Override
	public ResultSet getFunctions(String catalog, String schemaPattern, String functionNamePattern)
			throws SQLException {
		try (PreparedStatement statement = conn.prepareStatement(
				"SELECT " +
						"null as FUNCTION_CAT, " +
						"function_name as FUNCTION_NAME, " +
						"schema_name as FUNCTION_SCHEM, " +
						"description as REMARKS," +
						"CASE function_type " +
						"WHEN 'table' THEN " + functionReturnsTable + " " +
						"WHEN 'table_macro' THEN " + functionReturnsTable + " " +
						"ELSE " + functionNoTable + " " +
						"END as FUNCTION_TYPE " +
						"FROM duckdb_functions() " +
						"WHERE function_name like ? and " +
						"schema_name like ?"
		)) {
			statement.setString(1, functionNamePattern);
			statement.setString(2, schemaPattern);

			CachedRowSet cachedRowSet = RowSetProvider.newFactory().createCachedRowSet();
			cachedRowSet.populate(statement.executeQuery());
			return cachedRowSet;
		}
	}

	@Override
	public ResultSet getFunctionColumns(String catalog, String schemaPattern, String functionNamePattern,
			String columnNamePattern) throws SQLException {
		return conn.createStatement().executeQuery("SELECT NULL WHERE FALSE");
	}

	@Override
	public ResultSet getPseudoColumns(String catalog, String schemaPattern, String tableNamePattern,
			String columnNamePattern) throws SQLException {
		throw new SQLFeatureNotSupportedException("getPseudoColumns");
	}

	@Override
	public boolean generatedKeyAlwaysReturned() throws SQLException {
		throw new SQLFeatureNotSupportedException("generatedKeyAlwaysReturned");
	}

}
