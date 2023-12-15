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

import static java.lang.System.lineSeparator;

public class DuckDBDatabaseMetaData implements DatabaseMetaData {
    DuckDBConnection conn;

    public DuckDBDatabaseMetaData(DuckDBConnection conn) {
        this.conn = conn;
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        return JdbcUtils.unwrap(this, iface);
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) {
        return iface.isInstance(this);
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
        return conn.url;
    }

    @Override
    public String getUserName() throws SQLException {
        return "";
    }

    @Override
    public boolean isReadOnly() throws SQLException {
        return conn.isReadOnly();
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
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery("PRAGMA version")) {
            rs.next();
            String result = rs.getString(1);
            return result;
        }
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
        return false;
    }

    @Override
    public boolean supportsTableCorrelationNames() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsDifferentTableCorrelationNames() throws SQLException {
        return false;
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
        return true;
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
        return false;
    }

    @Override
    public boolean supportsCatalogsInDataManipulation() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsCatalogsInProcedureCalls() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsCatalogsInTableDefinitions() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsCatalogsInIndexDefinitions() throws SQLException {
        return true;
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
        Statement statement = conn.createStatement();
        statement.closeOnCompletion();
        return statement.executeQuery(
            "SELECT DISTINCT catalog_name AS 'TABLE_CAT' FROM information_schema.schemata ORDER BY \"TABLE_CAT\"");
    }

    @Override
    public ResultSet getSchemas() throws SQLException {
        Statement statement = conn.createStatement();
        statement.closeOnCompletion();
        return statement.executeQuery(
            "SELECT schema_name AS 'TABLE_SCHEM', catalog_name AS 'TABLE_CATALOG' FROM information_schema.schemata ORDER BY \"TABLE_CATALOG\", \"TABLE_SCHEM\"");
    }

    @Override
    public ResultSet getSchemas(String catalog, String schemaPattern) throws SQLException {
        StringBuilder sb = new StringBuilder(512);
        sb.append("SELECT schema_name AS 'TABLE_SCHEM', catalog_name AS 'TABLE_CATALOG'");
        sb.append(lineSeparator());
        sb.append("FROM information_schema.schemata");
        sb.append(lineSeparator());
        if (catalog != null || schemaPattern != null) {
            sb.append("WHERE ");
        }

        if (catalog != null) {
            if (catalog.isEmpty()) {
                sb.append("catalog_name IS NULL");
            } else {
                sb.append("catalog_name = ?");
            }
            sb.append(lineSeparator());
        }
        if (schemaPattern != null) {
            if (catalog != null) {
                sb.append("AND ");
            }
            if (schemaPattern.isEmpty()) {
                sb.append("schema_name IS NULL");
            } else {
                sb.append("schema_name LIKE ?");
            }
            sb.append(lineSeparator());
        }
        sb.append("ORDER BY \"TABLE_CATALOG\", \"TABLE_SCHEM\"");
        sb.append(lineSeparator());

        PreparedStatement ps = conn.prepareStatement(sb.toString());
        int paramIndex = 0;
        if (catalog != null && !catalog.isEmpty()) {
            ps.setString(++paramIndex, catalog);
        }
        if (schemaPattern != null && !schemaPattern.isEmpty()) {
            ps.setString(++paramIndex, schemaPattern);
        }
        ps.closeOnCompletion();
        return ps.executeQuery();
    }

    @Override
    public ResultSet getTableTypes() throws SQLException {
        String[] tableTypesArray = new String[] {"BASE TABLE", "LOCAL TEMPORARY", "VIEW"};
        StringBuilder stringBuilder = new StringBuilder(128);
        boolean first = true;
        for (String tableType : tableTypesArray) {
            if (!first) {
                stringBuilder.append("\nUNION ALL\n");
            }
            stringBuilder.append("SELECT '");
            stringBuilder.append(tableType);
            stringBuilder.append("'");
            if (first) {
                stringBuilder.append(" AS 'TABLE_TYPE'");
                first = false;
            }
        }
        stringBuilder.append("\nORDER BY TABLE_TYPE");
        Statement statement = conn.createStatement();
        statement.closeOnCompletion();
        return statement.executeQuery(stringBuilder.toString());
    }

    @Override
    public ResultSet getTables(String catalog, String schemaPattern, String tableNamePattern, String[] types)
        throws SQLException {
        StringBuilder str = new StringBuilder(512);

        str.append("SELECT table_catalog AS 'TABLE_CAT'").append(lineSeparator());
        str.append(", table_schema AS 'TABLE_SCHEM'").append(lineSeparator());
        str.append(", table_name AS 'TABLE_NAME'").append(lineSeparator());
        str.append(", table_type AS 'TABLE_TYPE'").append(lineSeparator());
        str.append(", NULL::VARCHAR AS 'REMARKS'").append(lineSeparator());
        str.append(", NULL::VARCHAR AS 'TYPE_CAT'").append(lineSeparator());
        str.append(", NULL::VARCHAR AS 'TYPE_SCHEM'").append(lineSeparator());
        str.append(", NULL::VARCHAR AS 'TYPE_NAME'").append(lineSeparator());
        str.append(", NULL::VARCHAR AS 'SELF_REFERENCING_COL_NAME'").append(lineSeparator());
        str.append(", NULL::VARCHAR AS 'REF_GENERATION'").append(lineSeparator());
        str.append("FROM information_schema.tables").append(lineSeparator());

        // tableNamePattern - a table name pattern; must match the table name as it is stored in the database
        if (tableNamePattern == null) {
            // non-standard behavior.
            tableNamePattern = "%";
        }
        str.append("WHERE table_name LIKE ?").append(lineSeparator());

        // catalog - a catalog name; must match the catalog name as it is stored in the database;
        // "" retrieves those without a catalog;
        // null means that the catalog name should not be used to narrow the search
        boolean hasCatalogParam = false;
        if (catalog != null) {
            str.append("AND table_catalog ");
            if (catalog.isEmpty()) {
                str.append("IS NULL").append(lineSeparator());
            } else {
                str.append("= ?").append(lineSeparator());
                hasCatalogParam = true;
            }
        }

        // schemaPattern - a schema name pattern; must match the schema name as it is stored in the database;
        // "" retrieves those without a schema;
        // null means that the schema name should not be used to narrow the search
        boolean hasSchemaParam = false;
        if (schemaPattern != null) {
            str.append("AND table_schema ");
            if (schemaPattern.isEmpty()) {
                str.append("IS NULL").append(lineSeparator());
            } else {
                str.append("LIKE ?").append(lineSeparator());
                hasSchemaParam = true;
            }
        }

        if (types != null && types.length > 0) {
            str.append("AND table_type IN (").append(lineSeparator());
            for (int i = 0; i < types.length; i++) {
                if (i > 0) {
                    str.append(',');
                }
                str.append('?');
            }
            str.append(')');
        }

        // ordered by TABLE_TYPE, TABLE_CAT, TABLE_SCHEM and TABLE_NAME.
        str.append("ORDER BY table_type").append(lineSeparator());
        str.append(", table_catalog").append(lineSeparator());
        str.append(", table_schema").append(lineSeparator());
        str.append(", table_name").append(lineSeparator());

        PreparedStatement ps = conn.prepareStatement(str.toString());

        int paramOffset = 1;
        ps.setString(paramOffset++, tableNamePattern);

        if (hasCatalogParam) {
            ps.setString(paramOffset++, catalog);
        }
        if (hasSchemaParam) {
            ps.setString(paramOffset++, schemaPattern);
        }

        if (types != null && types.length > 0) {
            for (int i = 0; i < types.length; i++) {
                ps.setString(paramOffset + i, types[i]);
            }
        }
        ps.closeOnCompletion();
        return ps.executeQuery();
    }

    @Override
    public ResultSet getColumns(String catalogPattern, String schemaPattern, String tableNamePattern,
                                String columnNamePattern) throws SQLException {
        if (catalogPattern == null) {
            catalogPattern = "%";
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
        StringBuilder values_str = new StringBuilder(256);
        values_str.append("VALUES(NULL::STRING, NULL::INTEGER)");
        try (Statement gunky_statement = conn.createStatement();
             // TODO this could get slow with many many columns and we really only need the
             // types :/
             ResultSet rs = gunky_statement.executeQuery(
                 "SELECT DISTINCT data_type FROM information_schema.columns ORDER BY data_type")) {
            while (rs.next()) {
                values_str.append(", ('")
                    .append(rs.getString(1))
                    .append("', ")
                    .append(
                        DuckDBResultSetMetaData.type_to_int(DuckDBResultSetMetaData.TypeNameToType(rs.getString(1))))
                    .append(")");
            }
        }

        PreparedStatement ps = conn.prepareStatement(
            "SELECT table_catalog AS 'TABLE_CAT', table_schema AS 'TABLE_SCHEM', table_name AS 'TABLE_NAME', column_name as 'COLUMN_NAME', type_id AS 'DATA_TYPE', c.data_type AS 'TYPE_NAME', NULL AS 'COLUMN_SIZE', NULL AS 'BUFFER_LENGTH', numeric_precision AS 'DECIMAL_DIGITS', 10 AS 'NUM_PREC_RADIX', CASE WHEN is_nullable = 'YES' THEN 1 else 0 END AS 'NULLABLE', NULL as 'REMARKS', column_default AS 'COLUMN_DEF', NULL AS 'SQL_DATA_TYPE', NULL AS 'SQL_DATETIME_SUB', character_octet_length AS 'CHAR_OCTET_LENGTH', ordinal_position AS 'ORDINAL_POSITION', is_nullable AS 'IS_NULLABLE', NULL AS 'SCOPE_CATALOG', NULL AS 'SCOPE_SCHEMA', NULL AS 'SCOPE_TABLE', NULL AS 'SOURCE_DATA_TYPE', '' AS 'IS_AUTOINCREMENT', '' AS 'IS_GENERATEDCOLUMN'  FROM information_schema.columns c JOIN (" +
            values_str +
            ") t(type_name, type_id) ON c.data_type = t.type_name WHERE table_catalog LIKE ? AND table_schema LIKE ? AND table_name LIKE ? AND column_name LIKE ? ORDER BY \"TABLE_CAT\",\"TABLE_SCHEM\", \"TABLE_NAME\", \"ORDINAL_POSITION\"");
        ps.setString(1, catalogPattern);
        ps.setString(2, schemaPattern);
        ps.setString(3, tableNamePattern);
        ps.setString(4, columnNamePattern);
        ps.closeOnCompletion();
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
        Statement statement = conn.createStatement();
        statement.closeOnCompletion();
        return statement.executeQuery("SELECT NULL WHERE FALSE");
    }

    @Override
    public ResultSet getProcedureColumns(String catalog, String schemaPattern, String procedureNamePattern,
                                         String columnNamePattern) throws SQLException {
        Statement statement = conn.createStatement();
        statement.closeOnCompletion();
        return statement.executeQuery("SELECT NULL WHERE FALSE");
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
        StringBuilder pw = new StringBuilder(512);
        pw.append("WITH constraint_columns AS (").append(lineSeparator());
        pw.append("SELECT").append(lineSeparator());
        pw.append("  database_name AS \"TABLE_CAT\"").append(lineSeparator());
        pw.append(", schema_name AS \"TABLE_SCHEM\"").append(lineSeparator());
        pw.append(", table_name AS \"TABLE_NAME\"").append(lineSeparator());
        pw.append(", unnest(constraint_column_names) AS \"COLUMN_NAME\"").append(lineSeparator());
        pw.append(", CAST(NULL AS VARCHAR) AS \"PK_NAME\"").append(lineSeparator());
        pw.append("FROM duckdb_constraints").append(lineSeparator());
        pw.append("WHERE constraint_type = 'PRIMARY KEY'").append(lineSeparator());
        // catalog param
        if (catalog != null) {
            if (catalog.isEmpty()) {
                pw.append("AND database_name IS NULL").append(lineSeparator());
            } else {
                pw.append("AND database_name = ?").append(lineSeparator());
            }
        }
        // schema param
        if (schema != null) {
            if (schema.isEmpty()) {
                pw.append("AND schema_name IS NULL").append(lineSeparator());
            } else {
                pw.append("AND schema_name = ?").append(lineSeparator());
            }
        }
        // table name param
        pw.append("AND table_name = ?").append(lineSeparator());

        pw.append(")").append(lineSeparator());
        pw.append("SELECT \"TABLE_CAT\"").append(lineSeparator());
        pw.append(", \"TABLE_SCHEM\"").append(lineSeparator());
        pw.append(", \"TABLE_NAME\"").append(lineSeparator());
        pw.append(", \"COLUMN_NAME\"").append(lineSeparator());
        pw.append(", CAST(ROW_NUMBER() OVER ").append(lineSeparator());
        pw.append("(PARTITION BY \"TABLE_CAT\", \"TABLE_SCHEM\", \"TABLE_NAME\") AS INT) AS \"KEY_SEQ\"")
            .append(lineSeparator());
        pw.append(", \"PK_NAME\"").append(lineSeparator());
        pw.append("FROM constraint_columns").append(lineSeparator());
        pw.append("ORDER BY \"TABLE_CAT\", \"TABLE_SCHEM\", \"TABLE_NAME\", \"KEY_SEQ\"").append(lineSeparator());

        int paramIndex = 1;
        PreparedStatement ps = conn.prepareStatement(pw.toString());

        if (catalog != null && !catalog.isEmpty()) {
            ps.setString(paramIndex++, catalog);
        }
        if (schema != null && !schema.isEmpty()) {
            ps.setString(paramIndex++, schema);
        }
        ps.setString(paramIndex++, table);
        ps.closeOnCompletion();
        return ps.executeQuery();
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
                                       String foreignCatalog, String foreignSchema, String foreignTable)
        throws SQLException {
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
        return false;
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
        return false;
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
        return false;
    }

    @Override
    public RowIdLifetime getRowIdLifetime() throws SQLException {
        throw new SQLFeatureNotSupportedException("getRowIdLifetime");
    }

    @Override
    public boolean supportsStoredFunctionsUsingCallSyntax() throws SQLException {
        return false;
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
     * SPECIFIC_NAME String => the name which uniquely identifies this function within its schema. This is a user
     * specified, or DBMS generated, name that may be different then the FUNCTION_NAME for example with overload
     * functions
     */
    @Override
    public ResultSet getFunctions(String catalog, String schemaPattern, String functionNamePattern)
        throws SQLException {
        try (PreparedStatement statement =
                 conn.prepareStatement("SELECT "
                                       + "null as FUNCTION_CAT, "
                                       + "function_name as FUNCTION_NAME, "
                                       + "schema_name as FUNCTION_SCHEM, "
                                       + "description as REMARKS,"
                                       + "CASE function_type "
                                       + "WHEN 'table' THEN " + functionReturnsTable + " "
                                       + "WHEN 'table_macro' THEN " + functionReturnsTable + " "
                                       + "ELSE " + functionNoTable + " "
                                       + "END as FUNCTION_TYPE "
                                       + "FROM duckdb_functions() "
                                       + "WHERE function_name like ? and "
                                       + "schema_name like ?")) {
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
        Statement statement = conn.createStatement();
        statement.closeOnCompletion();
        return statement.executeQuery("SELECT NULL WHERE FALSE");
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
