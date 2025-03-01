package com.game.db.util;

import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import javax.sql.DataSource;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

/**
 * JDBC工具类，提供数据库连接和操作功能
 */
public class JdbcUtil {
    private static final Map<String, DataSource> DATA_SOURCE_MAP = new ConcurrentHashMap<>();
    private static String defaultDataSourceName = "default";
    private static boolean autoAlterColumn = false; // 是否自动修改字段长度
    
    /**
     * 初始化默认数据源
     */
    public static void initDefaultDataSource(String url, String username, String password) {
        initDataSource(defaultDataSourceName, url, username, password);
    }
    
    /**
     * 初始化指定名称的数据源
     */
    public static void initDataSource(String name, String url, String username, String password) {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(url);
        config.setUsername(username);
        config.setPassword(password);
        config.setMaximumPoolSize(10);
        config.setMinimumIdle(5);
        config.setIdleTimeout(30000);
        config.setConnectionTimeout(30000);
        config.setAutoCommit(true);
        
        HikariDataSource dataSource = new HikariDataSource(config);
        DATA_SOURCE_MAP.put(name, dataSource);
    }
    
    /**
     * 获取默认数据源的连接
     */
    public static Connection getConnection() throws SQLException {
        return getConnection(defaultDataSourceName);
    }
    
    /**
     * 获取指定数据源的连接
     */
    public static Connection getConnection(String dataSourceName) throws SQLException {
        DataSource dataSource = DATA_SOURCE_MAP.get(dataSourceName);
        if (dataSource == null) {
            throw new SQLException("DataSource not found: " + dataSourceName);
        }
        return dataSource.getConnection();
    }
    
    /**
     * 执行查询
     */
    public static List<Map<String, Object>> executeQuery(String sql, Object... params) throws SQLException {
        try (Connection conn = getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            
            for (int i = 0; i < params.length; i++) {
                stmt.setObject(i + 1, params[i]);
            }
            
            try (ResultSet rs = stmt.executeQuery()) {
                List<Map<String, Object>> result = new ArrayList<>();
                ResultSetMetaData metaData = rs.getMetaData();
                int columnCount = metaData.getColumnCount();
                
                while (rs.next()) {
                    Map<String, Object> row = new HashMap<>();
                    for (int i = 1; i <= columnCount; i++) {
                        String columnName = metaData.getColumnLabel(i);
                        Object value = rs.getObject(i);
                        row.put(columnName, value);
                    }
                    result.add(row);
                }
                
                return result;
            }
        }
    }
    
    /**
     * 执行更新操作（插入、更新、删除）
     */
    public static int executeUpdate(String sql, Object... params) throws SQLException {
        try (Connection conn = getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            
            for (int i = 0; i < params.length; i++) {
                stmt.setObject(i + 1, params[i]);
            }
            
            return stmt.executeUpdate();
        }
    }
    
    /**
     * 执行批量更新
     */
    public static int[] executeBatch(String sql, List<Object[]> paramsList) throws SQLException {
        try (Connection conn = getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            
            for (Object[] params : paramsList) {
                for (int i = 0; i < params.length; i++) {
                    stmt.setObject(i + 1, params[i]);
                }
                stmt.addBatch();
            }
            
            return stmt.executeBatch();
        }
    }
    
    /**
     * 关闭资源
     */
    public static void close(Connection conn, Statement stmt, ResultSet rs) {
        if (rs != null) {
            try {
                rs.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        
        if (stmt != null) {
            try {
                stmt.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        
        if (conn != null) {
            try {
                conn.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }
    
    /**
     * 关闭所有数据源
     */
    public static void closeAllDataSources() {
        for (DataSource dataSource : DATA_SOURCE_MAP.values()) {
            if (dataSource instanceof AutoCloseable) {
                try {
                    ((AutoCloseable) dataSource).close();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
        DATA_SOURCE_MAP.clear();
    }
    
    /**
     * 设置是否自动修改字段长度
     * @param autoAlter true表示自动修改，false表示抛出异常
     */
    public static void setAutoAlterColumn(boolean autoAlter) {
        autoAlterColumn = autoAlter;
    }
    
    /**
     * 检查字符串字段长度
     * @param value 字段值
     * @param tableName 表名
     * @param columnName 列名
     * @throws SQLException 当字段长度超出限制且不允许自动修改时抛出异常
     */
    public static void checkFieldLength(String value, String tableName, String columnName) throws SQLException {
        if (value == null) {
            return;
        }
        
        try (Connection conn = getConnection()) {
            // 获取字段当前长度限制
            int currentMaxLength = getColumnMaxLength(conn, tableName, columnName);
            
            // 如果字段值长度超出限制
            if (value.length() > currentMaxLength) {
                if (autoAlterColumn) {
                    // 自动修改字段长度
                    int newLength = Math.max(value.length(), currentMaxLength * 2); // 新长度为当前值长度和当前限制的2倍中的较大值
                    alterColumnLength(conn, tableName, columnName, newLength);
                } else {
                    // 抛出异常
                    throw new SQLException("字段 " + columnName + " 的值长度(" + value.length() + 
                                         ")超出最大限制(" + currentMaxLength + ")");
                }
            }
        }
    }
    
    /**
     * 获取列的最大长度
     */
    private static int getColumnMaxLength(Connection conn, String tableName, String columnName) throws SQLException {
        String sql = "";
        String dbType = conn.getMetaData().getDatabaseProductName().toLowerCase();
        
        if (dbType.contains("mysql")) {
            sql = "SELECT CHARACTER_MAXIMUM_LENGTH FROM INFORMATION_SCHEMA.COLUMNS " +
                  "WHERE TABLE_NAME = ? AND COLUMN_NAME = ? AND TABLE_SCHEMA = DATABASE()";
        } else if (dbType.contains("oracle")) {
            sql = "SELECT DATA_LENGTH FROM ALL_TAB_COLUMNS WHERE TABLE_NAME = ? AND COLUMN_NAME = ?";
        } else if (dbType.contains("postgresql")) {
            sql = "SELECT character_maximum_length FROM information_schema.columns " +
                  "WHERE table_name = ? AND column_name = ?";
        } else if (dbType.contains("sqlserver") || dbType.contains("microsoft")) {
            sql = "SELECT CHARACTER_MAXIMUM_LENGTH FROM INFORMATION_SCHEMA.COLUMNS " +
                  "WHERE TABLE_NAME = ? AND COLUMN_NAME = ?";
        } else {
            throw new SQLException("不支持的数据库类型: " + dbType);
        }
        
        try (PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setString(1, tableName);
            stmt.setString(2, columnName);
            
            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    int length = rs.getInt(1);
                    return length > 0 ? length : Integer.MAX_VALUE; // 对于不限长度的类型，返回最大整数值
                } else {
                    throw new SQLException("找不到表 " + tableName + " 的列 " + columnName);
                }
            }
        }
    }
    
    /**
     * 修改列的长度
     */
    private static void alterColumnLength(Connection conn, String tableName, String columnName, int newLength) throws SQLException {
        String sql = "";
        String dbType = conn.getMetaData().getDatabaseProductName().toLowerCase();
        
        // 首先获取列的数据类型
        String dataType = getColumnDataType(conn, tableName, columnName);
        
        if (dbType.contains("mysql")) {
            sql = "ALTER TABLE " + tableName + " MODIFY COLUMN " + columnName + " " + dataType + "(" + newLength + ")";
        } else if (dbType.contains("oracle")) {
            sql = "ALTER TABLE " + tableName + " MODIFY " + columnName + " " + dataType + "(" + newLength + ")";
        } else if (dbType.contains("postgresql")) {
            sql = "ALTER TABLE " + tableName + " ALTER COLUMN " + columnName + " TYPE " + dataType + "(" + newLength + ")";
        } else if (dbType.contains("sqlserver") || dbType.contains("microsoft")) {
            sql = "ALTER TABLE " + tableName + " ALTER COLUMN " + columnName + " " + dataType + "(" + newLength + ")";
        } else {
            throw new SQLException("不支持的数据库类型: " + dbType);
        }
        
        try (Statement stmt = conn.createStatement()) {
            stmt.execute(sql);
        }
    }
    
    /**
     * 获取列的数据类型
     */
    private static String getColumnDataType(Connection conn, String tableName, String columnName) throws SQLException {
        String sql = "";
        String dbType = conn.getMetaData().getDatabaseProductName().toLowerCase();
        
        if (dbType.contains("mysql")) {
            sql = "SELECT DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS " +
                  "WHERE TABLE_NAME = ? AND COLUMN_NAME = ? AND TABLE_SCHEMA = DATABASE()";
        } else if (dbType.contains("oracle")) {
            sql = "SELECT DATA_TYPE FROM ALL_TAB_COLUMNS WHERE TABLE_NAME = ? AND COLUMN_NAME = ?";
        } else if (dbType.contains("postgresql")) {
            sql = "SELECT data_type FROM information_schema.columns " +
                  "WHERE table_name = ? AND column_name = ?";
        } else if (dbType.contains("sqlserver") || dbType.contains("microsoft")) {
            sql = "SELECT DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS " +
                  "WHERE TABLE_NAME = ? AND COLUMN_NAME = ?";
        } else {
            throw new SQLException("不支持的数据库类型: " + dbType);
        }
        
        try (PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setString(1, tableName);
            stmt.setString(2, columnName);
            
            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    return rs.getString(1);
                } else {
                    throw new SQLException("找不到表 " + tableName + " 的列 " + columnName);
                }
            }
        }
    }
    
    /**
     * 设置PreparedStatement参数，并检查字符串类型的字段长度
     * @param stmt PreparedStatement对象
     * @param index 参数索引
     * @param value 参数值
     * @param tableName 表名
     * @param columnName 列名
     * @throws SQLException SQL异常
     */
    public static void setParameter(PreparedStatement stmt, int index, Object value, 
                                   String tableName, String columnName) throws SQLException {
        if (value instanceof String) {
            checkFieldLength((String)value, tableName, columnName);
        }
        stmt.setObject(index, value);
    }
    
    /**
     * 关闭资源
     */
    public static void close(Connection conn, Statement stmt, ResultSet rs) {
        if (rs != null) {
            try {
                rs.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        
        if (stmt != null) {
            try {
                stmt.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        
        if (conn != null) {
            try {
                conn.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }
    
    /**
     * 关闭所有数据源
     */
    public static void closeAllDataSources() {
        for (DataSource dataSource : DATA_SOURCE_MAP.values()) {
            if (dataSource instanceof AutoCloseable) {
                try {
                    ((AutoCloseable) dataSource).close();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
        DATA_SOURCE_MAP.clear();
    }
}