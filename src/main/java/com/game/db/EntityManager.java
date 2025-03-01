package com.game.db;

import com.game.db.metadata.EntityMetadata;
import com.game.db.util.JdbcUtil;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 实体管理器，负责实体的CRUD操作
 */
public class EntityManager {
    private static final Map<Class<?>, EntityMetadata<?>> METADATA_CACHE = new ConcurrentHashMap<>();
    
    /**
     * 获取实体元数据
     */
    @SuppressWarnings("unchecked")
    public static <T> EntityMetadata<T> getMetadata(Class<T> entityClass) {
        return (EntityMetadata<T>) METADATA_CACHE.computeIfAbsent(entityClass, EntityMetadata::new);
    }
    
    /**
     * 根据ID加载实体
     */
    public static <T> T load(Class<T> entityClass, Object id) throws SQLException {
        EntityMetadata<T> metadata = getMetadata(entityClass);
        String sql = metadata.generateSelectByIdSql();
        
        try (Connection conn = JdbcUtil.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            
            stmt.setObject(1, id);
            
            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    return mapResultSetToEntity(rs, metadata);
                }
                return null;
            }
        }
    }
    
    /**
     * 加载所有实体
     */
    public static <T> List<T> loadAll(Class<T> entityClass) throws SQLException {
        EntityMetadata<T> metadata = getMetadata(entityClass);
        String sql = metadata.generateSelectSql();
        
        try (Connection conn = JdbcUtil.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql);
             ResultSet rs = stmt.executeQuery()) {
            
            List<T> result = new ArrayList<>();
            while (rs.next()) {
                result.add(mapResultSetToEntity(rs, metadata));
            }
            return result;
        }
    }
    
    /**
     * 保存实体（插入或更新）
     */
    public static <T> void save(T entity) throws SQLException {
        @SuppressWarnings("unchecked")
        Class<T> entityClass = (Class<T>) entity.getClass();
        EntityMetadata<T> metadata = getMetadata(entityClass);
        
        Object id = metadata.getIdValue(entity);
        if (id == null || (id instanceof Number && ((Number) id).longValue() == 0)) {
            insert(entity, metadata);
        } else {
            update(entity, metadata);
        }
    }
    
    /**
     * 插入实体
     */
    private static <T> void insert(T entity, EntityMetadata<T> metadata) throws SQLException {
        String sql = metadata.generateInsertSql();
        Object[] values = metadata.getInsertValues(entity);
        
        JdbcUtil.executeUpdate(sql, values);
    }
    
    /**
     * 更新实体
     */
    private static <T> void update(T entity, EntityMetadata<T> metadata) throws SQLException {
        String sql = metadata.generateUpdateSql();
        Object[] values = metadata.getUpdateValues(entity);
        
        JdbcUtil.executeUpdate(sql, values);
    }
    
    /**
     * 删除实体
     */
    public static <T> void delete(T entity) throws SQLException {
        @SuppressWarnings("unchecked")
        Class<T> entityClass = (Class<T>) entity.getClass();
        EntityMetadata<T> metadata = getMetadata(entityClass);
        
        String sql = metadata.generateDeleteSql();
        Object id = metadata.getIdValue(entity);
        
        JdbcUtil.executeUpdate(sql, id);
    }
    
    /**
     * 根据ID删除实体
     */
    public static <T> void deleteById(Class<T> entityClass, Object id) throws SQLException {
        EntityMetadata<T> metadata = getMetadata(entityClass);
        String sql = metadata.generateDeleteSql();
        
        JdbcUtil.executeUpdate(sql, id);
    }
    
    /**
     * 将ResultSet映射为实体对象
     */
    private static <T> T mapResultSetToEntity(ResultSet rs, EntityMetadata<T> metadata) throws SQLException {
        try {
            T entity = metadata.getEntityClass().newInstance();
            
            metadata.getColumns().forEach(column -> {
                try {
                    String columnName = column.getColumnName();
                    private static void setEntityField(Object entity, Field field, ResultSet rs, String columnName) throws Exception {
                        field.setAccessible(true);
                        
                        if (field.isAnnotationPresent(JsonField.class)) {
                            // 处理 JSON 字段
                            String jsonValue = rs.getString(columnName);
                            Object value = JsonConverter.fromJson(jsonValue, field.getType());
                            field.set(entity, value);
                        } else {
                            // 处理普通字段
                            Object value = rs.getObject(columnName);
                            field.set(entity, value);
                        }
                    }
                    
                    private static void setPreparedStatementParameter(PreparedStatement ps, int index, Object value, Field field) throws Exception {
                        if (field.isAnnotationPresent(JsonField.class)) {
                            // 处理 JSON 字段
                            String jsonValue = JsonConverter.toJson(value);
                            ps.setString(index, jsonValue);
                        } else {
                            // 处理普通字段
                            ps.setObject(index, value);
                        }
                    }
                } catch (SQLException e) {
                    throw new RuntimeException("Failed to get column value: " + column.getColumnName(), e);
                }
            });
            
            return entity;
        } catch (InstantiationException | IllegalAccessException e) {
            throw new RuntimeException("Failed to create entity instance", e);
        }
    }
}