package com.game.db.metadata;

import com.game.db.annotation.*;
import java.lang.reflect.Field;
import java.util.*;

/**
 * 实体元数据，存储实体类与数据库表的映射信息
 */
public class EntityMetadata<T> {
    private final Class<T> entityClass;
    private final String tableName;
    private final String entityName;
    private final Map<String, ColumnMetadata> columnMap = new HashMap<>();
    private final List<ColumnMetadata> columns = new ArrayList<>();
    private ColumnMetadata idColumn;

    public EntityMetadata(Class<T> entityClass) {
        this.entityClass = entityClass;
        
        // 获取实体注解
        Entity entityAnnotation = entityClass.getAnnotation(Entity.class);
        if (entityAnnotation == null) {
            throw new IllegalArgumentException("Class " + entityClass.getName() + " is not annotated with @Entity");
        }
        
        // 获取表注解
        Table tableAnnotation = entityClass.getAnnotation(Table.class);
        if (tableAnnotation == null) {
            throw new IllegalArgumentException("Class " + entityClass.getName() + " is not annotated with @Table");
        }
        
        this.tableName = tableAnnotation.name();
        this.entityName = entityAnnotation.name().isEmpty() ? entityClass.getSimpleName() : entityAnnotation.name();
        
        // 解析字段
        parseFields();
    }
    
    private void parseFields() {
        for (Field field : getAllFields(entityClass)) {
            // 忽略被@Transient标记的字段
            if (field.isAnnotationPresent(Transient.class)) {
                continue;
            }
            
            field.setAccessible(true);
            
            // 获取列注解
            Column columnAnnotation = field.getAnnotation(Column.class);
            String columnName = (columnAnnotation != null && !columnAnnotation.name().isEmpty()) 
                ? columnAnnotation.name() : field.getName();
            
            ColumnMetadata columnMetadata = new ColumnMetadata(field, columnName);
            
            // 检查是否为主键
            if (field.isAnnotationPresent(Id.class)) {
                Id idAnnotation = field.getAnnotation(Id.class);
                columnMetadata.setPrimaryKey(true);
                columnMetadata.setAutoIncrement(idAnnotation.autoIncrement());
                this.idColumn = columnMetadata;
            }
            
            columns.add(columnMetadata);
            columnMap.put(columnName, columnMetadata);
        }
        
        if (idColumn == null) {
            throw new IllegalArgumentException("Entity " + entityClass.getName() + " does not have an @Id field");
        }
    }
    
    private List<Field> getAllFields(Class<?> clazz) {
        List<Field> fields = new ArrayList<>();
        Class<?> currentClass = clazz;
        
        while (currentClass != null && currentClass != Object.class) {
            fields.addAll(Arrays.asList(currentClass.getDeclaredFields()));
            currentClass = currentClass.getSuperclass();
        }
        
        return fields;
    }
    
    public Class<T> getEntityClass() {
        return entityClass;
    }
    
    public String getTableName() {
        return tableName;
    }
    
    public String getEntityName() {
        return entityName;
    }
    
    public List<ColumnMetadata> getColumns() {
        return Collections.unmodifiableList(columns);
    }
    
    public ColumnMetadata getColumn(String columnName) {
        return columnMap.get(columnName);
    }
    
    public ColumnMetadata getIdColumn() {
        return idColumn;
    }
    
    /**
     * 生成查询SQL
     */
    public String generateSelectSql() {
        StringBuilder sql = new StringBuilder("SELECT ");
        
        for (int i = 0; i < columns.size(); i++) {
            if (i > 0) {
                sql.append(", ");
            }
            sql.append(columns.get(i).getColumnName());
        }
        
        sql.append(" FROM ").append(tableName);
        return sql.toString();
    }
    
    /**
     * 生成根据ID查询的SQL
     */
    public String generateSelectByIdSql() {
        return generateSelectSql() + " WHERE " + idColumn.getColumnName() + " = ?";
    }
    
    /**
     * 生成插入SQL
     */
    public String generateInsertSql() {
        StringBuilder sql = new StringBuilder("INSERT INTO ").append(tableName).append(" (");
        StringBuilder values = new StringBuilder(") VALUES (");
        
        boolean first = true;
        for (ColumnMetadata column : columns) {
            // 跳过自增主键
            if (column.isPrimaryKey() && column.isAutoIncrement()) {
                continue;
            }
            
            if (!first) {
                sql.append(", ");
                values.append(", ");
            } else {
                first = false;
            }
            
            sql.append(column.getColumnName());
            values.append("?");
        }
        
        sql.append(values).append(")");
        return sql.toString();
    }
    
    /**
     * 生成更新SQL
     */
    public String generateUpdateSql() {
        StringBuilder sql = new StringBuilder("UPDATE ").append(tableName).append(" SET ");
        
        boolean first = true;
        for (ColumnMetadata column : columns) {
            // 跳过主键
            if (column.isPrimaryKey()) {
                continue;
            }
            
            if (!first) {
                sql.append(", ");
            } else {
                first = false;
            }
            
            sql.append(column.getColumnName()).append(" = ?");
        }
        
        sql.append(" WHERE ").append(idColumn.getColumnName()).append(" = ?");
        return sql.toString();
    }
    
    /**
     * 生成删除SQL
     */
    public String generateDeleteSql() {
        return "DELETE FROM " + tableName + " WHERE " + idColumn.getColumnName() + " = ?";
    }
    
    /**
     * 获取实体对象的ID值
     */
    public Object getIdValue(T entity) {
        return idColumn.getValue(entity);
    }
    
    /**
     * 获取实体对象的所有列值（用于插入）
     */
    public Object[] getInsertValues(T entity) {
        List<Object> values = new ArrayList<>();
        
        for (ColumnMetadata column : columns) {
            // 跳过自增主键
            if (column.isPrimaryKey() && column.isAutoIncrement()) {
                continue;
            }
            
            values.add(column.getValue(entity));
        }
        
        return values.toArray();
    }
    
    /**
     * 获取实体对象的所有列值（用于更新）
     */
    public Object[] getUpdateValues(T entity) {
        List<Object> values = new ArrayList<>();
        
        // 添加非主键列的值
        for (ColumnMetadata column : columns) {
            if (!column.isPrimaryKey()) {
                values.add(column.getValue(entity));
            }
        }
        
        // 添加WHERE条件的ID值
        values.add(getIdValue(entity));
        
        return values.toArray();
    }
}