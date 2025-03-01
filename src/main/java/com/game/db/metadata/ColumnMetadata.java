package com.game.db.metadata;

import java.lang.reflect.Field;
import net.bytebuddy.ByteBuddy;
import net.bytebuddy.description.modifier.Visibility;
import net.bytebuddy.dynamic.DynamicType;
import net.bytebuddy.implementation.FieldAccessor;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 列元数据，存储实体字段与数据库列的映射信息
 */
public class ColumnMetadata {
    private final Field field;
    private final String columnName;
    private boolean primaryKey;
    private boolean autoIncrement;
    private FieldAccessor accessor;
    
    // 用于缓存生成的访问器类
    private static final ConcurrentHashMap<Field, FieldAccessor> ACCESSOR_CACHE = new ConcurrentHashMap<>();
    
    public ColumnMetadata(Field field, String columnName) {
        this.field = field;
        this.columnName = columnName;
        this.accessor = createOrGetAccessor(field);
    }
    
    /**
     * 从实体对象中获取字段值
     */
    public Object getValue(Object entity) {
        try {
            // 优先使用生成的访问器
            if (accessor != null) {
                return accessor.get(entity);
            }
            // 降级到反射
            return field.get(entity);
        } catch (Exception e) {
            throw new RuntimeException("Failed to get value from field: " + field.getName(), e);
        }
    }
    
    /**
     * 设置实体对象的字段值
     */
    public void setValue(Object entity, Object value) {
        try {
            // 优先使用生成的访问器
            if (accessor != null) {
                accessor.set(entity, value);
                return;
            }
            // 降级到反射
            field.set(entity, value);
        } catch (Exception e) {
            throw new RuntimeException("Failed to set value to field: " + field.getName(), e);
        }
    }
    
    /**
     * 创建或获取字段访问器
     */
    private FieldAccessor createOrGetAccessor(Field field) {
        return ACCESSOR_CACHE.computeIfAbsent(field, f -> {
            try {
                // 确保字段可访问
                field.setAccessible(true);
                
                // 生成访问器类
                String className = "FieldAccessor" + field.getDeclaringClass().getSimpleName() + 
                                  field.getName() + System.nanoTime();
                
                DynamicType.Builder<?> builder = new ByteBuddy()
                    .subclass(FieldAccessor.class)
                    .name("com.game.db.metadata.generated." + className)
                    .defineMethod("get", Object.class, Visibility.PUBLIC)
                    .withParameter(Object.class)
                    .intercept(FieldAccessor.ofField(field.getName()).getter())
                    .defineMethod("set", void.class, Visibility.PUBLIC)
                    .withParameters(Object.class, Object.class)
                    .intercept(FieldAccessor.ofField(field.getName()).setter());
                
                Class<?> accessorClass = builder.make()
                    .load(ColumnMetadata.class.getClassLoader())
                    .getLoaded();
                
                return (FieldAccessor) accessorClass.getDeclaredConstructor().newInstance();
            } catch (Exception e) {
                // 如果生成失败，返回null，将降级使用反射
                return null;
            }
        });
    }
    
    /**
     * 字段访问器接口
     */
    public interface FieldAccessor {
        Object get(Object target);
        void set(Object target, Object value);
    }
}