package com.game.db.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * 标记字段为数据库列
 */
@Target(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
public @interface Column {
    /**
     * 列名，默认使用字段名
     */
    String name() default "";
    
    /**
     * 是否允许为空
     */
    boolean nullable() default true;
    
    /**
     * 列描述
     */
    String comment() default "";
    
    /**
     * 列长度，适用于字符串类型
     */
    int length() default 255;
}