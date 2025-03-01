package com.game.db.loader;

import com.game.db.EntityManager;
import com.game.db.metadata.EntityMetadata;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

/**
 * 游戏数据加载器，负责加载和缓存游戏数据
 */
public class GameDataLoader {
    private static final Logger LOGGER = Logger.getLogger(GameDataLoader.class.getName());
    
    // 数据缓存，按实体类型分组
    private final Map<Class<?>, Map<Object, Object>> dataCache = new ConcurrentHashMap<>();
    
    /**
     * 加载指定类型的所有实体数据
     */
    public <T, K> Map<K, T> loadAll(Class<T> entityClass, Function<T, K> keyExtractor) {
        try {
            List<T> entities = EntityManager.loadAll(entityClass);
            Map<K, T> entityMap = entities.stream()
                    .collect(Collectors.toMap(keyExtractor, Function.identity()));
            
            // 缓存数据
            @SuppressWarnings("unchecked")
            Map<Object, Object> cache = (Map<Object, Object>) (Map<?, ?>) entityMap;
            dataCache.put(entityClass, cache);
            
            LOGGER.info("Loaded " + entities.size() + " entities of type: " + entityClass.getName());
            return entityMap;
        } catch (SQLException e) {
            LOGGER.log(Level.SEVERE, "Failed to load entities: " + entityClass.getName(), e);
            return new HashMap<>();
        }
    }
    
    /**
     * 获取指定类型的所有实体数据（从缓存）
     */
    @SuppressWarnings("unchecked")
    public <T, K> Map<K, T> getAll(Class<T> entityClass) {
        return (Map<K, T>) dataCache.getOrDefault(entityClass, new HashMap<>());
    }
    
    /**
     * 根据ID获取实体（从缓存）
     */
    @SuppressWarnings("unchecked")
    public <T, K> T get(Class<T> entityClass, K id) {
        Map<Object, Object> cache = dataCache.get(entityClass);
        if (cache == null) {
            return null;
        }
        return (T) cache.get(id);
    }
    
    /**
     * 重新加载指定类型的所有实体数据
     */
    public <T, K> Map<K, T> reload(Class<T> entityClass, Function<T, K> keyExtractor) {
        dataCache.remove(entityClass);
        return loadAll(entityClass, keyExtractor);
    }
    
    /**
     * 重新加载所有数据
     */
    public void reloadAll() {
        dataCache.clear();
        LOGGER.info("All data cache cleared");
    }
    
    /**
     * 添加或更新缓存中的实体
     */
    @SuppressWarnings("unchecked")
    public <T, K> void updateCache(Class<T> entityClass, T entity, Function<T, K> keyExtractor) {
        K key = keyExtractor.apply(entity);
        Map<Object, Object> cache = dataCache.computeIfAbsent(entityClass, k -> new ConcurrentHashMap<>());
        cache.put(key, entity);
    }
    
    /**
     * 从缓存中移除实体
     */
    public <T, K> void removeFromCache(Class<T> entityClass, K key) {
        Map<Object, Object> cache = dataCache.get(entityClass);
        if (cache != null) {
            cache.remove(key);
        }
    }
}