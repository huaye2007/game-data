package com.game.db;

import com.game.db.async.AsyncSaveManager;
import com.game.db.loader.GameDataLoader;
import com.game.db.util.JdbcUtil;

import java.util.Map;
import java.util.function.Function;
import java.util.logging.Logger;

/**
 * 游戏数据服务，整合数据加载和异步保存功能
 */
public class GameDataService {
    private static final Logger LOGGER = Logger.getLogger(GameDataService.class.getName());
    
    private final GameDataLoader dataLoader;
    private final AsyncSaveManager saveManager;
    
    /**
     * 创建游戏数据服务
     * 
     * @param saveThreadCount 保存线程数
     * @param saveInterval 保存间隔（毫秒）
     */
    public GameDataService(int saveThreadCount, long saveInterval) {
        this.dataLoader = new GameDataLoader();
        this.saveManager = new AsyncSaveManager(saveThreadCount, saveInterval);
    }
    
    /**
     * 初始化数据库连接
     */
    public void initDatabase(String url, String username, String password) {
        JdbcUtil.initDefaultDataSource(url, username, password);
        LOGGER.info("Database initialized: " + url);
    }
    
    /**
     * 启动服务
     */
    public void start() {
        saveManager.start();
        LOGGER.info("GameDataService started");
    }
    
    /**
     * 停止服务
     */
    public void stop() {
        saveManager.stop();
        JdbcUtil.closeAllDataSources();
        LOGGER.info("GameDataService stopped");
    }
    
    /**
     * 加载指定类型的所有实体数据
     */
    public <T, K> Map<K, T> loadAll(Class<T> entityClass, Function<T, K> keyExtractor) {
        return dataLoader.loadAll(entityClass, keyExtractor);
    }
    
    /**
     * 获取指定类型的所有实体数据（从缓存）
     */
    public <T, K> Map<K, T> getAll(Class<T> entityClass) {
        return dataLoader.getAll(entityClass);
    }
    
    /**
     * 根据ID获取实体（从缓存）
     */
    public <T, K> T get(Class<T> entityClass, K id) {
        return dataLoader.get(entityClass, id);
    }
    
    /**
     * 保存实体（异步）
     */
    public <T> void save(T entity) {
        saveManager.submit(entity);
    }
    
    /**
     * 立即保存指定类型的所有实体
     */
    public void flush(Class<?> entityClass) {
        saveManager.flushEntityType(entityClass);
    }
    
    /**
     * 立即保存所有实体
     */
    public void flushAll() {
        saveManager.flushAll();
    }
    
    /**
     * 重新加载指定类型的所有实体数据
     */
    public <T, K> Map<K, T> reload(Class<T> entityClass, Function<T, K> keyExtractor) {
        return dataLoader.reload(entityClass, keyExtractor);
    }
    
    /**
     * 重新加载所有数据
     */
    public void reloadAll() {
        dataLoader.reloadAll();
    }
    
    /**
     * 获取数据加载器
     */
    public GameDataLoader getDataLoader() {
        return dataLoader;
    }
    
    /**
     * 获取异步保存管理器
     */
    public AsyncSaveManager getSaveManager() {
        return saveManager;
    }
}