package com.game.db.async;

import com.game.db.EntityManager;

import java.sql.SQLException;
import java.util.Map;
import java.util.concurrent.*;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * 异步保存管理器，负责异步保存实体数据
 */
public class AsyncSaveManager {
    private static final Logger LOGGER = Logger.getLogger(AsyncSaveManager.class.getName());
    
    /**
     * 数据库操作类型
     */
    public enum OperationType {
        INSERT,
        UPDATE,
        DELETE
    }
    
    /**
     * 实体操作包装类，包含实体对象和操作类型
     */
    private static class EntityOperation {
        private final Object entity;
        private final OperationType operationType;
        
        public EntityOperation(Object entity, OperationType operationType) {
            this.entity = entity;
            this.operationType = operationType;
        }
        
        public Object getEntity() {
            return entity;
        }
        
        public OperationType getOperationType() {
            return operationType;
        }
    }
    
    // 双缓冲保存Map
    private final Map<Class<?>, Map<Object, EntityOperation>> saveMapsA = new ConcurrentHashMap<>();
    private final Map<Class<?>, Map<Object, EntityOperation>> saveMapsB = new ConcurrentHashMap<>();
    
    // 当前活跃的Map标记
    private volatile boolean usingMapA = true;
    
    // 用于同步Map切换的锁
    private final Object switchLock = new Object();
    
    // 保存线程池
    private final ScheduledExecutorService executor;
    
    // 保存间隔（毫秒）
    private final long saveInterval;
    
    // 切换Map后的等待时间（毫秒）
    private final long switchDelayMs = 50;
    
    // 是否正在运行
    private volatile boolean running = false;
    
    /**
     * 创建异步保存管理器
     * 
     * @param threadCount 保存线程数
     * @param saveInterval 保存间隔（毫秒）
     */
    public AsyncSaveManager(int threadCount, long saveInterval) {
        // 只使用一个线程进行批量保存
        this.executor = Executors.newScheduledThreadPool(1);
        this.saveInterval = saveInterval;
    }
    
    /**
     * 获取当前活跃的保存Map
     */
    private Map<Class<?>, Map<Object, EntityOperation>> getActiveSaveMaps() {
        return usingMapA ? saveMapsA : saveMapsB;
    }

    /**
     * 获取当前非活跃的保存Map
     */
    private Map<Class<?>, Map<Object, EntityOperation>> getInactiveSaveMaps() {
        return usingMapA ? saveMapsB : saveMapsA;
    }
    
    /**
     * 获取实体的键，默认使用实体本身
     * 可以根据实际情况重写此方法，例如使用实体ID作为键
     */
    protected Object getEntityKey(Object entity) {
        return entity;
    }
    
    /**
     * 启动异步保存
     */
    public void start() {
        if (running) {
            return;
        }
        
        running = true;
        executor.scheduleWithFixedDelay(this::processSaveMaps, saveInterval, saveInterval, TimeUnit.MILLISECONDS);
        LOGGER.info("AsyncSaveManager started with interval: " + saveInterval + "ms");
    }
    
    /**
     * 停止异步保存
     */
    public void stop() {
        if (!running) {
            return;
        }
        
        running = false;
        executor.shutdown();
        try {
            if (!executor.awaitTermination(30, TimeUnit.SECONDS)) {
                executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            executor.shutdownNow();
            Thread.currentThread().interrupt();
        }
        
        // 保存所有剩余数据
        processSaveMaps();
        LOGGER.info("AsyncSaveManager stopped");
    }
    
    /**
     * 提交实体进行异步插入
     */
    public <T> void insert(T entity) {
        if (entity == null) {
            return;
        }
        
        @SuppressWarnings("unchecked")
        Class<T> entityClass = (Class<T>) entity.getClass();
        Object key = getEntityKey(entity);
        
        synchronized (switchLock) {
            Map<Class<?>, Map<Object, EntityOperation>> activeMaps = getActiveSaveMaps();
            Map<Object, EntityOperation> entityMap = activeMaps.computeIfAbsent(entityClass, k -> new ConcurrentHashMap<>());
            
            EntityOperation existingOp = entityMap.get(key);
            
            if (existingOp != null) {
                // 如果已存在DELETE操作，需要先删除再插入
                if (existingOp.getOperationType() == OperationType.DELETE) {
                    entityMap.put(key, new EntityOperation(entity, OperationType.INSERT));
                } else {
                    // 如果已存在其他操作，说明保存数据逻辑出问题了
                    LOGGER.warning("尝试插入一个已经在待保存队列中的实体: " + entityClass.getName());
                }
            } else {
                // 不存在则直接添加INSERT操作
                entityMap.put(key, new EntityOperation(entity, OperationType.INSERT));
            }
        }
    }
    
    /**
     * 提交实体进行异步更新
     */
    public <T> void update(T entity) {
        if (entity == null) {
            return;
        }
        
        @SuppressWarnings("unchecked")
        Class<T> entityClass = (Class<T>) entity.getClass();
        Object key = getEntityKey(entity);
        
        synchronized (switchLock) {
            Map<Class<?>, Map<Object, EntityOperation>> activeMaps = getActiveSaveMaps();
            Map<Object, EntityOperation> entityMap = activeMaps.computeIfAbsent(entityClass, k -> new ConcurrentHashMap<>());
            
            EntityOperation existingOp = entityMap.get(key);
            
            if (existingOp != null) {
                // 如果已存在INSERT操作，则不需要做任何事情
                if (existingOp.getOperationType() == OperationType.INSERT) {
                    return;
                }
                // 其他情况更新为UPDATE操作
                entityMap.put(key, new EntityOperation(entity, OperationType.UPDATE));
            } else {
                // 不存在则添加UPDATE操作
                entityMap.put(key, new EntityOperation(entity, OperationType.UPDATE));
            }
        }
    }
    
    /**
     * 提交实体进行异步删除
     */
    public <T> void delete(T entity) {
        if (entity == null) {
            return;
        }
        
        @SuppressWarnings("unchecked")
        Class<T> entityClass = (Class<T>) entity.getClass();
        Object key = getEntityKey(entity);
        
        synchronized (switchLock) {
            Map<Class<?>, Map<Object, EntityOperation>> activeMaps = getActiveSaveMaps();
            Map<Object, EntityOperation> entityMap = activeMaps.computeIfAbsent(entityClass, k -> new ConcurrentHashMap<>());
            
            EntityOperation existingOp = entityMap.get(key);
            
            if (existingOp != null) {
                // 如果已存在INSERT操作，则直接从队列中移除
                if (existingOp.getOperationType() == OperationType.INSERT) {
                    entityMap.remove(key);
                    return;
                }
                // 其他情况更新为DELETE操作
                entityMap.put(key, new EntityOperation(entity, OperationType.DELETE));
            } else {
                // 不存在则添加DELETE操作
                entityMap.put(key, new EntityOperation(entity, OperationType.DELETE));
            }
        }
    }
    
    /**
     * 提交实体进行异步保存（自动判断是插入还是更新）
     * 为了兼容旧代码，保留此方法
     */
    public <T> void submit(T entity) {
        // 默认作为更新处理
        update(entity);
    }
    /**
     * 处理所有保存Map
     */
    private void processSaveMaps() {
        Map<Class<?>, Map<Object, EntityOperation>> mapsToProcess;
        
        synchronized (switchLock) {
            // 切换Map
            mapsToProcess = getInactiveSaveMaps();
            usingMapA = !usingMapA;
        }
        
        // 切换Map后等待一段时间，确保所有正在进行的写入操作完成
        try {
            Thread.sleep(switchDelayMs);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            LOGGER.warning("Map切换等待被中断");
        }
        
        for (Map.Entry<Class<?>, Map<Object, EntityOperation>> entry : mapsToProcess.entrySet()) {
            Class<?> entityClass = entry.getKey();
            Map<Object, EntityOperation> entityMap = entry.getValue();
            
            processSaveMap(entityClass, entityMap);
        }
        
        // 清理已处理的Map
        mapsToProcess.clear();
    }
    /**
     * 处理单个保存Map
     */
    private void processSaveMap(Class<?> entityClass, Map<Object, EntityOperation> entityMap) {
        if (entityMap.isEmpty()) {
            return;
        }
        
        int insertCount = 0;
        int updateCount = 0;
        int deleteCount = 0;
        
        // 按操作类型分组实体
        Map<OperationType, CopyOnWriteArrayList<Object>> batchOperations = new HashMap<>();
        for (OperationType type : OperationType.values()) {
            batchOperations.put(type, new CopyOnWriteArrayList<>());
        }
        
        // 将实体按操作类型分组
        for (Map.Entry<Object, EntityOperation> entry : entityMap.entrySet()) {
            EntityOperation operation = entry.getValue();
            batchOperations.get(operation.getOperationType()).add(operation.getEntity());
        }
        
        // 批量执行插入操作
        if (!batchOperations.get(OperationType.INSERT).isEmpty()) {
            try {
                EntityManager.batchInsert(batchOperations.get(OperationType.INSERT));
                insertCount = batchOperations.get(OperationType.INSERT).size();
            } catch (SQLException e) {
                LOGGER.log(Level.SEVERE, "Failed to batch insert entities: " + entityClass.getName(), e);
                // 批量操作失败，回退到单个操作
                fallbackToIndividualOperations(entityClass, entityMap, OperationType.INSERT);
            }
        }
        
        // 批量执行更新操作
        if (!batchOperations.get(OperationType.UPDATE).isEmpty()) {
            try {
                EntityManager.batchUpdate(batchOperations.get(OperationType.UPDATE));
                updateCount = batchOperations.get(OperationType.UPDATE).size();
            } catch (SQLException e) {
                LOGGER.log(Level.SEVERE, "Failed to batch update entities: " + entityClass.getName(), e);
                // 批量操作失败，回退到单个操作
                fallbackToIndividualOperations(entityClass, entityMap, OperationType.UPDATE);
            }
        }
        
        // 批量执行删除操作
        if (!batchOperations.get(OperationType.DELETE).isEmpty()) {
            try {
                EntityManager.batchDelete(batchOperations.get(OperationType.DELETE));
                deleteCount = batchOperations.get(OperationType.DELETE).size();
            } catch (SQLException e) {
                LOGGER.log(Level.SEVERE, "Failed to batch delete entities: " + entityClass.getName(), e);
                // 批量操作失败，回退到单个操作
                fallbackToIndividualOperations(entityClass, entityMap, OperationType.DELETE);
            }
        }
        
        StringBuilder logMsg = new StringBuilder();
        if (insertCount > 0) {
            logMsg.append("Batch inserted ").append(insertCount).append(" ");
        }
        if (updateCount > 0) {
            if (logMsg.length() > 0) logMsg.append(", ");
            logMsg.append("Batch updated ").append(updateCount).append(" ");
        }
        if (deleteCount > 0) {
            if (logMsg.length() > 0) logMsg.append(", ");
            logMsg.append("Batch deleted ").append(deleteCount).append(" ");
        }
        
        if (logMsg.length() > 0) {
            logMsg.append("entities of type: ").append(entityClass.getName());
            LOGGER.info(logMsg.toString());
        }
    }
    
    /**
     * 批量操作失败时回退到单个操作
     */
    private void fallbackToIndividualOperations(Class<?> entityClass, Map<Object, EntityOperation> entityMap, 
                                               OperationType operationType) {
        LOGGER.warning("回退到单个" + operationType + "操作: " + entityClass.getName());
        int successCount = 0;
        
        for (Map.Entry<Object, EntityOperation> entry : entityMap.entrySet()) {
            EntityOperation operation = entry.getValue();
            Object key = entry.getKey();
            
            if (operation.getOperationType() != operationType) {
                continue;
            }
            
            try {
                switch (operation.getOperationType()) {
                    case INSERT:
                        EntityManager.insert(operation.getEntity());
                        successCount++;
                        break;
                    case UPDATE:
                        EntityManager.update(operation.getEntity());
                        successCount++;
                        break;
                    case DELETE:
                        EntityManager.delete(operation.getEntity());
                        successCount++;
                        break;
                }
            } catch (SQLException e) {
                LOGGER.log(Level.SEVERE, "单个" + operation.getOperationType() + 
                        "操作也失败: " + entityClass.getName(), e);
                
                // 保存失败，需要检查活跃Map中是否已有新的操作
                synchronized (switchLock) {
                    Map<Class<?>, Map<Object, EntityOperation>> activeMaps = getActiveSaveMaps();
                    Map<Object, EntityOperation> activeEntityMap = activeMaps.computeIfAbsent(
                        entityClass, k -> new ConcurrentHashMap<>());
                    
                    // 处理失败的操作
                    EntityOperation existingOp = activeEntityMap.get(key);
                    if (existingOp == null) {
                        // 如果活跃Map中没有该实体的操作，直接添加失败的操作
                        activeEntityMap.put(key, operation);
                    } else {
                        // 根据失败操作类型和已存在操作类型决定最终操作
                        switch (operation.getOperationType()) {
                            case INSERT:
                                // INSERT失败：如果不是DELETE，则保持INSERT
                                if (existingOp.getOperationType() != OperationType.DELETE) {
                                    activeEntityMap.put(key, operation);
                                }
                                break;
                            case UPDATE:
                                // UPDATE失败：根据已存在操作类型决定
                                if (existingOp.getOperationType() == OperationType.INSERT) {
                                    // 保持INSERT状态
                                    break;
                                } else if (existingOp.getOperationType() == OperationType.DELETE) {
                                    // 保持DELETE状态
                                    break;
                                } else {
                                    // 其他情况保持UPDATE
                                    activeEntityMap.put(key, operation);
                                }
                                break;
                            case DELETE:
                                // DELETE失败：如果是INSERT则移除，否则保持DELETE
                                if (existingOp.getOperationType() == OperationType.INSERT) {
                                    activeEntityMap.remove(key);
                                } else {
                                    activeEntityMap.put(key, operation);
                                }
                                break;
                        }
                    }
                }
            }
        }
        
        if (successCount > 0) {
            LOGGER.info("回退成功: " + successCount + " " + operationType + " 操作完成: " + entityClass.getName());
        }
    }
    /**
     * 立即保存指定类型的所有实体
     */
    public void flushEntityType(Class<?> entityClass) {
        Map<Object, EntityOperation> entityMapA = saveMapsA.get(entityClass);
        Map<Object, EntityOperation> entityMapB = saveMapsB.get(entityClass);
        
        if (entityMapA != null) {
            processSaveMap(entityClass, entityMapA);
            entityMapA.clear();
        }
        
        if (entityMapB != null) {
            processSaveMap(entityClass, entityMapB);
            entityMapB.clear();
        }
    }
    
    /**
     * 立即保存所有实体
     */
    public void flushAll() {
        processSaveMaps();
        
        // 确保两个Map都被处理
        Map<Class<?>, Map<Object, EntityOperation>> remainingMaps = getActiveSaveMaps();
        for (Map.Entry<Class<?>, Map<Object, EntityOperation>> entry : remainingMaps.entrySet()) {
            Class<?> entityClass = entry.getKey();
            Map<Object, EntityOperation> entityMap = entry.getValue();
            
            processSaveMap(entityClass, entityMap);
        }
        
        remainingMaps.clear();
    }
}