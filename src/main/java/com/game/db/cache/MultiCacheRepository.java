import com.game.db.EntityManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public abstract class MultiCacheRepository<K, V> extends AbstractCacheRepository<V> implements CacheRepository<K, List<V>> {

    private static final Logger logger = LoggerFactory.getLogger(MultiCacheRepository.class);

    private final ConcurrentMap<K, List<V>> cache = new ConcurrentHashMap<>();

    public MultiCacheRepository(Class<V> entityClass) {
        super(entityClass);
    }

    @Override
    public List<V> load(K key) {
        List<V> entities = cache.get(key);
        if (entities == null) {
            logger.debug("Cache miss for key: {}", key);
            entities = getEntityList(key);
            if (entities == null || entities.isEmpty()) {
                entities = new ArrayList<>(); // Cache empty list to avoid repeated DB calls
            }
            cache.put(key, entities);
            logger.debug("Loaded and cached {} entities for key: {}", entities.size(), key);
        } else {
            logger.debug("Cache hit for key: {}", key);
        }
        return entities;
    }

    protected abstract List<V> getEntityList(K key);

    @Override
    public List<V> get(K key) {
        return cache.get(key);
    }

    @Override
    public void put(K key, List<V> value) {
        cache.put(key, value);
        logger.debug("Updated cache for key: {} with {} entities", key, value.size());
    }

    @Override
    public void remove(K key) {
        cache.remove(key);
        logger.debug("Removed entry from cache for key: {}", key);
    }

    @Override
    public List<V> loadAll() {
        List<V> allEntities = getAllEntities();
        ConcurrentMap<K, List<V>> tempCache = new ConcurrentHashMap<>();
        for (V entity : allEntities) {
            K key = extractKey(entity); // You need to implement this method
            tempCache.computeIfAbsent(key, k -> new ArrayList<>()).add(entity);
        }
        cache.putAll(tempCache);
        logger.info("Loaded and cached all entities. Total keys: {}", cache.size());
        return allEntities;
    }


    protected abstract K extractKey(V entity); // Implement this to extract the key from the entity

    protected abstract List<V> getAllEntities();
}