import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public abstract class UniqueCacheRepository<K, V> extends AbstractCacheRepository<K, V> implements CacheRepository<K, V> {

    private static final Logger logger = LoggerFactory.getLogger(UniqueCacheRepository.class);
    private final Map<K, V> cache = new ConcurrentHashMap<>();

    public UniqueCacheRepository(Class<V> entityClass) {
        super(entityClass);
    }

    @Override
    public V load(K key) {
        V value = get(key);
        if (value == null) {
            logger.debug("Loading entity with key {} from database", key);
            value = getEntity(key);
            if (value != null) {
                put(key, value);
                logger.info("Loaded and cached entity with key {}", key);
            } else {
                logger.warn("No entity found with key {}", key);
            }
        } else {
            logger.debug("Retrieved entity with key {} from cache", key);
        }
        return value;
    }

    @Override
    public V get(K key) {
        return cache.get(key);
    }

    @Override
    public void put(K key, V value) {
        cache.put(key, value);
        logger.debug("Added entity with key {} to cache", key);
    }

    @Override
    public void remove(K key) {
        cache.remove(key);
        logger.debug("Removed entity with key {} from cache", key);
    }

    @Override
    public Map<K, V> loadAll() {
        throw new UnsupportedOperationException("Loading all unique entities is not supported.");
    }

    protected abstract V getEntity(K key);
}