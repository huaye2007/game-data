import com.game.db.GameDataService;
import com.game.db.async.AsyncSaveManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public abstract class AbstractCacheRepository<V> {

    private static final Logger logger = LoggerFactory.getLogger(AbstractCacheRepository.class);

    protected final Class<V> entityClass;
    protected final GameDataService dataService;
    protected final AsyncSaveManager saveManager;

    public AbstractCacheRepository(Class<V> entityClass) {
        this.entityClass = entityClass;
        this.dataService = GameDataService.getInstance();
        this.saveManager = AsyncSaveManager.getInstance();
        logger.info("Initialized AbstractCacheRepository for entity class: {}", entityClass.getSimpleName());
    }

    protected V getEntity(Object id) {
        try {
            V entity = dataService.getEntity(entityClass, id);
            logger.debug("Retrieved entity from database: {} with id: {}", entityClass.getSimpleName(), id);
            return entity;
        } catch (Exception e) {
            logger.error("Error retrieving entity from database: {} with id: {}", entityClass.getSimpleName(), id, e);
            return null;
        }
    }

    protected List<V> getEntityList(Object key) {
        try {
            List<V> entities = dataService.getEntityList(entityClass, key);
            logger.debug("Retrieved entity list from database: {} with key: {}", entityClass.getSimpleName(), key);
            return entities;
        } catch (Exception e) {
            logger.error("Error retrieving entity list from database: {} with key: {}", entityClass.getSimpleName(), key, e);
            return List.of();
        }
    }

    protected List<V> getAllEntities() {
        try {
            List<V> entities = dataService.getAllEntities(entityClass);
            logger.debug("Retrieved all entities from database: {}", entityClass.getSimpleName());
            return entities;
        } catch (Exception e) {
            logger.error("Error retrieving all entities from database: {}", entityClass.getSimpleName(), e);
            return List.of();
        }
    }


    public void asyncSave(V entity) {
        saveManager.saveData(entity);
        logger.debug("Scheduled asynchronous save for entity: {}", entity);
    }

    public void asyncBatchSave(List<V> entities) {
        saveManager.batchSaveData(entities);
        logger.debug("Scheduled asynchronous batch save for {} entities", entities.size());
    }
}