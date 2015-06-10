package org.radargun.stages;

import java.util.concurrent.atomic.AtomicInteger;
import javax.persistence.EntityManagerFactory;
import javax.persistence.metamodel.EntityType;

import org.radargun.DistStageAck;
import org.radargun.config.Property;
import org.radargun.config.Stage;
import org.radargun.traits.InjectTrait;
import org.radargun.traits.JpaProvider;
import org.radargun.traits.Transactional;

/**
 * @author Radim Vansa &lt;rvansa@redhat.com&gt;
 */
@Stage(doc = "Lists all entities in DB/cache.")
public class ListEntitiesStage extends AbstractDistStage {
   @Property(name = "class", doc = "Type of entity that should be removed.")
   protected String clazzName;

   @Property(doc = "List entities from cache. Default is false.")
   protected boolean cached;

   @InjectTrait(dependency = InjectTrait.Dependency.MANDATORY)
   protected JpaProvider jpaProvider;

   @InjectTrait(dependency = InjectTrait.Dependency.MANDATORY)
   protected Transactional transactional;

   @Override
   public DistStageAck executeOnSlave() {
      EntityManagerFactory emf = jpaProvider.getEntityManagerFactory();
      if (clazzName == null) {
         for (EntityType<?> entityType : emf.getMetamodel().getEntities()) {
            list(emf, entityType);
         }
      } else {
         final Class<?> cls;
         try {
            cls = Class.forName(clazzName);
         } catch (ClassNotFoundException e) {
            return errorResponse("Failed to load entity class", e);
         }
         list(emf, emf.getMetamodel().entity(cls));
      }
      return successfulResponse();
   }

   private void list(EntityManagerFactory emf, EntityType<?> entityType) {
      log.infof("Listing %s entities of type '%s':", cached ? "cached" : "persisted", entityType.getJavaType().getName());
      if (!cached) {
         final AtomicInteger counter = new AtomicInteger();
         JpaUtils.scroll(emf, transactional, entityType, 100, new JpaUtils.EntityConsumer() {
            @Override
            public void accept(Object entity) {
               log.info(String.format("%d:\t%s", counter.incrementAndGet(), entity));
            }
         });
      } else {
         int counter = 0;
         for (Object entity : jpaProvider.getSecondLevelCacheEntities(entityType.getJavaType().getName())) {
            log.info(String.format("%d:\t%s", ++counter, entity));
         }
      }
   }

}
