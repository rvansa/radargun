package org.radargun.stages;

import org.radargun.DistStageAck;
import org.radargun.config.Property;
import org.radargun.config.Stage;
import org.radargun.jpa.EntityGenerator;
import org.radargun.traits.InjectTrait;
import org.radargun.traits.JpaProvider;

@Stage(doc = "Clears second level cache")
public class ClearSecondLevelCacheStage extends AbstractDistStage {
   @Property(doc = "Entity generator used for class selection.", complexConverter = EntityGenerator.Converter.class)
   private EntityGenerator entityGenerator;

   @Property(name = "class", doc = "Type of entity that should be removed.")
   private String clazzName;

   @Property(doc = "Clear all second level caches. Default is false.")
   private boolean all = false;

   @Property(doc = "Check if the cache is empty after clear, and fail if not (note: beware of background operation). Default is false.")
   private boolean check = false;

   @InjectTrait(dependency = InjectTrait.Dependency.MANDATORY)
   JpaProvider jpaProvider;

   @Override
   public DistStageAck executeOnSlave() {
      if (all) {
         jpaProvider.getEntityManagerFactory().getCache().evictAll();
      } else {
         Class<?> targetEntity = null;
         if (clazzName == null) {
            if (entityGenerator == null) {
               entityGenerator = (EntityGenerator) slaveState.get(EntityGenerator.ENTITY_GENERATOR);
            }
            if (entityGenerator == null) {
               return errorResponse("No entity class/generator specified, and none used before.");
            }
            targetEntity = entityGenerator.entityClass();
         } else {
            try {
               targetEntity = Class.forName(clazzName);
            } catch (ClassNotFoundException e) {
               return errorResponse("Cannot find entity class", e);
            }
         }
         jpaProvider.getEntityManagerFactory().getCache().evict(targetEntity);
         if (check) {
            JpaProvider.SecondLevelCacheStatistics stats = jpaProvider.getSecondLevelCacheStatistics(targetEntity.getName());
            // kind of hack: for Hibernate this makes sure that the region is evicted
            if (stats.getElementCountInMemory() > 0) {
               errorResponse("Cache still contains " + stats.getElementCountInMemory() + " entities");
            }
         }
      }
      return successfulResponse();
   }
}
