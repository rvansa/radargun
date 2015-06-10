package org.radargun.traits;

import java.util.Collection;
import javax.persistence.EntityManagerFactory;

import org.radargun.Operation;

/**
 * @author Radim Vansa &lt;rvansa@redhat.com&gt;
 */
@Trait(doc = "Provides JPA EntityManagerFactory")
public interface JpaProvider {
   String TRAIT = JpaProvider.class.getSimpleName();
   Operation FIND = Operation.register(TRAIT + ".Find");
   Operation PERSIST = Operation.register(TRAIT + ".Persist");
   Operation REMOVE = Operation.register(TRAIT + ".Remove");
   Operation QUERY = Operation.register(TRAIT + ".Query");

   EntityManagerFactory getEntityManagerFactory();

   SecondLevelCacheStatistics getSecondLevelCacheStatistics(String cacheName);

   <T> Collection<T> getSecondLevelCacheEntities(String name);
   // TODO: reset stats
   // TODO: this could replace InternalsExposition

   class SecondLevelCacheStatistics {
      private final long hitCount;
      private final long missCount;
      private final long putCount;
      private final long elementCountInMemory;
      private final long elementCountOnDisk;

      public SecondLevelCacheStatistics(long hitCount, long missCount, long putCount, long elementCountInMemory, long elementCountOnDisk) {
         this.hitCount = hitCount;
         this.missCount = missCount;
         this.putCount = putCount;
         this.elementCountInMemory = elementCountInMemory;
         this.elementCountOnDisk = elementCountOnDisk;
      }

      public long getHitCount() {
         return hitCount;
      }

      public long getMissCount() {
         return missCount;
      }

      public long getPutCount() {
         return putCount;
      }

      public long getElementCountInMemory() {
         return elementCountInMemory;
      }

      public long getElementCountOnDisk() {
         return elementCountOnDisk;
      }
   }
}
