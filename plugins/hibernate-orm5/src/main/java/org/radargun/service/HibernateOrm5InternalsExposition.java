package org.radargun.service;

import java.util.Collections;
import java.util.Map;

import org.hibernate.cache.infinispan.impl.BaseRegion;
import org.hibernate.cache.internal.StandardQueryCache;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.infinispan.stats.Stats;
import org.radargun.logging.Log;
import org.radargun.logging.LogFactory;
import org.radargun.traits.InternalsExposition;

/**
 * @author Radim Vansa &lt;rvansa@redhat.com&gt;
 */
public class HibernateOrm5InternalsExposition implements InternalsExposition {
   protected static final String QUERY_CACHE = "query-cache";
   protected static final String SECOND_LEVEL_CACHE = "second-level-cache";
   private final Log log = LogFactory.getLog(getClass());
   private final HibernateOrm5Service service;

   public HibernateOrm5InternalsExposition(HibernateOrm5Service service) {
      this.service = service;
   }

   @Override
   public Map<String, Number> getValues() {
      return Collections.EMPTY_MAP;
   }

   @Override
   public String getCustomStatistics(String type) {
      String[] parts = type.split(":");
      if (parts.length == 0) {
         log.warnf("No type");
         return null;
      }
      if (SECOND_LEVEL_CACHE.equals(parts[0])) {
         if (parts.length != 3) {
            log.warnf("Unknown type '%s'", type);
         }
         return getStatsFromRegion(parts[2], getRegion(parts[1]));
      } else if (QUERY_CACHE.equals(parts[0])) {
         if (parts.length != 2) {
            log.warnf("Unknown type '%s'", type);
         }
         // beware: hits/misses does not match to actually retrieved cached results;
         // after finding the cached result we check against timestamps cache, too
         return getStatsFromRegion(parts[1], getRegion(StandardQueryCache.class.getName()));
      } else {
         log.warnf("Unknown part '%s'", parts[0]);
      }
      return null;
   }

   protected String getStatsFromRegion(String property, BaseRegion region) {
      if (region == null) {
         return null;
      }
      Stats stats = region.getCache().getStats();
      switch (property) {
         case "hits":
            return String.valueOf(stats.getHits());
         case "misses":
            return String.valueOf(stats.getMisses());
         case "numberOfEntries":
            return String.valueOf(stats.getCurrentNumberOfEntries());
         default:
            log.warnf("Unknown property '%s'", property);
            return null;
      }
   }

   @Override
   public void resetCustomStatistics(String type) {
      if (type.equals(QUERY_CACHE)) {
         BaseRegion region = getRegion(StandardQueryCache.class.getName());
         if (region == null) return;
         region.getCache().getStats().reset();
         return;
      }
      String[] parts = type.split(":");
      if (parts.length != 2) throw new IllegalArgumentException(type);
      if (SECOND_LEVEL_CACHE.equals(parts[0])) {
         BaseRegion region = getRegion(parts[1]);
         if (region == null) return;
         Stats stats = region.getCache().getStats();
         stats.reset();
      } else {
         log.warnf("Unknown part '%s'", parts[0]);
      }
   }

   protected BaseRegion getRegion(String regionName) {
      SessionFactoryImplementor sfi = service.getEntityManagerFactory().unwrap(SessionFactoryImplementor.class);
      if (!sfi.getSettings().isSecondLevelCacheEnabled()) {
         log.warn("Second-level cache is not enabled");
         return null;
      }
      BaseRegion region = (BaseRegion) sfi.getSecondLevelCacheRegion(regionName);
      if (region == null) {
         log.warnf("Cannot find region '%s' available are: %s", regionName, sfi.getAllSecondLevelCacheRegions().keySet());
         return null;
      }
      return region;
   }
}
