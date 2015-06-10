package org.radargun.stages;

import java.text.DecimalFormat;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.radargun.DistStageAck;
import org.radargun.StageResult;
import org.radargun.config.Stage;
import org.radargun.jpa.EntityGenerator;
import org.radargun.reporting.Report;
import org.radargun.stages.test.TestStage;
import org.radargun.state.SlaveState;
import org.radargun.stats.Statistics;
import org.radargun.traits.InjectTrait;
import org.radargun.traits.JpaProvider;
import org.radargun.utils.MinMax;

/**
 * @author Radim Vansa &lt;rvansa@redhat.com&gt;
 */
@Stage(doc = "Follows crud-operations-test-setup, gathering the proper statistics in the end.")
public class CrudOperationsTestStage extends TestStage {
   @InjectTrait(dependency = InjectTrait.Dependency.MANDATORY)
   protected JpaProvider jpaProvider;

   @Override
   protected CrudAck newStatisticsAck(List<Statistics> statistics, boolean failed) {
      EntityGenerator entityGenerator = (EntityGenerator) slaveState.get(EntityGenerator.ENTITY_GENERATOR);
      JpaProvider.SecondLevelCacheStatistics stats = jpaProvider.getSecondLevelCacheStatistics(entityGenerator.entityClass().getName());
      return new CrudAck(slaveState, statistics, runningTest.getUsedThreads(), failed,
            stats.getHitCount(), stats.getMissCount(), stats.getElementCountInMemory());
   }

   @Override
   public StageResult processAckOnMaster(List<DistStageAck> acks) {
      StageResult result = super.processAckOnMaster(acks);
      if (result.isError()) return result;
      MinMax.Double hitRatio = new MinMax.Double();
      MinMax.Long numEntries = new MinMax.Long();
      Map<Integer, Report.SlaveResult> hitRatios = new HashMap<>();
      Map<Integer, Report.SlaveResult> numEntriesPerSlave = new HashMap<>();
      DecimalFormat formatter = new DecimalFormat("##.#%");
      for (CrudAck ack : instancesOf(acks, CrudAck.class)) {
         double ratio = 0;
         if (ack.cacheHits + ack.cacheMisses != 0) {
            ratio = (double) ack.cacheHits / (double) (ack.cacheHits + ack.cacheMisses);
            hitRatio.add(ratio);
         }
         hitRatios.put(ack.getSlaveIndex(), new Report.SlaveResult(formatter.format(ratio), false));

         numEntries.add(ack.cacheEntries);
         numEntriesPerSlave.put(ack.getSlaveIndex(), new Report.SlaveResult(String.valueOf(ack.cacheEntries), false));
      }
      Report.Test test = getTest(true);
      if (test != null) {
         test.addResult(getTestIteration(), new Report.TestResult("Hit ratio", hitRatios, hitRatio.toString(formatter), false));
         test.addResult(getTestIteration(), new Report.TestResult("Local cache entries", numEntriesPerSlave, numEntries.toString(), false));
      } else {
         log.info("No test name - results are not recorded");
      }
      return result;
   }

   private static class CrudAck extends StatisticsAck {
      private final long cacheHits;
      private final long cacheMisses;
      private final long cacheEntries;

      protected CrudAck(SlaveState slaveState, List<Statistics> statistics, int usedThreads, boolean failed,
                        long cacheHits, long cacheMisses, long cacheEntries) {
         super(slaveState, statistics, usedThreads, failed);
         this.cacheHits = cacheHits;
         this.cacheMisses = cacheMisses;
         this.cacheEntries = cacheEntries;
      }
   }
}
