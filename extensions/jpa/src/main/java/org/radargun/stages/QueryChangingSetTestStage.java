package org.radargun.stages;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.radargun.DistStageAck;
import org.radargun.StageResult;
import org.radargun.config.Stage;
import org.radargun.reporting.Report;
import org.radargun.stages.query.QueryBase;
import org.radargun.stages.test.TestStage;
import org.radargun.state.SlaveState;
import org.radargun.stats.Statistics;
import org.radargun.traits.InjectTrait;
import org.radargun.traits.InternalsExposition;

/**
 * @author Radim Vansa &lt;rvansa@redhat.com&gt;
 */
@Stage(doc = "Executes test; follows query-changing-set-test-setup, gathering statistics in the end.")
public class QueryChangingSetTestStage extends TestStage {

   @InjectTrait
   private InternalsExposition internalsExposition;

   private QueryBase base;

   @Override
   public DistStageAck executeOnSlave() {
      base = (QueryBase) slaveState.get(QueryBase.class.getName());
      if (base == null) {
         return errorResponse("This stage was not preceded by query-changing-set-test-setup");
      }
      if (internalsExposition != null) {
         internalsExposition.resetCustomStatistics("query-cache");
      }
      return super.executeOnSlave();
   }

   @Override
   protected QueryAck newStatisticsAck(List<Statistics> statistics, boolean failed) {
      QueryBase.Data data = base.createQueryData(internalsExposition);
      return new QueryAck(slaveState, statistics, runningTest.getUsedThreads(), failed, data);
   }

   @Override
   public StageResult processAckOnMaster(List<DistStageAck> acks) {
      StageResult result = super.processAckOnMaster(acks);
      if (result.isError()) return result;

      Map<Integer, QueryBase.Data> results = instancesOf(acks, QueryAck.class).stream()
            .collect(Collectors.toMap(ack -> ack.getSlaveIndex(), ack -> ack.data));
      Report.Test test = getTest(true); // the test was already created in super.processAckOnMaster

      base.checkAndRecordResults(results, test, getTestIteration());
      return result;
   }

   protected static class QueryAck extends StatisticsAck {
      private final QueryBase.Data data;

      public QueryAck(SlaveState slaveState, List<Statistics> iterations, int usedThreads, boolean failed, QueryBase.Data data) {
         super(slaveState, iterations, usedThreads, failed);
         this.data = data;
      }
   }

}
