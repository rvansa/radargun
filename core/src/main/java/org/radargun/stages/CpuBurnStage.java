package org.radargun.stages;

import org.radargun.DistStageAck;
import org.radargun.config.Property;
import org.radargun.config.Stage;
import org.radargun.stages.test.Blackhole;
import org.radargun.state.ServiceListenerAdapter;

/**
 * @author Radim Vansa &ltrvansa@redhat.com&gt;
 */
@Stage(doc = "Burns CPU time in several threads to simulate CPU intensive app.")
public class CpuBurnStage extends AbstractDistStage {
   @Property(doc = "Number of threads burning CPU.", optional = false)
   public int numThreads;

   @Property(doc = "If set to true, all threads are stopped and the num-threads attribute is ignored.")
   public boolean stop = false;

   @Override
   public DistStageAck executeOnSlave() {
      State state = (State) slaveState.remove(CpuBurnStage.class.getName());
      if (state != null) {
         if (!stop) {
            log.warn("There are already running threads!");
         }
         state.stop();
      } else if (stop) {
         log.warn("There are no running threads!");
      }
      if (!stop) {
         if (numThreads <= 0) {
            return errorResponse("Cannot use num-threads <= 0!");
         }
         slaveState.put(CpuBurnStage.class.getName(), new State(numThreads));
      }
      return successfulResponse();
   }

   private class State extends ServiceListenerAdapter {
      final Thread[] threads;
      volatile boolean terminate = false;

      private State(int numThreads) {
         threads = new Thread[numThreads];
         for (int i = 0; i < numThreads; ++i) {
            threads[i] = new Thread(() -> {
               while (!terminate) {
                  Blackhole.consumeCpu();
               }
            }, "CpuBurner-" + i);
            threads[i].start();
         }
         slaveState.addServiceListener(this);
      }

      public void stop() {
         terminate = true;
         for (Thread t : threads) {
            try {
               t.join();
            } catch (InterruptedException e) {
               log.warn("Interrupted while waiting for thread to finish", e);
               Thread.currentThread().interrupt();
            }
         }
         slaveState.removeServiceListener(this);
      }

      @Override
      public void beforeServiceStop(boolean graceful) {
         stop();
      }
   }
}
