package org.radargun.stages;

import java.util.concurrent.atomic.AtomicReferenceArray;
import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;

import org.radargun.Operation;
import org.radargun.config.Property;
import org.radargun.config.PropertyDelegate;
import org.radargun.config.Stage;
import org.radargun.jpa.EntityGenerator;
import org.radargun.stages.test.Conversation;
import org.radargun.stages.test.SchedulingSelector;
import org.radargun.stages.test.Stressor;
import org.radargun.stages.test.TestSetupStage;
import org.radargun.traits.InjectTrait;
import org.radargun.traits.InternalsExposition;
import org.radargun.traits.JpaProvider;
import org.radargun.traits.Transactional;

/**
 * @author Radim Vansa &lt;rvansa@redhat.com&gt;
 */
@Stage(doc = "Tests create-read-update-delete operations with JPA entities.")
public class CrudOperationsTestSetupStage extends TestSetupStage {
   private final boolean trace = log.isTraceEnabled();

   @Property(doc = "Generator of the entities", complexConverter = EntityGenerator.Converter.class)
   protected EntityGenerator entityGenerator;

   @PropertyDelegate(prefix = "create.")
   protected TxInvocationSetting create = new TxInvocationSetting();

   @PropertyDelegate(prefix = "read.")
   protected TxInvocationSetting read = new TxInvocationSetting();

   @PropertyDelegate(prefix = "update.")
   protected TxInvocationSetting update = new TxInvocationSetting();

   @PropertyDelegate(prefix = "delete.")
   protected TxInvocationSetting delete = new TxInvocationSetting();

   @Property(doc = "Max number of identifiers returned within one id update query. Default is 1000")
   protected int queryMaxResults = 1000;

   @InjectTrait(dependency = InjectTrait.Dependency.MANDATORY)
   protected JpaProvider jpaProvider;

   @InjectTrait(dependency = InjectTrait.Dependency.MANDATORY)
   protected Transactional transactional;

   @InjectTrait
   protected InternalsExposition internalsExposition;

   private EntityManagerFactory entityManagerFactory;
   private AtomicReferenceArray loadedIds;
   private QueryThread queryThread;

   @Override
   protected SchedulingSelector<Conversation> createSelector() {
      return new SchedulingSelector.Builder<>(Conversation.class)
            .add(new Create(), create.invocations, create.interval)
            .add(new Read(), read.invocations, read.interval)
            .add(new Update(), update.invocations, update.interval)
            .add(new Delete(), delete.invocations, delete.interval)
            .build();
   }

   @Override
   protected void prepare() {
      if (entityGenerator == null) {
         entityGenerator = (EntityGenerator) slaveState.get(EntityGenerator.ENTITY_GENERATOR);
         if (entityGenerator == null) {
            throw new IllegalStateException("Entity generator was not specified and no entity generator was used before.");
         }
      } else {
         slaveState.put(EntityGenerator.ENTITY_GENERATOR, entityGenerator);
      }

      entityManagerFactory = jpaProvider.getEntityManagerFactory();

      int numEntries = JpaUtils.getNumEntries(entityManagerFactory, transactional, entityGenerator.entityClass());
      loadedIds = new AtomicReferenceArray(numEntries);
      queryThread = new QueryThread(this, entityGenerator.entityClass(), loadedIds, jpaProvider, transactional, queryMaxResults);
      log.infof("Database contains %d entities", numEntries);
      queryThread.dirtyUpdateLoadedIds();
      log.info("First update finished");
      queryThread.start();
      runningTest.addStopListener(new Runnable() {
         @Override
         public void run() {
            try {
               queryThread.setFinished();
               queryThread.join();
            } catch (InterruptedException e) {
               log.error("Failed to join updater thread", e);
            }
         }
      });

      if (internalsExposition != null) {
         internalsExposition.resetCustomStatistics("second-level-cache:" + entityGenerator.entityClass().getName());
      }
   }

   private Object getIdNotNull(int index) {
      int initialIndex = index;
      Object id;
      while (!runningTest.isTerminated()) {
         id = loadedIds.get(index);
         if (id != null) {
            return id;
         }
         index = (index + 1) % loadedIds.length();
         if (index == initialIndex) {
            throw new RuntimeException("No set id!");
         }
      }
      throw new RuntimeException("Test was terminated");
   }

   private abstract class TxConversation implements Conversation {
      private final Operation txOperation;
      private final TxInvocationSetting invocationSetting;

      public TxConversation(Operation txOperation, TxInvocationSetting invocationSetting) {
         this.txOperation = txOperation;
         this.invocationSetting = invocationSetting;
      }

      @Override
      public void run(Stressor stressor) throws InterruptedException {
         EntityManager entityManager = entityManagerFactory.createEntityManager();
         Transactional.Transaction tx = null;
         try {
            tx = transactional.getTransaction();
            tx.wrap(entityManager);
            stressor.startTransaction(tx);

            for (int i = 0; i < invocationSetting.transactionSize; ++i) {
               invokeInTransaction(stressor, entityManager);
            }

            if (invocationSetting.shouldCommit(stressor.getRandom())) {
               stressor.commitTransaction(tx, txOperation);
            }
            tx = null;
         } finally {
            if (tx != null) {
               stressor.rollbackTransaction(tx, txOperation);
            }
            entityManager.close();
         }
      }
      protected abstract void invokeInTransaction(Stressor stressor, EntityManager entityManager);
   }

   private class Create extends TxConversation {
      public Create() {
         super(JpaInvocations.Create.TX, create);
      }

      @Override
      protected void invokeInTransaction(Stressor stressor, EntityManager entityManager) {
         Object entity = entityGenerator.create(stressor.getRandom());
         stressor.makeRequest(new JpaInvocations.Create(entityManager, entity));
      }
   }

   private class Read extends TxConversation {
      public Read() {
         super(JpaInvocations.Find.TX, read);
      }

      @Override
      protected void invokeInTransaction(Stressor stressor, EntityManager entityManager) {
         int index = stressor.getRandom().nextInt(loadedIds.length());
         Object id = getIdNotNull(index);
         stressor.makeRequest(new JpaInvocations.Find(entityManager, entityGenerator.entityClass(), id));
      }
   }

   private class Update extends TxConversation {
      public Update() {
         super(JpaInvocations.Update.TX, update);
      }

      @Override
      protected void invokeInTransaction(Stressor stressor, EntityManager entityManager) {
         int index = stressor.getRandom().nextInt(loadedIds.length());
         Object id = getIdNotNull(index);
         Object entity = stressor.makeRequest(new JpaInvocations.Find(entityManager, entityGenerator.entityClass(), id));
         if (entity != null) {
            entityGenerator.mutate(entity, stressor.getRandom());
            stressor.makeRequest(new JpaInvocations.Update(entityManager, entity));
         }
      }
   }

   private class Delete extends TxConversation {
      public Delete() {
         super(JpaInvocations.Remove.TX, delete);
      }

      @Override
      protected void invokeInTransaction(Stressor stressor, EntityManager entityManager) {
         // When two stressors try to remove the same entity concurrenlty, the whole
         // transactions will fail. Therefore, even with the same rate of creates
         // and removals, usually there are more entries created than removed.
         // QueryThread stores this surplus and it should be removed before regular ids.
         Object id = queryThread.getNextOverflowId();
         if (id == null) {
            int index = stressor.getRandom().nextInt(loadedIds.length());
            id = getIdNotNull(index);
         }
         Object entity = stressor.makeRequest(new JpaInvocations.Find(entityManager, entityGenerator.entityClass(), id));
         if (entity != null) {
            stressor.makeRequest(new JpaInvocations.Remove(entityManager, entity));
         }
      }
   }
}
