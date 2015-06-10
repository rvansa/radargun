package org.radargun.stages;

import java.util.Comparator;
import java.util.NoSuchElementException;
import java.util.Random;
import java.util.concurrent.ConcurrentSkipListSet;
import javax.persistence.EntityManager;
import javax.persistence.PersistenceUnitUtil;

import org.radargun.Operation;
import org.radargun.config.Init;
import org.radargun.config.Property;
import org.radargun.config.PropertyDelegate;
import org.radargun.config.Stage;
import org.radargun.jpa.EntityGenerator;
import org.radargun.stages.query.Invocations;
import org.radargun.stages.query.QueryBase;
import org.radargun.stages.query.QueryConfiguration;
import org.radargun.stages.test.Conversation;
import org.radargun.stages.test.SchedulingSelector;
import org.radargun.stages.test.Stressor;
import org.radargun.stages.test.TestSetupStage;
import org.radargun.traits.InjectTrait;
import org.radargun.traits.JpaProvider;
import org.radargun.traits.Query;
import org.radargun.traits.Queryable;
import org.radargun.traits.Transactional;

/**
 * @author Radim Vansa &lt;rvansa@redhat.com&gt;
 */
@Stage(doc = "Executes both queries and delete-creates or updates.")
public class QueryChangingSetTestSetupStage extends TestSetupStage {
   protected static final Operation RENEW = Operation.register(JpaProvider.TRAIT + ".Renew");

   @PropertyDelegate
   protected QueryConfiguration query = new QueryConfiguration();

   @PropertyDelegate
   protected QueryBase base = new QueryBase();

   @Property(doc = "Generator of the entities. Default is last used entity generator.",
         complexConverter = EntityGenerator.Converter.class)
   protected EntityGenerator entityGenerator;

   @PropertyDelegate(prefix = "queries.")
   protected TestSetupStage.InvocationSetting queries = new InvocationSetting();

   @PropertyDelegate(prefix = "renewals.")
   protected InvocationSetting renewals = new InvocationSetting();

   @PropertyDelegate(prefix = "updates.")
   protected InvocationSetting updates = new InvocationSetting();

   @InjectTrait(dependency = InjectTrait.Dependency.MANDATORY)
   private Queryable queryable;

   @InjectTrait(dependency = InjectTrait.Dependency.MANDATORY)
   private Transactional transactional;

   @InjectTrait(dependency = InjectTrait.Dependency.MANDATORY)
   protected JpaProvider jpaProvider;

   protected PersistenceUnitUtil persistenceUnitUtil;
   protected ConcurrentSkipListSet<Number> ids = new ConcurrentSkipListSet<>(new Comparator<Number>() {
      @Override
      public int compare(Number o1, Number o2) {
         return Double.compare(o1.doubleValue(), o2.doubleValue());
      }
   });

   @Init
   public void init() {
      base.init(queryable, query);
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
      if (!query.clazz.equals(entityGenerator.entityClass().getName())) {
         throw new IllegalStateException("Using entity generator for class " + entityGenerator.entityClass()
               + " but query should be executed for " + query.clazz);
      }

      persistenceUnitUtil = jpaProvider.getEntityManagerFactory().getPersistenceUnitUtil();
   }

   @Override
   protected SchedulingSelector<Conversation> createSelector() {
      return new SchedulingSelector.Builder<>(Conversation.class)
            .add(new QueryConversation(), queries.invocations, queries.interval)
            .add(new Renew(), renewals.invocations, renewals.interval)
            .add(new Update(), updates.invocations, updates.interval)
            .build();
   }

   protected Number getRandomId(Random random) {
      int size = ids.size();
      if (size == 0) {
         return null;
      }
      Number first, last;
      try {
         first = ids.first();
         last = ids.last();
      } catch (NoSuchElementException e) {
         return null;
      }
      double randomIdValue = random.nextDouble() * (last.doubleValue() - first.doubleValue())/ size + first.doubleValue();
      Number id = ids.floor(randomIdValue);
      return id;
   }

   private class QueryConversation implements Conversation {
      @Override
      public void run(Stressor stressor) throws InterruptedException {
         int randomQueryNumber = stressor.getRandom().nextInt(base.getNumQueries());
         Query query = base.buildQuery(randomQueryNumber);
         Query.Result queryResult;
         Transactional.Transaction tx = null;
         try (Query.Context context = queryable.createContext(null)) {
            tx = transactional.getTransaction();
            tx.wrap(context);
            stressor.startTransaction(tx);

            queryResult = stressor.makeRequest(new Invocations.Query(query, context));

            for (Object entity : queryResult.values()) {
               Number id = (Number) persistenceUnitUtil.getIdentifier(entity);
               ids.add(id);
            }

            int size = queryResult.size();
            int min = base.getMinResultSize();
            if (base.isCheckSameResult() && min >= 0 && min != size) {
               throw new IllegalStateException("Another thread reported " + min + " results while we have " + size);
            }
            base.updateMinResultSize(size);
            int max = base.getMaxResultSize();
            if (base.isCheckSameResult() && max >= 0 && max != size) {
               throw new IllegalStateException("Another thread reported " + max + " results while we have " + size);
            }
            base.updateMaxResultSize(size);

            stressor.commitTransaction(tx, null);
            tx = null;
         } finally {
            if (tx != null) stressor.rollbackTransaction(tx, null);
         }
      }
   }

   private class Renew implements Conversation {
      @Override
      public void run(Stressor stressor) throws InterruptedException {
         Number id = getRandomId(stressor.getRandom());
         if (id == null) return;
         EntityManager entityManager = jpaProvider.getEntityManagerFactory().createEntityManager();
         Transactional.Transaction tx = null;
         try {
            Object entity;
            for (; ; ) {
               entity = stressor.makeRequest(new JpaInvocations.Find(entityManager, entityGenerator.entityClass(), id));
               ids.remove(id);
               if (entity != null) break;
               id = getRandomId(stressor.getRandom());
               if (id == null) return;
            }
            stressor.makeRequest(new JpaInvocations.Remove(entityManager, entity));
            entity = entityGenerator.copy(entity);
            stressor.makeRequest(new JpaInvocations.Create(entityManager, entity));

            stressor.commitTransaction(tx, RENEW);
            tx = null;
         } finally {
            if (tx != null) stressor.rollbackTransaction(tx, RENEW);
            entityManager.close();
         }
      }
   }

   private class Update implements Conversation {
      @Override
      public void run(Stressor stressor) throws InterruptedException {
         Number id = getRandomId(stressor.getRandom());
         if (id == null) return;
         EntityManager entityManager = jpaProvider.getEntityManagerFactory().createEntityManager();
         Transactional.Transaction tx = null;
         try {
            Object entity;
            for (; ; ) {
               entity = stressor.makeRequest(new JpaInvocations.Find(entityManager, entityGenerator.entityClass(), id));
               if (entity != null) break;
               ids.remove(id);
               id = getRandomId(stressor.getRandom());
               if (id == null) return;
            }
            entityGenerator.mutate(entity, stressor.getRandom());
            stressor.makeRequest(new JpaInvocations.Update(entityManager, entity));

            stressor.commitTransaction(tx, JpaInvocations.Update.TX);
            tx = null;
         } finally {
            if (tx != null) stressor.rollbackTransaction(tx, JpaInvocations.Update.TX);
            entityManager.close();
         }
      }
   }
}
