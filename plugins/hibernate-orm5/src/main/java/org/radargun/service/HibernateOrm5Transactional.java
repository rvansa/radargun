package org.radargun.service;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.transaction.SystemException;
import javax.transaction.TransactionManager;

import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.engine.transaction.jta.platform.spi.JtaPlatform;
import org.radargun.logging.Log;
import org.radargun.logging.LogFactory;
import org.radargun.traits.Transactional;

/**
 * @author Radim Vansa &lt;rvansa@redhat.com&gt;
 */
public class HibernateOrm5Transactional implements Transactional {
   private static final Log log = LogFactory.getLog(HibernateOrm5Transactional.class);
   private static final boolean trace = log.isTraceEnabled();
   private final HibernateOrm5Service service;

   private TransactionManager tm;
   private boolean initialized = false;
   private boolean jta = true; // TODO detect PersistenceUnitTransactionType.JTA

   public HibernateOrm5Transactional(HibernateOrm5Service service) {
      this.service = service;
   }

   private void init() {
      if (initialized) return;
      if (!service.isRunning()) {
         throw new IllegalStateException("init() can be called only when the service is started");
      }
      EntityManagerFactory emf = service.getEntityManagerFactory();
      if (jta) {
         SessionFactoryImplementor sf = emf.unwrap(SessionFactoryImplementor.class);
         JtaPlatform jtaPlatform = sf.getServiceRegistry().getService(JtaPlatform.class);
         if (jtaPlatform != null) {
            tm = jtaPlatform.retrieveTransactionManager();
         } else {
            tm = null;
         }
         if (service.transactionTimeout > 0 && tm != null) {
            try {
               tm.setTransactionTimeout(service.transactionTimeout);
            } catch (SystemException e) {
               log.error("Failed to set transaction timeout", e);
            }
         }
      }
      initialized = true;
   }

   @Override
   public Configuration getConfiguration(String resourceName) {
      return Configuration.TRANSACTIONAL;
   }

   @Override
   public Transaction getTransaction() {
      init();
      return new Transaction();
   }

   private class Transaction implements Transactional.Transaction {
      private EntityManager entityManger;

      @Override
      public <T> T wrap(T resource) {
         if (resource instanceof EntityManager) {
            this.entityManger = (EntityManager) resource;
         } else if (resource instanceof HibernateOrm5Queryable.QueryContextImpl) {
            this.entityManger = ((HibernateOrm5Queryable.QueryContextImpl) resource).entityManager;
         } else {
            throw new IllegalArgumentException("Cannot wrap " + resource);
         }
         return resource;
      }

      @Override
      public void begin() {
         if (trace) log.trace("BEGIN");
         if (jta) {
            try {
               tm.begin();
            } catch (Exception e) {
               throw new RuntimeException("Cannot begin transaction", e);
            }
            entityManger.joinTransaction();
         } else {
            entityManger.getTransaction().begin();
         }
      }

      @Override
      public void commit() {
         if (trace) log.trace("COMMIT");
         try {
            if (jta) {
               try {
                  tm.commit();
               } catch (Exception e) {
                  throw new RuntimeException("Cannot commit transaction", e);
               }
            } else {
               entityManger.getTransaction().commit();
            }
         } catch (RuntimeException e) {
            log.trace("COMMIT failed");
            throw e;
         }
      }

      @Override
      public void rollback() {
         if (trace) log.trace("ROLLBACK");
         try {
            if (jta) {
               try {
                  tm.rollback();
               } catch (Exception e) {
                  throw new RuntimeException("Cannot rollback transaction", e);
               }
            } else {
               entityManger.getTransaction().rollback();
            }
         } catch (RuntimeException e) {
            log.trace("ROLLBACK failed");
            throw e;
         }
      }
   }
}
