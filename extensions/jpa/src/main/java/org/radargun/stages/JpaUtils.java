package org.radargun.stages;

import java.util.List;
import java.util.Set;
import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.criteria.CriteriaBuilder;
import javax.persistence.criteria.CriteriaDelete;
import javax.persistence.criteria.CriteriaQuery;
import javax.persistence.criteria.Path;
import javax.persistence.criteria.Root;
import javax.persistence.metamodel.EntityType;
import javax.persistence.metamodel.SingularAttribute;

import org.radargun.logging.Log;
import org.radargun.logging.LogFactory;
import org.radargun.traits.JpaProvider;
import org.radargun.traits.Transactional;

/**
 * Common JPA operations
 *
 * @author Radim Vansa &lt;rvansa@redhat.com&gt;
 */
public class JpaUtils {
   private static final Log log = LogFactory.getLog(JpaUtils.class);

   private JpaUtils() {}

   public static int getNumEntries(EntityManagerFactory entityManagerFactory, Transactional transactional, Class entityClazz) {
      EntityManager entityManager = entityManagerFactory.createEntityManager();
      Transactional.Transaction tx = transactional.getTransaction();
      tx.wrap(entityManager);
      tx.begin();
      try {
         CriteriaBuilder cb = entityManager.getCriteriaBuilder();
         CriteriaQuery<Long> query = cb.createQuery(Long.class);
         Root root = query.from(entityClazz);
         query = query.select(cb.count(root));
         return entityManager.createQuery(query).getSingleResult().intValue();
      } finally {
         tx.commit();
         entityManager.close();
      }
   }

   public static void dropEntities(JpaProvider jpaProvider, Transactional transactional, Class<?> targetEntity) {
      EntityManagerFactory emf = jpaProvider.getEntityManagerFactory();
      CriteriaBuilder cb = emf.getCriteriaBuilder();
      EntityManager em = emf.createEntityManager();
      Transactional.Transaction tx = transactional.getTransaction();
      tx.wrap(em);
      tx.begin();
      try {
         CriteriaDelete criteriaDelete = cb.createCriteriaDelete(targetEntity);
         criteriaDelete.from(targetEntity);
         em.createQuery(criteriaDelete).executeUpdate();
         tx.commit();
      } finally {
         em.close();
      }
   }

   public static <T> void scroll(EntityManagerFactory entityManagerFactory, Transactional transactional, EntityType<T> entityType, int batchSize, EntityConsumer<T> consumer) {
      final Class<?> idClass = entityType.getIdType().getJavaType();
      if (idClass.isPrimitive()) {
         if (idClass.equals(boolean.class) || idClass.equals(char.class)) {
            log.errorf("Cannot list entities without numeric ID; %s has %s", entityType.getJavaType().getName(), idClass.getName());
            return;
         }
      } else if (!Number.class.isAssignableFrom(idClass)) {
         log.errorf("Cannot list entities without numeric ID; %s has %s", entityType.getJavaType().getName(), idClass.getName());
         return;
      }
      SingularAttribute idAttribute = null;
      Set<SingularAttribute<? super T, ?>> singularAttributes = entityType.getSingularAttributes();
      for (SingularAttribute singularAttribute : singularAttributes) {
         if (singularAttribute.isId()) {
            idAttribute = singularAttribute;
            break;
         }
      }
      if (idAttribute == null) {
         log.error("Did not found ID attribute for " + entityType.getJavaType().getName());
         return;
      }
      Number maxId = null;
      EntityManager em = entityManagerFactory.createEntityManager();
      try {
         boolean hasMoreEntries = true;
         while (hasMoreEntries) {
            Transactional.Transaction tx = transactional.getTransaction();
            em = tx.wrap(em);
            tx.begin();
            try {
               CriteriaBuilder cb = em.getCriteriaBuilder();
               CriteriaQuery<T> query = cb.createQuery(entityType.getJavaType());
               Root<?> root = query.from(entityType);
               Path idPath = root.get(idAttribute);
               query.orderBy(cb.asc(idPath));
               if (maxId != null) {
                  query.where(cb.gt(idPath, maxId));
               }
               List<T> list = em.createQuery(query).setMaxResults(batchSize).getResultList();
               for (T entity : list) {
                  consumer.accept(entity);
               }
               if (!list.isEmpty()) {
                  maxId = (Number) entityManagerFactory.getPersistenceUnitUtil().getIdentifier(list.get(list.size() - 1));
               }
               if (list.size() < batchSize) {
                  hasMoreEntries = false;
               }
               tx.commit();
               tx = null;
            } finally {
               if (tx != null) {
                  tx.rollback();
               }
            }
         }
      } finally{
         em.close();
      }
   }

   public interface EntityConsumer<T> {
      void accept(T entity);
   }
}
