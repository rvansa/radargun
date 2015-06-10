package org.radargun.service;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.persistence.EntityManager;
import javax.persistence.TypedQuery;
import javax.persistence.criteria.CriteriaBuilder;
import javax.persistence.criteria.CriteriaQuery;
import javax.persistence.criteria.Expression;
import javax.persistence.criteria.Order;
import javax.persistence.criteria.Path;
import javax.persistence.criteria.Predicate;
import javax.persistence.criteria.Root;
import javax.persistence.criteria.Selection;

import org.radargun.traits.Query;
import org.radargun.traits.Queryable;

/**
 * @author Radim Vansa &lt;rvansa@redhat.com&gt;
 */
public class HibernateOrm5Queryable implements Queryable {
   private final HibernateOrm5Service service;

   public HibernateOrm5Queryable(HibernateOrm5Service service) {
      this.service = service;
   }

   @Override
   public QueryBuilderImpl getBuilder(String containerName, Class<?> clazz) {
      return new QueryBuilderImpl(service.getEntityManagerFactory().getCriteriaBuilder(), clazz);
   }

   @Override
   public Query.Context createContext(String containerName) {
      return new QueryContextImpl(service.getEntityManagerFactory().createEntityManager());
   }

   @Override
   public void reindex(String containerName) {
   }

   protected static class QueryBuilderImpl implements Query.Builder {
      private final CriteriaBuilder cb;
      private final CriteriaQuery<Object> query;
      private final Root<?> root;
      private final List<Predicate> predicates = new ArrayList<>();
      private final List<Order> orderList = new ArrayList<>();
      private Query.SelectExpression[] projection;
      private List<Expression<?>> groupBy;
      private long offset = -1;
      private long limit = -1;
      private boolean built = false;

      public QueryBuilderImpl(CriteriaBuilder cb, Class<?> clazz) {
         this.cb = cb;
         this.query = this.cb.createQuery();
         this.root = this.query.from(clazz);
      }

      protected QueryBuilderImpl(CriteriaBuilder cb, Root<?> root) {
         this.cb = cb;
         this.root = root;
         this.query = null;
      }

      private void checkNotBuilt() {
         if (built) throw new IllegalStateException("Already built");
      }

      @Override
      public QueryBuilderImpl subquery() {
         return new QueryBuilderImpl(cb, root);
      }

      @Override
      public QueryBuilderImpl eq(Query.SelectExpression expression, Object value) {
         checkNotBuilt();
         predicates.add(cb.equal(root.get(expression.attribute), value));
         return this;
      }

      @Override
      public QueryBuilderImpl lt(Query.SelectExpression expression, Object value) {
         checkNotBuilt();
         predicates.add(cb.lt(root.get(expression.attribute), (Number) value));
         return this;
      }

      @Override
      public QueryBuilderImpl le(Query.SelectExpression expression, Object value) {
         checkNotBuilt();
         predicates.add(cb.le(root.get(expression.attribute), (Number) value));
         return this;
      }

      @Override
      public QueryBuilderImpl gt(Query.SelectExpression expression, Object value) {
         checkNotBuilt();
         predicates.add(cb.gt(root.get(expression.attribute), (Number) value));
         return this;
      }

      @Override
      public QueryBuilderImpl ge(Query.SelectExpression expression, Object value) {
         checkNotBuilt();
         predicates.add(cb.ge(root.get(expression.attribute), (Number) value));
         return this;
      }

      @Override
      public QueryBuilderImpl between(Query.SelectExpression expression, Object lowerBound, boolean lowerInclusive, Object upperBound, boolean upperInclusive) {
         checkNotBuilt();
         if (!lowerInclusive || !upperInclusive)
            throw new IllegalArgumentException("JPA does support only inclusive BETWEEN");
         predicates.add(cb.between(root.get(expression.attribute), (Comparable) lowerBound, (Comparable) upperBound));
         return this;
      }

      @Override
      public QueryBuilderImpl isNull(Query.SelectExpression expression) {
         checkNotBuilt();
         predicates.add(cb.isNull(root.get(expression.attribute)));
         return this;
      }

      @Override
      public QueryBuilderImpl like(Query.SelectExpression expression, String pattern) {
         checkNotBuilt();
         predicates.add(cb.like(root.get(expression.attribute), pattern));
         return this;
      }

      @Override
      public QueryBuilderImpl contains(Query.SelectExpression expression, Object value) {
         checkNotBuilt();
         predicates.add(cb.in(root.get(expression.attribute)).value(value));
         return this;
      }

      @Override
      public QueryBuilderImpl not(Query.Builder subquery) {
         checkNotBuilt();
         QueryBuilderImpl qb = (QueryBuilderImpl) subquery;
         Predicate p = qb.toPredicate();
         predicates.add(cb.not(p));
         return this;
      }

      @Override
      public QueryBuilderImpl any(Query.Builder... subqueries) {
         checkNotBuilt();
         Predicate[] ps = new Predicate[subqueries.length];
         for (int i = 0; i < subqueries.length; ++i) {
            ps[i] = ((QueryBuilderImpl) subqueries[i]).toPredicate();
         }
         predicates.add(cb.or(ps));
         return this;
      }

      @Override
      public QueryBuilderImpl orderBy(Query.SelectExpression expression) {
         checkNotBuilt();
         if (expression.asc) {
            orderList.add(cb.asc(root.get(expression.attribute)));
            return this;
         } else {
            orderList.add(cb.desc(root.get(expression.attribute)));
            return this;
         }
      }

      @Override
      public QueryBuilderImpl projection(Query.SelectExpression... expressions) {
         checkNotBuilt();
         if (expressions == null || expressions.length == 0) {
            throw new IllegalArgumentException(Arrays.toString(expressions));
         }
         projection = expressions;
         return this;
      }

      @Override
      public Query.Builder groupBy(String[] attributes) {
         checkNotBuilt();
         if (attributes == null || attributes.length == 0) {
            throw new IllegalArgumentException(Arrays.toString(attributes));
         }
         this.groupBy = Stream.of(attributes).map(root::get).collect(Collectors.toList());
         return this;
      }

      @Override
      public QueryBuilderImpl offset(long offset) {
         checkNotBuilt();
         if (offset < 0) throw new IllegalArgumentException(String.valueOf(offset));
         this.offset = offset;
         return this;
      }

      @Override
      public QueryBuilderImpl limit(long limit) {
         checkNotBuilt();
         if (limit <= 0) throw new IllegalArgumentException(String.valueOf(limit));
         this.limit = limit;
         return this;
      }

      @Override
      public QueryImpl build() {
         if (query == null) throw new IllegalArgumentException("This is not the top-level QueryBuilder");
         if (!built) {
            if (!predicates.isEmpty()) query.where(toPredicate());
            if (!orderList.isEmpty()) query.orderBy(orderList);
            if (groupBy != null) {
               query.groupBy(this.groupBy);
            }
            if (projection != null) {
               if (projection.length == 1) {
                  query.select(selectExpressionToSelection(projection[0]));
               } else {
                  query.multiselect(Stream.of(projection).map(this::selectExpressionToSelection).toArray(Selection[]::new));
               }
            } else {
               query.select(root);
            }
            built = true;
         }
         return new QueryImpl(query, offset, limit);
      }

      protected Selection<?> selectExpressionToSelection(Query.SelectExpression expression) {
         Path<?> attribute = root.get(expression.attribute);
         switch (expression.function) {
            case NONE:
               return attribute;
            case COUNT:
               return cb.count(attribute);
            case SUM:
               return cb.sum((Path<? extends Number>) attribute);
            case AVG:
               return cb.avg((Path<? extends Number>) attribute);
            case MIN:
               return cb.min((Path<? extends Number>) attribute);
            case MAX:
               return cb.max((Path<? extends Number>) attribute);
            default:
               throw new IllegalArgumentException("Unknown function " + expression.function);
         }
      }

      protected Predicate toPredicate() {
         if (predicates.size() == 0) {
            throw new IllegalArgumentException("Empty subquery");
         } else if (predicates.size() == 1) {
            return predicates.get(0);
         } else {
            return cb.and(predicates.toArray(new Predicate[predicates.size()]));
         }
      }
   }

   protected static class QueryContextImpl implements Query.Context {
      protected final EntityManager entityManager;

      public QueryContextImpl(EntityManager entityManager) {
         this.entityManager = entityManager;
      }

      @Override
      public void close() {
         entityManager.close();
      }
   }

   protected static class QueryImpl implements Query {
      private final CriteriaQuery query;
      private final long offset;
      private final long limit;

      public QueryImpl(CriteriaQuery<Object> query, long offset, long limit) {
         this.query = query;
         this.offset = offset;
         this.limit = limit;
      }

      @Override
      public QueryResultImpl execute(Query.Context context) {
         EntityManager entityManager = ((QueryContextImpl) context).entityManager;
         TypedQuery query = entityManager.createQuery(this.query);
         query.unwrap(org.hibernate.Query.class).setCacheable(true);
         if (offset > 0) query.setFirstResult((int) offset);
         if (limit > 0) query.setMaxResults((int) limit);
         return new QueryResultImpl(query.getResultList());
      }
   }

   protected static class QueryResultImpl implements Query.Result {
      private final List resultList;

      public QueryResultImpl(List resultList) {
         this.resultList = resultList;
      }

      @Override
      public int size() {
         return resultList.size();
      }

      @Override
      public Collection values() {
         return resultList;
      }
   }
}
