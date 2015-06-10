package org.radargun.jpa.entities;

import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;

import org.hibernate.annotations.Cache;
import org.hibernate.annotations.CacheConcurrencyStrategy;

/**
 * Read-only cached entity
 *
 * @author Radim Vansa &lt;rvansa@redhat.com&gt;
 */
@Entity
@Cache(usage = CacheConcurrencyStrategy.TRANSACTIONAL)
public class Mutable {
   @Id
   @GeneratedValue
   long id;

   String value;

   public Mutable() {
   }

   public Mutable(String value) {
      this.value = value;
   }

   public long getId() {
      return id;
   }

   public String getValue() {
      return value;
   }

   public void setValue(String value) {
      this.value = value;
   }

   @Override
   public String toString() {
      final StringBuilder sb = new StringBuilder("Mutable{");
      sb.append("id=").append(id);
      sb.append(", value='").append(value).append('\'');
      sb.append('}');
      return sb.toString();
   }
}
