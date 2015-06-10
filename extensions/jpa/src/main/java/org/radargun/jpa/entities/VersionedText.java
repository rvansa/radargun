package org.radargun.jpa.entities;

import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.Version;

/**
 * @author Radim Vansa &lt;rvansa@redhat.com&gt;
 */
@Entity
public class VersionedText {
   @Id
   @GeneratedValue
   public long id;

   public String text;

   @Version
   public long version;

   public long getId() {
      return id;
   }

   public void setId(long id) {
      this.id = id;
   }

   public String getText() {
      return text;
   }

   public void setText(String text) {
      this.text = text;
   }

   public long getVersion() {
      return version;
   }

   public void setVersion(long version) {
      this.version = version;
   }

   @Override
   public String toString() {
      final StringBuilder sb = new StringBuilder("VersionedString{");
      sb.append("id=").append(id);
      sb.append(", text='").append(text).append('\'');
      sb.append(", version=").append(version);
      sb.append('}');
      return sb.toString();
   }
}
