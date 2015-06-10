package org.radargun.jpa.entities;

import java.util.Random;

import javax.persistence.Column;
import javax.persistence.Embedded;
import javax.persistence.Entity;
import javax.persistence.Id;

import org.radargun.jpa.GeneratorHelper;

/**
 * @author Radim Vansa &lt;rvansa@redhat.com&gt;
 */
@Entity
public class EmbeddedContentEntity {
   @Id
   public String id;
   @Column(length = 65535)
   public String description;
   @Embedded
   public EmbeddableClass content;

   public EmbeddedContentEntity() {
   }

   public EmbeddedContentEntity(Object id, int size, Random random) {
      this.id = (String) id;
      description = GeneratorHelper.getRandomString(size / 2, random);
      content = new EmbeddableClass(GeneratorHelper.getRandomString(size - size / 2, random));
   }
}
