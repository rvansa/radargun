package org.radargun.jpa.entities;

import java.util.Random;

import javax.persistence.Column;
import javax.persistence.EmbeddedId;
import javax.persistence.Entity;

import org.radargun.jpa.GeneratorHelper;

/**
 * @author Radim Vansa &lt;rvansa@redhat.com&gt;
 */
@Entity
public class EmbeddedIdEntity {
   @EmbeddedId
   public EmbeddableId embeddableId;
   @Column(length = 65535)
   public String description;

   public EmbeddedIdEntity() {
   }

   public EmbeddedIdEntity(Object id, int size, Random random) {
      embeddableId = (EmbeddableId) id;
      description = GeneratorHelper.getRandomString(size, random);
   }
}
