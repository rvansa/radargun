package org.radargun.jpa;

import java.util.Random;

import org.radargun.config.DefinitionElement;
import org.radargun.config.Property;
import org.radargun.jpa.entities.Mutable;
import org.radargun.utils.RandomHelper;

/**
 * @author Radim Vansa &lt;rvansa@redhat.com&gt;
 */
@DefinitionElement(name = "mutable", doc = "Mutable entity with simple string")
public class MutableGenerator implements EntityGenerator<Mutable> {
   @Property(doc = "Length of the contained string")
   int length;

   @Override
   public Mutable create(Random random) {
      return new Mutable(RandomHelper.randomString(length, length, random));
   }

   @Override
   public void mutate(Mutable entity, Random random) {
      entity.setValue(RandomHelper.randomString(length, length, random));
   }

   @Override
   public Mutable copy(Mutable entity) {
      return new Mutable(entity.getValue());
   }

   @Override
   public Class<Mutable> entityClass() {
      return Mutable.class;
   }

   @Override
   public boolean hasGeneratedId() {
      return true;
   }

   @Override
   public boolean hasForeignKeys() {
      return false;
   }
}
