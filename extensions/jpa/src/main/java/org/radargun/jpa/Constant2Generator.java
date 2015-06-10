package org.radargun.jpa;

import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

import org.radargun.config.DefinitionElement;
import org.radargun.config.Property;
import org.radargun.jpa.entities.Constant2;
import org.radargun.utils.RandomHelper;

/**
 * @author Radim Vansa &lt;rvansa@redhat.com&gt;
 */
@DefinitionElement(name = "constant2", doc = "Immutable entity with simple string")
public class Constant2Generator implements EntityGenerator<Constant2> {
   @Property(doc = "Length of the contained string")
   int length;

   @Property(doc = "Offset for the stride.", optional = false)
   long strideIndex;

   @Property(doc = "Size of step in generated identifiers.", optional = false)
   long strideSize;

   protected AtomicLong counter = new AtomicLong();

   protected long nextId() {
      return counter.incrementAndGet() * strideSize + strideIndex;
   }

   @Override
   public Constant2 create(Random random) {
      return new Constant2(nextId(), RandomHelper.randomString(length, length));
   }

   @Override
   public void mutate(Constant2 entity, Random random) {
      throw new UnsupportedOperationException();
   }

   @Override
   public Constant2 copy(Constant2 entity) {
      return new Constant2(nextId(), entity.getValue());
   }

   @Override
   public Class<Constant2> entityClass() {
      return Constant2.class;
   }

   @Override
   public boolean hasGeneratedId() {
      return false;
   }

   @Override
   public boolean hasForeignKeys() {
      return false;
   }
}
