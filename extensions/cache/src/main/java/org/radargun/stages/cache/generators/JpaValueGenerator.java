package org.radargun.stages.cache.generators;

import java.io.Serializable;
import java.lang.annotation.Annotation;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Random;

import org.radargun.config.DefinitionElement;
import org.radargun.config.Init;
import org.radargun.config.Property;
import org.radargun.logging.Log;
import org.radargun.logging.LogFactory;

/**
 * @author Radim Vansa &lt;rvansa@redhat.com&gt;
 */
@DefinitionElement(name = "jpa", doc = "Instantiates JPA entities. The constructor for the entities must match to the generateValue() method.")
public class JpaValueGenerator implements ValueGenerator {
   protected static Log log = LogFactory.getLog(JpaValueGenerator.class);

   @Property(name = "class", doc = "Fully qualified name of the value class.", optional = false)
   private String clazzName;

   private Class<?> clazz;
   private Class<? extends Annotation> entityClazz;
   // TODO: replace these with MethodHandles
   private Constructor<?> ctor;
   private Method size;
   private Method check;

   @Init
   public void init() {
      try {
         entityClazz = (Class<? extends Annotation>) Class.forName("javax.persistence.Entity");
         clazz = Class.forName(clazzName);
         if (!clazz.isAnnotationPresent(entityClazz)) {
            throw new IllegalArgumentException("Class " + clazz.getName() + " is not an entity - no @Entity present");
         }
         ctor = clazz.getConstructor(Object.class, int.class, Random.class);
      } catch (Exception e) {
         // trace as this can happen on master node
         log.trace("Could not initialize generator " + this, e);
      }
      try {
         size = clazz.getMethod("size");
      } catch (NoSuchMethodException e) {
         log.trace("Cannot find method size() on " + entityClazz.getName(), e);
      }
      try {
         check = clazz.getMethod("check", int.class);
      } catch (NoSuchMethodException e) {
         log.trace("Cannot find method check(int) on " + entityClazz.getName(), e);
      }
   }

   @Override
   public Object generateValue(Object key, int size, Random random) {
      if (ctor == null) throw new IllegalStateException("The generator was not properly initialized");
      try {
         return ctor.newInstance(key, size, random);
      } catch (Exception e) {
         throw new IllegalStateException(e);
      }
   }

   @Override
   public int sizeOf(Object value) {
      if (size != null) {
         try {
            return ((Integer) size.invoke(value)).intValue();
         } catch (IllegalAccessException | InvocationTargetException e) {
            throw new RuntimeException(e);
         }
      }
      throw new UnsupportedOperationException();
   }

   @Override
   public boolean checkValue(Object value, Object key, int expectedSize) {
      if (clazz == null) throw new IllegalStateException("The generator was not properly initialized");
      if (!clazz.isInstance(value)) {
         return false;
      }
      if (check != null) {
         try {
            return ((Boolean) check.invoke(value, expectedSize)).booleanValue();
         } catch (IllegalAccessException | InvocationTargetException e) {
            throw new RuntimeException(e);
         }
      }
      return true;
   }



   public abstract static class JpaValue implements Serializable {
      public int size() {
         throw new UnsupportedOperationException();
      }

      public boolean check(int expectedSize) {
         return true;
      }
   }

}
