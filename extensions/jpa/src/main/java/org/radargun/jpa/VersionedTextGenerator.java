package org.radargun.jpa;

import java.util.Random;

import org.radargun.config.DefinitionElement;
import org.radargun.config.Property;
import org.radargun.jpa.entities.VersionedText;
import org.radargun.utils.RandomHelper;

/**
 * @author Radim Vansa &lt;rvansa@redhat.com&gt;
 */
@DefinitionElement(name = "versioned-text", doc = "Entity with simple string and version")
public class VersionedTextGenerator implements EntityGenerator<VersionedText> {
   @Property(doc = "Length of the contained string")
   int length;

   @Override
   public VersionedText create(Random random) {
      VersionedText entity = new VersionedText();
      entity.setText(RandomHelper.randomString(length, length, random));
      return entity;
   }

   @Override
   public void mutate(VersionedText entity, Random random) {
      entity.setText(RandomHelper.randomString(length, length, random));
   }

   @Override
   public VersionedText copy(VersionedText entity) {
      String text = entity.getText();
      VersionedText copy = new VersionedText();
      copy.setText(text);
      return copy;
   }

   @Override
   public Class<VersionedText> entityClass() {
      return VersionedText.class;
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
