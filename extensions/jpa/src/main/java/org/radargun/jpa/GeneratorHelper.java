package org.radargun.jpa;

import java.util.Random;

public class GeneratorHelper {
   private GeneratorHelper() {}

   public static String getRandomString(int size, Random random) {
      StringBuilder sb = new StringBuilder(size);
      for (int i = 0; i < size; ++i) {
         sb.append((char) (random.nextInt(26) + 'A'));
      }
      return sb.toString();
   }
}
