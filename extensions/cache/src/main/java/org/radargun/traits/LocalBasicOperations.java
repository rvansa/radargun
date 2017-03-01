package org.radargun.traits;

/**
 * @author Radim Vansa &lt;rvansa@redhat.com&gt;
 */
@Trait(doc = "Basic operations that are not propagated to other clustered nodes.")
public interface LocalBasicOperations {
   <K, V> Cache<K, V> getLocalCache(String cacheName);

   interface Cache<K, V> extends BasicOperations.Cache<K, V> {
      /**
       * Determines if the key is local on given node.
       *
       * @param key
       * @return
       */
      Ownership getOwnership(Object key);
   }

   enum Ownership {
      /**
       * Entry with given key should not be stored on given node.
       */
      NON_OWNER,
      /**
       * Entry with given key should be stored on given node.
       */
      OWNER,
      /**
       * Entry with given key should be stored on given node, and this node has special role for that key.
       */
      PRIMARY,
      /**
       * Entry with given key should be stored on given node, but this node does not have any special role.
       */
      BACKUP
   }
}
