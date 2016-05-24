package org.radargun.service;

import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.EntryListener;
import com.hazelcast.core.IMap;
import com.hazelcast.core.MapEvent;
import org.radargun.traits.ContinuousQuery;
import org.radargun.traits.Query;

/**
 * @author vjuranek
 */
public class HazelcastContinuousQuery implements ContinuousQuery {

   protected final Hazelcast3Service service;

   public HazelcastContinuousQuery(Hazelcast3Service service) {
      this.service = service;
   }

   @Override
   public ListenerReference createContinuousQuery(String mapName, Query query, ContinuousQuery.Listener cqListener) {
      String id = getMap(mapName).addEntryListener(new Listener(cqListener), ((HazelcastQuery) query).getPredicate(), true);
      return new ListenerReference(id);
   }

   @Override
   public void removeContinuousQuery(String mapName, ContinuousQuery.ListenerReference listenerReference) {
      ListenerReference ref = (ListenerReference) listenerReference;
      getMap(mapName).removeEntryListener(ref.id);
   }

   protected IMap<Object, Object> getMap(String mapName) {
      return service.getMap(mapName);
   }

   public static class Listener implements EntryListener {

      private final ContinuousQuery.Listener cqListener;

      public Listener(ContinuousQuery.Listener cqListener) {
         this.cqListener = cqListener;
      }

      @Override
      public void entryAdded(EntryEvent entryEvent) {
         cqListener.onEntryJoined(entryEvent.getKey(), entryEvent.getValue());
      }

      @Override
      public void entryRemoved(EntryEvent entryEvent) {
         cqListener.onEntryLeft(entryEvent.getKey());
      }

      @Override
      public void entryUpdated(EntryEvent entryEvent) {
         //TODO check, if this is correct
         cqListener.onEntryJoined(entryEvent.getKey(), entryEvent.getValue());
      }

      @Override
      public void entryEvicted(EntryEvent entryEvent) {
         cqListener.onEntryLeft(entryEvent.getKey());
      }

      @Override
      public void mapCleared(MapEvent mapEvent) {
      }

      @Override
      public void mapEvicted(MapEvent mapEvent) {
      }
   }

   private static class ListenerReference implements ContinuousQuery.ListenerReference {
      final String id;

      private ListenerReference(String id) {
         this.id = id;
      }
   }
}
