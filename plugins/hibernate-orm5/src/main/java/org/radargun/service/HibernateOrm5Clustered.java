package org.radargun.service;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.hibernate.cache.infinispan.InfinispanRegionFactory;
import org.hibernate.cache.spi.RegionFactory;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachemanagerlistener.annotation.Merged;
import org.infinispan.notifications.cachemanagerlistener.annotation.ViewChanged;
import org.infinispan.notifications.cachemanagerlistener.event.MergeEvent;
import org.infinispan.notifications.cachemanagerlistener.event.ViewChangedEvent;
import org.infinispan.remoting.transport.Address;
import org.radargun.traits.Clustered;


/**
 * @author Radim Vansa &lt;rvansa@redhat.com&gt;
 */
@Listener
public class HibernateOrm5Clustered implements Clustered {
   protected final List<Membership> membershipHistory = new ArrayList<Membership>();
   private final HibernateOrm5Service service;

   public HibernateOrm5Clustered(HibernateOrm5Service service) {
      this.service = service;
   }

   void register() {
      EmbeddedCacheManager cacheManager = getCacheManager();
      if (cacheManager != null) {
         cacheManager.addListener(this);
      }
      synchronized (this) {
         membershipHistory.add(Membership.create(convert(cacheManager.getTransport().getMembers())));
      }
   }

   void unregister() {
      EmbeddedCacheManager cacheManager = getCacheManager();
      if (cacheManager != null) {
         cacheManager.removeListener(this);
      }
   }

   @Override
   public boolean isCoordinator() {
      EmbeddedCacheManager cacheManager = getCacheManager();
      if (cacheManager == null) {
         return false;
      } else {
         return cacheManager.getTransport().isCoordinator();
      }
   }

   @ViewChanged
   public synchronized void viewChanged(ViewChangedEvent e) {
      membershipHistory.add(Membership.create(convert(e.getNewMembers())));
   }

   @Merged
   public synchronized void merged(MergeEvent e) {
      membershipHistory.add(Membership.create(convert(e.getNewMembers())));
   }

   public synchronized void stopped() {
      membershipHistory.add(Membership.empty());
   }

   @Override
   public synchronized Collection<Member> getMembers() {
      if (membershipHistory.isEmpty()) return null;
      return membershipHistory.get(membershipHistory.size() - 1).members;
   }

   private Collection<Member> convert(List<Address> addresses) {
      EmbeddedCacheManager cacheManager = getCacheManager();
      if (cacheManager == null) {
         throw new IllegalStateException();
      }
      Collection<Member> members = new ArrayList<>(addresses.size());
      boolean coord = true;
      for (Address address : addresses) {
         members.add(new Member(address.toString(), cacheManager.getAddress().equals(address), coord));
         coord = false;
      }
      return members;
   }

   private EmbeddedCacheManager getCacheManager() {
      RegionFactory regionFactory = service.getRegionFactory();
      if (regionFactory instanceof InfinispanRegionFactory) {
         return ((InfinispanRegionFactory) regionFactory).getCacheManager();
      }
      return null;
   }

   @Override
   public synchronized List<Membership> getMembershipHistory() {
      return new ArrayList<>(membershipHistory);
   }
}
