package org.radargun.service;

import org.radargun.Service;
import org.radargun.traits.CacheListeners;
import org.radargun.traits.ProvidesTrait;

/**
 * @author Radim Vansa &lt;rvansa@redhat.com&gt;
 */
@Service(doc = InfinispanEmbeddedService.SERVICE_DESCRIPTION)
public class Infinispan70EmbeddedService extends Infinispan60EmbeddedService {
   @ProvidesTrait
   public CacheListeners createListeners() {
      return new InfinispanCacheListeners(this);
   }
}
