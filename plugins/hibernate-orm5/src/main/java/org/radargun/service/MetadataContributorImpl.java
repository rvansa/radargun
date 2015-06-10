package org.radargun.service;

import org.hibernate.boot.spi.InFlightMetadataCollector;
import org.hibernate.boot.spi.MetadataContributor;
import org.jboss.jandex.IndexView;

/**
 * @author Radim Vansa &lt;rvansa@redhat.com&gt;
 */
public class MetadataContributorImpl implements MetadataContributor {
   @Override
   public void contribute(InFlightMetadataCollector metadataCollector, IndexView jandexIndex) {
      HibernateOrm5Service.instance.updateMetadata(metadataCollector);
   }
}
