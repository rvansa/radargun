package org.radargun.service;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.persistence.Entity;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;
import javax.persistence.SharedCacheMode;

import org.hibernate.FlushMode;
import org.hibernate.annotations.CacheConcurrencyStrategy;
import org.hibernate.boot.spi.InFlightMetadataCollector;
import org.hibernate.c3p0.internal.C3P0ConnectionProvider;
import org.hibernate.cache.ehcache.EhCacheRegionFactory;
import org.hibernate.cache.infinispan.InfinispanRegionFactory;
import org.hibernate.cache.infinispan.impl.BaseRegion;
import org.hibernate.cache.spi.Region;
import org.hibernate.cache.spi.RegionFactory;
import org.hibernate.cache.spi.access.AccessType;
import org.hibernate.cfg.AvailableSettings;
import org.hibernate.cfg.Environment;
import org.hibernate.dialect.H2Dialect;
import org.hibernate.dialect.PostgreSQL94Dialect;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.hikaricp.internal.HikariCPConnectionProvider;
import org.hibernate.mapping.PersistentClass;
import org.infinispan.context.Flag;
import org.radargun.Service;
import org.radargun.config.ClasspathScanner;
import org.radargun.config.DefinitionElement;
import org.radargun.config.DocumentedValue;
import org.radargun.config.Property;
import org.radargun.config.XmlConverter;
import org.radargun.logging.Log;
import org.radargun.logging.LogFactory;
import org.radargun.traits.InternalsExposition;
import org.radargun.traits.JpaProvider;
import org.radargun.traits.Lifecycle;
import org.radargun.traits.ProvidesTrait;
import org.radargun.traits.Queryable;
import org.radargun.traits.Transactional;
import org.radargun.utils.ReflexiveConverters;

/**
 * @author Radim Vansa &lt;rvansa@redhat.com&gt;
 */
@Service(doc = "Service for Hibernate ORM 5.x")
public class HibernateOrm5Service implements Lifecycle, JpaProvider {
   // hack to enable to read configuration from services
   public static HibernateOrm5Service instance;
   private static Log log = LogFactory.getLog(HibernateOrm5Service.class);
   private final HibernateOrm5Clustered clustered;

   protected JacamarHelper jacamarHelper = new JacamarHelper();
   protected JndiHelper jndiHelper = new JndiHelper();
   protected JtaHelper jtaHelper = new JtaHelper();

   @Property(doc = "JTA transaction timeout, in seconds.")
   protected int transactionTimeout = -1;

   @Property(doc = "Persistence unit to be used. Default is 'default'")
   private String persistenceUnit;

   @Property(doc = "Flush mode. By default not set.")
   private FlushMode flushMode;

   @Property(doc = "HBM -> DDL settings. By default not set.")
   private Hbm2DdlMode hbm2ddlAuto;

   @Property(doc = "Show SQL commands. By default not set.")
   private Boolean showSql;

   @Property(doc = "Additional properties to be passed to the entity manager factory.", complexConverter = Prop.Converter.class)
   private List<Prop> properties = Collections.EMPTY_LIST;

   @Property(doc = "Database", complexConverter = Database.Converter.class, optional = false)
   private Database database;

   @Property(doc = "Connection pool. Default is the native hibernate implementation.", complexConverter = ConnectionPoolConverter.class)
   private ConnectionPool connectionPool = new DefaultConnectionPool();

   @Property(doc = "Second level/query caching settings. By default not set.", complexConverter = Cache.Converter.class)
   private Cache cache;

   @Property(doc = "Enable statistics collection. By default not set.")
   private Boolean generateStatistics;

   @Property(doc = "Log statistics for each session. By default not set")
   private Boolean logSessionMetrics;

   private volatile boolean running;
   // we can have only one instance, since if Infinispan would be used as cache,
   // two cache managers would use the same JMX domain and fail with default configuration
   private EntityManagerFactory entityManagerFactory;

   public HibernateOrm5Service() {
      instance = this;
      clustered = createClustered();
   }

   @ProvidesTrait
   public Lifecycle getLifecycle() {
      return this;
   }

   @ProvidesTrait
   public JpaProvider getJpaProvider() {
      return this;
   }

   @ProvidesTrait
   public Transactional createTransactional() {
      return new HibernateOrm5Transactional(this);
   }

   @ProvidesTrait
   public InternalsExposition createInternalsExposition() {
      return new HibernateOrm5InternalsExposition(this);
   }

   @ProvidesTrait
   public Queryable createQueryable() {
      return new HibernateOrm5Queryable(this);
   }

   protected HibernateOrm5Clustered createClustered() {
      return new HibernateOrm5Clustered(this);
   }

   @ProvidesTrait
   public HibernateOrm5Clustered getClustered() {
      return clustered;
   }

   @Override
   public void start() {
      try {
         if (connectionPool instanceof IronJacamar) {
            String datasourceDefinitions = ((IronJacamar) connectionPool).datasourceDefinitions;
            File datasources = File.createTempFile("datasources-", "-ds.xml");
            datasources.deleteOnExit();
            Files.write(datasources.toPath(), datasourceDefinitions.getBytes(), StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
            jacamarHelper.start(Collections.EMPTY_LIST, Collections.singletonList(datasources.toURI().toURL()));
         } else {
            jndiHelper.start();
            jtaHelper.start();
         }
         entityManagerFactory = createEntityManagerFactory();
         clustered.register();
         running = true;
      } catch (Throwable throwable) {
         throw new RuntimeException("Failed to start service", throwable);
      }
   }

   @Override
   public void stop() {
      try {
         clustered.unregister();
         if (entityManagerFactory != null && entityManagerFactory.isOpen()) {
            entityManagerFactory.close();
         }
         if (connectionPool instanceof IronJacamar) {
            jacamarHelper.stop();
         } else {
            jtaHelper.stop();
            jndiHelper.stop();
         }
         running = false;
      } catch (Throwable throwable) {
         throw new RuntimeException("Failed to stop service", throwable);
      }
   }

   @Override
   public boolean isRunning() {
      return running;
   }

   @Override
   public EntityManagerFactory getEntityManagerFactory() {
      return entityManagerFactory;
   }

   @Override
   public SecondLevelCacheStatistics getSecondLevelCacheStatistics(String cacheName) {
      SessionFactoryImplementor sfi = entityManagerFactory.unwrap(SessionFactoryImplementor.class);
      org.hibernate.stat.SecondLevelCacheStatistics stats = sfi.getStatisticsImplementor().getSecondLevelCacheStatistics(cacheName);
      return new SecondLevelCacheStatistics(stats.getHitCount(), stats.getMissCount(), stats.getPutCount(),
            stats.getElementCountInMemory(), stats.getElementCountOnDisk());
   }

   @Override
   public <T> Collection<T> getSecondLevelCacheEntities(String name) {
      SessionFactoryImplementor sfi = entityManagerFactory.unwrap(SessionFactoryImplementor.class);
      Region region = sfi.getSecondLevelCacheRegion(name);
      if (region instanceof BaseRegion) {
         // we don't want to retrieve remote entries here
         return ((BaseRegion) region).getCache().withFlags(Flag.CACHE_MODE_LOCAL).entrySet();
      } else {
         throw new IllegalArgumentException("Cannot retrieve entities, region for " + name + " is " + region);
      }
   }

   private EntityManagerFactory createEntityManagerFactory() {
      Map<String, Object> propertyMap = new HashMap<>();
      database.applyProperties(propertyMap);
      connectionPool.applyProperties(propertyMap, database);
      if (hbm2ddlAuto != null)
         propertyMap.put(AvailableSettings.HBM2DDL_AUTO, hbm2ddlAuto.getValue());
      if (showSql != null)
         propertyMap.put(AvailableSettings.SHOW_SQL, showSql.toString());
      if (flushMode != null)
         propertyMap.put("org.hibernate.flushMode", flushMode.toString());
      if (cache != null) {
         cache.applyProperties(propertyMap);
      } else {
         propertyMap.put(AvailableSettings.USE_SECOND_LEVEL_CACHE, Boolean.FALSE.toString());
      }
      if (generateStatistics != null) {
         propertyMap.put(AvailableSettings.GENERATE_STATISTICS, generateStatistics.toString());
      }
      if (logSessionMetrics != null) {
         propertyMap.put(AvailableSettings.LOG_SESSION_METRICS, logSessionMetrics.toString());
      }
      for (Prop p : properties) {
         Object previous = propertyMap.put(p.name, p.value);
         if (previous != null && !previous.equals(p.value)) {
            log.warnf("Overriding property '%s' -> '%s' with '%s'", p.name, previous, p.value);
         }
      }
      List<Class<?>> entityClasses = new ArrayList<>();
      ClasspathScanner.scanClasspath(Object.class, Entity.class, "org.radargun.jpa.entities", entityClasses::add);
      log.debug("Persistence properties: " + propertyMap);
      propertyMap.put(org.hibernate.jpa.AvailableSettings.LOADED_CLASSES, entityClasses);
      return Persistence.createEntityManagerFactory(persistenceUnit, propertyMap);
   }

   public RegionFactory getRegionFactory() {
      return getEntityManagerFactory().unwrap(SessionFactoryImplementor.class).getServiceRegistry().getService(RegionFactory.class);
   }

   public void updateMetadata(InFlightMetadataCollector metadataCollector) {
      if (cache != null && cache.cacheSettingsOverrides != null) {
         for (CacheSettingsOverride override : cache.cacheSettingsOverrides) {
            if (override.strategy != null) {
               AccessType accessType = override.strategy.toAccessType();
               PersistentClass entityBinding = metadataCollector.getEntityBinding(override.clazz);
               if (entityBinding == null) {
                  throw new IllegalArgumentException("Entity " + override.clazz + " does not have entity binding.");
               }
               entityBinding.getRootClass()
                     .setCacheConcurrencyStrategy(accessType != null ? accessType.getExternalName() : null);
            }
         }
      }
   }

   protected enum Hbm2DdlMode {
      @DocumentedValue("Validate the schema, make no changes to the database.")
      VALIDATE("validate"),
      @DocumentedValue("Update the schema.")
      UPDATE("update"),
      @DocumentedValue("Create the schema, destroying previous data.")
      CREATE("create"),
      @DocumentedValue("Drop the schema when the SessionFactory is closed.")
      CREATE_DROP("create-drop");

      private final String value;

      Hbm2DdlMode(String value) {
         this.value = value;
      }

      public String getValue() {
         return value;
      }
   }

   protected interface Database {
      void applyProperties(Map<String, Object> properties);

      String getDriverClassName();

      String getUrl();

      String getDataSourceClassName();

      Map<String, String> getDataSourceProperties();

      public static class Converter extends ReflexiveConverters.ObjectConverter {
         public Converter() {
            super(Database.class);
         }
      }
   }

   protected interface ConnectionPool {
      void applyProperties(Map<String, Object> properties, Database database);
   }

   @DefinitionElement(name = "property", doc = "Custom property to be passed to entity manager factory.")
   public static class Prop {
      @Property(doc = "Name of the property", optional = false)
      public String name;

      @Property(doc = "Value of the property", optional = false)
      public String value;

      private static class Converter extends ReflexiveConverters.ListConverter {
         public Converter() {
            super(new Class[]{Prop.class});
         }
      }
   }

   @DefinitionElement(name = "h2", doc = "Connects to H2 database")
   public static class H2 implements Database {
      @Property(doc = "URL of the database.")
      String url;

      @Override
      public void applyProperties(Map<String, Object> properties) {
         properties.put(AvailableSettings.DIALECT, H2Dialect.class.getName());
      }

      @Override
      public String getDriverClassName() {
         return org.h2.Driver.class.getName();
      }

      @Override
      public String getDataSourceClassName() {
         return org.h2.jdbcx.JdbcDataSource.class.getName();
      }

      @Override
      public Map<String, String> getDataSourceProperties() {
         return Collections.singletonMap("url", url);
      }

      @Override
      public String getUrl() {
         return url;
      }
   }

   public abstract static class Postgres implements Database {
      @Property(doc = "Server address")
      String serverAddress;

      @Property(doc = "Server port")
      Integer serverPort;

      @Property(doc = "Database name")
      String database;

      public void applyProperties(Map<String, Object> properties) {
      }

      @Override
      public String getDriverClassName() {
         return org.postgresql.Driver.class.getName();
      }

      @Override
      public String getDataSourceClassName() {
         return org.postgresql.ds.PGSimpleDataSource.class.getName();
      }

      @Override
      public Map<String, String> getDataSourceProperties() {
         HashMap<String, String> map = new HashMap<>();
         map.put("ServerName", serverAddress);
         map.put("DatabaseName", database);
         if (serverPort != null) map.put("PortNumber", serverPort.toString());
         return map;
      }

      public String getUrl() {
         return "jdbc:postgresql://" + serverAddress + ':' + serverPort + '/' + database;
      }
   }

   @DefinitionElement(name = "postgres94", doc = "Connect to PostgreSQL database")
   public static class Postgres94 extends Postgres {
      @Override
      public void applyProperties(Map<String, Object> properties) {
         super.applyProperties(properties);
         properties.put(AvailableSettings.DIALECT, PostgreSQL94Dialect.class.getName());
      }
   }

   protected static class ConnectionPoolConverter extends ReflexiveConverters.ObjectConverter {
      public ConnectionPoolConverter() {
         super(ConnectionPool.class);
      }
   }

   @DefinitionElement(name = "default", doc = "Default connection pool, not recommended for production use.")
   public static class DefaultConnectionPool implements ConnectionPool {
      @Override
      public void applyProperties(Map<String, Object> properties, Database database) {
         properties.put(AvailableSettings.DRIVER, database.getDriverClassName());
         properties.put(AvailableSettings.URL, database.getUrl());
      }
   }

   @DefinitionElement(name = "c3p0", doc = "C3P0 connection pool.")
   public static class C3P0 implements ConnectionPool {
      @Property(doc = "Minimum pool size.")
      Integer minSize;

      @Property(doc = "Maximum pool size.")
      Integer maxSize;

      @Property(doc = "Timeout")
      Integer timeout;

      @Property(doc = "Maximum number of statements")
      Integer maxStatements;

      @Property(doc = "Idle test period.")
      Integer idleTestPeriod;

      @Override
      public void applyProperties(Map<String, Object> properties, Database database) {
         properties.put(AvailableSettings.CONNECTION_PROVIDER, C3P0ConnectionProvider.class.getName());
         properties.put(AvailableSettings.DRIVER, database.getDriverClassName());
         properties.put(AvailableSettings.URL, database.getUrl());

         if (minSize != null)
            properties.put("hibernate.c3p0.min_size", minSize.toString());
         if (maxSize != null)
            properties.put("hibernate.c3p0.max_size", maxSize.toString());
         if (timeout != null)
            properties.put("hibernate.c3p0.timeout", timeout.toString());
         if (maxStatements != null)
            properties.put("hibernate.c3p0.max_statements", maxStatements.toString());
         if (idleTestPeriod != null)
            properties.put("hibernate.c3p0.idle_test_period", idleTestPeriod.toString());
      }
   }

   @DefinitionElement(name = "hikari-cp", doc = "Hikari Connection Pool")
   public static class HikariCP implements ConnectionPool {
      @Property(doc = "Test query executed to verify connection.")
      String connectionTestQuery;

      @Property(doc = "Maximum pool size.")
      Integer maxSize;

      @Override
      public void applyProperties(Map<String, Object> properties, Database database) {
         properties.put(AvailableSettings.CONNECTION_PROVIDER, HikariCPConnectionProvider.class.getName());
         properties.put("hibernate.hikari.dataSourceClassName", database.getDataSourceClassName());

         for (Map.Entry<String, String> entry : database.getDataSourceProperties().entrySet()) {
            properties.put("hibernate.hikari.dataSource." + entry.getKey(), entry.getValue());
         }
         if (connectionTestQuery != null)
            properties.put("hibernate.hikari.connectionTestQuery", connectionTestQuery);
         if (maxSize != null)
            properties.put("hibernate.hikari.maximumPoolSize", maxSize.toString());
      }
   }

   @DefinitionElement(name = "iron-jacamar", doc = "Iron Jacamar")
   public static class IronJacamar implements ConnectionPool {
      @Property(doc = "Data source JNDI name.")
      String dataSourceJndi;

      @Property(doc = "Set if this DS implements JTA.")
      boolean jta;

      @Property(doc = "Definitions of datasources that will be deployed into Fungal.", optional = false,
            complexConverter = XmlConverter.class)
      String datasourceDefinitions;

      @Override
      public void applyProperties(Map<String, Object> properties, Database database) {
         if (jta) {
            properties.put(org.hibernate.jpa.AvailableSettings.JTA_DATASOURCE, dataSourceJndi);
         } else {
            properties.put(org.hibernate.jpa.AvailableSettings.NON_JTA_DATASOURCE, dataSourceJndi);
         }
      }
   }

   public abstract static class Cache {
      @Property(doc = "Cache query results. By default not set.")
      private Boolean useQueryCache;

      @Property(doc = "Default cache concurrency strategy. By default not set.")
      private CacheConcurrencyStrategy defaultCacheConcurrencyStrategy;

      @Property(doc = "Overrides for given entities.", complexConverter = CacheSettingsOverride.Converter.class)
      private List<CacheSettingsOverride> cacheSettingsOverrides;

      @Property(doc = "Shared cache mode. By default not set.")
      private SharedCacheMode sharedCacheMode;

      @Property(doc = "Use reference entries. By default not set.")
      private Boolean useDirectReferenceEntries;

      @Property(doc = "Enable use of structured second-level cache entries. By default not set.")
      private Boolean useStructuredEntries;

      @Property(doc = "Optimize the cache for minimal puts instead of minimal gets. By default not set.")
      private Boolean useMinimalPuts;

      public void applyProperties(Map<String, Object> properties) {
         properties.put(AvailableSettings.USE_SECOND_LEVEL_CACHE, Boolean.TRUE.toString());
         if (useQueryCache != null) {
            properties.put(AvailableSettings.USE_QUERY_CACHE, useQueryCache.toString());
         }
         if (defaultCacheConcurrencyStrategy != null) {
            properties.put(AvailableSettings.DEFAULT_CACHE_CONCURRENCY_STRATEGY, defaultCacheConcurrencyStrategy.toAccessType().getExternalName());
         }
         if (useDirectReferenceEntries != null) {
            properties.put(AvailableSettings.USE_DIRECT_REFERENCE_CACHE_ENTRIES, useDirectReferenceEntries.toString());
         }
         if (useStructuredEntries != null) {
            properties.put(AvailableSettings.USE_STRUCTURED_CACHE, useStructuredEntries.toString());
         }
         if (useMinimalPuts != null) {
            properties.put(AvailableSettings.USE_MINIMAL_PUTS, useMinimalPuts.toString());
         }
         if (sharedCacheMode != null) {
            properties.put(org.hibernate.jpa.AvailableSettings.SHARED_CACHE_MODE, sharedCacheMode.toString());
         }
      }

      protected static class Converter extends ReflexiveConverters.ObjectConverter {
         public Converter() {
            super(Cache.class);
         }
      }
   }

   @DefinitionElement(name = "override", doc = "Override caching setting for particular class.")
   public static class CacheSettingsOverride {
      @Property(name = "class", doc = "Class name", optional = false)
      private String clazz;

      @Property(doc = "Cache concurrency strategy. By default not set.", optional = false)
      private CacheConcurrencyStrategy strategy;

      public static class Converter extends ReflexiveConverters.ListConverter {
         public Converter() {
            super(new Class[]{CacheSettingsOverride.class});
         }
      }
   }

   @DefinitionElement(name = "infinispan", doc = "Second-level cache implemented by Infinispan")
   public static class InfinispanCache extends Cache {
      @Property(doc = "Infinispan configuration file.")
      String configuration;

      @Property(doc = "Mapping of classes to specific caches", complexConverter = ClassMapping.Converter.class)
      List<ClassMapping> classMappings = Collections.EMPTY_LIST;

      @Override
      public void applyProperties(Map<String, Object> properties) {
         super.applyProperties(properties);
         properties.put(AvailableSettings.CACHE_REGION_FACTORY, InfinispanRegionFactory.class.getName());
         if (configuration != null)
            properties.put(InfinispanRegionFactory.INFINISPAN_CONFIG_RESOURCE_PROP, configuration);
         for (ClassMapping mapping : classMappings) {
            properties.put("hibernate.cache.infinispan." + mapping.clazz + ".cfg", mapping.toCache);
         }
      }
   }

   @DefinitionElement(name = "ehcache", doc = "Second-level cache implemented by EHCache")
   public static class EhcacheCache extends Cache {
      @Property(doc = "EHCache configuration file.")
      String configuration;

      @Override
      public void applyProperties(Map<String, Object> properties) {
         properties.put(Environment.CACHE_REGION_FACTORY, EhCacheRegionFactory.class.getName());
         if (configuration != null) {
            properties.put(AvailableSettings.CACHE_PROVIDER_CONFIG, configuration);
         }
      }
   }

   @DefinitionElement(name = "mapping", doc = "Defines mapping specific map to certain cache.")
   public static class ClassMapping {
      @Property(name = "class", doc = "Class name", optional = false)
      String clazz;

      @Property(doc = "Cache name", optional = false)
      String toCache;

      public static class Converter extends ReflexiveConverters.ListConverter {
         public Converter() {
            super(new Class[]{ClassMapping.class});
         }
      }
   }
}
