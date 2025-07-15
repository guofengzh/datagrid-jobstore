# Spring Boot + Quartz + Infinispan

## Why forking?

This fork is to enable [datagrid-jobstore](https://github.com/EXXETA/datagrid-jobstore) to run on Spring boot.

## Configuration

Configuration of Quartz in quartz.properties is now placed in the application.yaml/properties file. For example:
```
# application.properties
org.quartz.threadPool.threadCount = 3
```
This project supports some default configurations, see [QuartzInfinispanStoreAutoConfiguration.java](https://github.com/guofengzh/datagrid-jobstore/blob/master/spring-boot-infinispan-quartz-starter/src/main/java/com/exxeta/jobstore/infinispan/autoconfig/QuartzInfinispanStoreAutoConfiguration.java).

## Sample Application

[infinispan-quartz-demo](https://github.com/guofengzh/datagrid-jobstore/blob/master/infinispan-quartz-demo/src/main/resources/infinispan.xml) is a sample application. The content of infinispan.xml is a required configuration. This configuration file is referenced by the application.yml file. This is required by [infinispan-spring-boot3-starter-embedded](https://infinispan.org/docs/stable/titles/spring_boot/starter.html) to configure Infinispan.

> The following is the original readme content, please refer to the [original project](https://github.com/EXXETA/datagrid-jobstore).

# Cluster Cache JobStore

This project allows you to use a key/value store as a Quartz JobStore. See [Quartz tutorial Job Stores](http://quartz-scheduler.org/documentation/quartz-2.x/tutorials/tutorial-lesson-09) for more information. The Framework is already implemented for the data grids Infinispan and Hazelcast. Both implementations have been deployed and tested in a JBoss EAP-6.3.0 environment but may be used in any environment. 

Documentation of

InfinispanJobStore v1.0  
HazelcastJobStore v1.0  
ClusterCacheJobStore v1.0 

Author Felix Finkbeiner  
Company [EXXETA AG](http://www.exxeta.com)

**!!! HANDLE WITH CAUTION !!!**

# 1. How to use the InfinispanJobStore in JBoss
This documentation only describes how to use the InfinispanJobStore within a JBoss EAP environment.

## 1.1 JBoss configuration

### 1.1.1 Caches
Configure four Infinispan caches in your JBoss configuration file (standalone.xml) like so:


	<cache-container name="jobStoreCaches" jndi-name="java:jboss/infinispan/jobStore" start="EAGER">
	    <transport lock-timeout="60000"/>
	    <replicated-cache name="cacheOne" mode="SYNC">
	        <locking isolation="READ_COMMITTED"/>
	        <transaction mode="NON_XA" locking="PESSIMISTIC"/>
	        <eviction strategy="NONE"/>
	    </replicated-cache>
	    <replicated-cache name="cacheTwo" mode="SYNC">
	        <locking isolation="READ_COMMITTED"/>
	        <transaction mode="NON_XA" locking="PESSIMISTIC"/>
	        <eviction strategy="NONE"/>
	    </replicated-cache>
	    <replicated-cache name="cacheThree" mode="SYNC">
	        <locking isolation="READ_COMMITTED"/>
	        <transaction mode="NON_XA" locking="PESSIMISTIC"/>
	        <eviction strategy="NONE"/>
	    </replicated-cache>
	    <replicated-cache name="cacheFour" mode="SYNC">
	        <locking isolation="READ_COMMITTED"/>
	        <transaction mode="NON_XA" locking="PESSIMISTIC"/>
	        <eviction strategy="NONE"/>
	    </replicated-cache>
	</cache-container>
	
	
### 1.1.2 JGroups
Configure JGroups to use udp and choose your network settings:


	<subsystem xmlns="urn:jboss:domain:jgroups:1.1" default-stack="udp">
        <stack name="udp">
            <transport type="UDP" socket-binding="jgroups-udp"/>
            <protocol type="PING"/>
            <protocol type="MERGE3"/>
            <protocol type="FD_SOCK" socket-binding="jgroups-udp-fd"/>
            <protocol type="FD"/>
            <protocol type="VERIFY_SUSPECT"/>
            <protocol type="pbcast.NAKACK"/>
            <protocol type="UNICAST2"/>
            <protocol type="pbcast.STABLE"/>
            <protocol type="pbcast.GMS"/>
            <protocol type="UFC"/>
            <protocol type="MFC"/>
            <protocol type="FRAG2"/>
            <protocol type="RSVP"/>
        </stack>
    </subsystem>



	<socket-binding-group name="standard-sockets" default-interface="public" port-offset="${jboss.socket.binding.port-offset:0}">
        <socket-binding name="management-native" interface="management" port="${jboss.management.native.port:9999}"/>
        <socket-binding name="management-http" interface="management" port="${jboss.management.http.port:9990}"/>
        <socket-binding name="management-https" interface="management" port="${jboss.management.https.port:9443}"/>
        <socket-binding name="ajp" port="8009"/>
        <socket-binding name="http" port="8080"/>
        <socket-binding name="https" port="8443"/>
        <socket-binding name="jgroups-mping" port="0" multicast-address="${jboss.default.multicast.address:230.0.0.4}" multicast-port="45700"/>
        <socket-binding name="jgroups-tcp" port="7600"/>
        <socket-binding name="jgroups-tcp-fd" port="57600"/>
        <socket-binding name="jgroups-udp" port="55200" multicast-address="${jboss.default.multicast.address:230.0.0.4}" multicast-port="45688"/>
        <socket-binding name="jgroups-udp-fd" port="54200"/>
        <socket-binding name="remoting" port="4447"/>
        <socket-binding name="txn-recovery-environment" port="4712"/>
        <socket-binding name="txn-status-manager" port="4713"/>
        <outbound-socket-binding name="mail-smtp">
            <remote-destination host="localhost" port="25"/>
        </outbound-socket-binding>
    </socket-binding-group>


## 1.2 Quartz configuration

Within your project create the quartz.properties file under
> src/main/webapp/WEB-INF/classes/quartz.properties

This file will tell Quartz what options and which JobStore it should use.
Assign:

* The names of the previously created Infinispan caches

* The JNDI of your Infinispan container

* The full qualifying class name of the InfinispanJobStore

* Additional configurations of your scheduler visit [Quartz configuration](http://quartz-scheduler.org/documentation/quartz-2.x/configuration/) for more information

Note that we use the names of the caches and the JNDI that we declared under 1.1.1

	#============================================================================
	# Configure ThreadPool  
	#============================================================================
	
	org.quartz.threadPool.class: org.quartz.simpl.SimpleThreadPool
	org.quartz.threadPool.threadCount: 2
	org.quartz.threadPool.threadPriority: 5
	
	#============================================================================
	# Configure JobStore  
	#============================================================================

	org.quartz.jobStore.misfireThreshold: 60000
	
	org.quartz.jobStore.class=com.exxeta.quartzscheduler.infinispan.InfinispanJobStore
	
	org.quartz.jobStore.infinispanJNDI: java:jboss/infinispan/jobStore  
	org.quartz.jobStore.infinispanJobStoreCacheJobsByKey: cacheOne  
	org.quartz.jobStore.infinispanJobStoreCacheTriggersByKey: cacheTwo  
	org.quartz.jobStore.infinispanJobStoreCacheCalendarsByName: cacheThree  
	org.quartz.jobStore.infinispanJobStoreCacheMetaData: cacheFour

## 1.3 Start the scheduler in your application

Create a Quartz scheduler within your application by using org.quartz.impl.StdSchedulerFactory()


	@Singleton
	public class SchedulerBean {
		private static final Logger LOGGER = Logger.getLogger(SchedulerBean.class);
		
		@Resource(lookup="java:jboss/infinispan/jobStore")
		private CacheContainer container;
		private Scheduler scheduler;
	
		@PostConstruct
		public void setupScheduler() {
			SchedulerFactory schedulerFactory = new org.quartz.impl.StdSchedulerFactory();
			try {
				scheduler = schedulerFactory.getScheduler();
				LOGGER.info("-------------Starting Scheduler---------------");
				scheduler.start();
			} catch (SchedulerException e) {
				LOGGER.error(e.getMessage());
			}
		}
		
		public Scheduler getScheduler(){
			return scheduler;
		}
	}

Run your application and schedule some jobs.

---
# 2. How to use the HazelcastJobStore

To tell Quartz that it should use the HazelcastJobStore create the quartz.properties file usual under
> src/main/webapp/WEB-INF/classes/quartz.properties

For a functioning JobStore you only have to assign the full qualifying class name of the HazelcastJobStore and configure your scheduler, visit [Quartz configuration](http://quartz-scheduler.org/documentation/quartz-2.x/configuration/) for more information.

	#============================================================================
	# Configure ThreadPool  
	#============================================================================
	
	org.quartz.threadPool.class: org.quartz.simpl.SimpleThreadPool
	org.quartz.threadPool.threadCount: 2
	org.quartz.threadPool.threadPriority: 5
	
	#============================================================================
	# Configure JobStore  
	#============================================================================

	org.quartz.jobStore.misfireThreshold: 60000

	org.quartz.jobStore.class=com.exxeta.jobstore.hazelcast.HazelcastJobStore

If you want to specify your Hazelcast settings e.g. change the port, WAN, ... you can use a configuration XML.

	org.quartz.jobStore.hazelcastConfigurationXML: C:\\hazelcastconfiguration.xml


---
# 3. How to create a JobStore using ClusterCacheJobStore

If you want to create a JobStore for a key/value store other than Infinispan you can use the ClusterCacheJobStore for that.
You need to provide:

* Your own CacheConnector

* A listener for topology changes in the cluster

* Initialization of the CacheConnector in the ClusterCacheJobStore 

See the **com.exxeta.jobstore.infinispan** package as example:

## 3.1 com.exxeta.quartzscheduler.CacheConnector

The CacheConnector interface is used to separate the key/value store access from the JobStore logic. You need to implement this interface for the key/value store you want to use. Your CacheConnector provides all the data grid specific operations like:

* get, put, remove, contains,...

* getNodeID() a identifier that represents the node in the cluster

* Locks

* Start-, commit-, Rollback-Transaction

* Cluster listener for leaving nodes

## 3.2 cluster listener

You need to provide a Listener that recognizes when a node leaves the cluster. Has a node left the cluster than your listener must call the **recover** method on the ClusterCacheJobStore with the node-ID of the node who left. See the Javadoc or InfinispanCacheConnectorSupport.createClusterRecoverListener(..), InfinispanCacheConnectorSupport.getNodeID() and InfinispanClusterManagementListener for a detailed example.

## 3.3 Initialization of the CacheConnector

Your newly created CacheConnector may requires some Initialization and needs to be set in the ClusterCacheJobStore. To archive that, you need to inherit from ClusterCacheJobStore and implement the abstract method **createCacheConnector()** by setting the **connector** attribute of the Superclass to your CacheConnector. 

this.connector = new yourOwnCacheConnector()

To tell Quartz that it should use your JobStore you have to set the **org.quartz.jobStore.class=** attribute in the **quartz.properties** file to your JobStore class (the class who inherits from ClusterCacheJobStore). If your CacheConnector needs some additional information, for example a JNDI to your key/value store, you can also set that within the **quartz.properties** file using:

org.quartz.jobStore.yourAttribute: value

*yourAttribute* is an Attribute in your JobStore class that needs a public setter because Quartz uses reflection to set them.

See com.exxeta.jobstore.infinispan.InfinispanJobStore as example.

---
# License

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software 
distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.






