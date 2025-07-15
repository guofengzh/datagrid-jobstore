/*
 * Copyright (C) 2015 Felix Finkbeiner and EXXETA AG
 * http://www.exxeta.com
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * 
 */

package com.exxeta.jobstore.infinispan;

import java.util.Collection;
import java.util.Set;

import javax.naming.InitialContext;
import javax.naming.NamingException;

import jakarta.transaction.*;
import org.infinispan.Cache;
import org.infinispan.context.Flag;
import org.infinispan.manager.CacheContainer;
import org.infinispan.util.concurrent.TimeoutException;
import org.quartz.Calendar;
import org.quartz.JobDetail;
import org.quartz.JobPersistenceException;
import org.quartz.SchedulerConfigException;

import com.exxeta.jobstore.TriggerWrapper;

/**
 * 
 * @author Felix Finkbeiner
 *
 */
public class InfinispanCacheConnectorWithLocking extends InfinispanCacheConnectorSupport {
	
	/**For Transactions and Locking on a Class wide (Threadbased) level*/
	protected TransactionManager transactionManager = null;

	public InfinispanCacheConnectorWithLocking(	String infinispanJNDI,
												String infinispanJobStoreCacheJobsByKey,
												String infinispanJobStoreCacheTriggersByKey,
												String infinispanJobStoreCacheCalendarsByName,
												String infinispanJobStoreCacheMetaData				
													) throws SchedulerConfigException{
		
		//Get access to Infinispan
		CacheContainer infinispanContainer;
		try {
			InitialContext ic = new InitialContext();
			infinispanContainer = (CacheContainer) ic.lookup(infinispanJNDI);
		} catch (NamingException e) {
			e.printStackTrace();
			throw new SchedulerConfigException("infinispanJNDI is not set propperly: " + infinispanJNDI);
		}
		
		//setup cache
		Cache<String, JobDetail> jobsByKeyTempCache = infinispanContainer.getCache(infinispanJobStoreCacheJobsByKey);
		Cache<String, TriggerWrapper> triggerByKeyTempCache = infinispanContainer.getCache(infinispanJobStoreCacheTriggersByKey);
		Cache<String, Calendar> calendarsByNameTempCache = infinispanContainer.getCache(infinispanJobStoreCacheCalendarsByName);
		Cache<String, Set<String>> metaDataTempCache = infinispanContainer.getCache(infinispanJobStoreCacheMetaData);
		
		//setup advanced caches
		this.jobsByKey = jobsByKeyTempCache.getAdvancedCache();
		this.triggersByKey = triggerByKeyTempCache.getAdvancedCache();
		this.calendarsByName = calendarsByNameTempCache.getAdvancedCache();
		this.metaData = metaDataTempCache.getAdvancedCache();
		
		//Flags for more performance at simple operations
		this.jobsByKey.withFlags(Flag.IGNORE_RETURN_VALUES);
		this.triggersByKey.withFlags(Flag.IGNORE_RETURN_VALUES);
		this.calendarsByName.withFlags(Flag.IGNORE_RETURN_VALUES);
		this.metaData.withFlags(Flag.IGNORE_RETURN_VALUES);
		
		
		//Transactionmanager is the same for every Cache of the Container
		this.transactionManager = this.triggersByKey.getTransactionManager();
	}


	/* (non-Javadoc)
	 * @see de.exxeta.thesis.scheduler.cacheConnector#lockJob(java.lang.String)
	 */
	@Override
	public boolean lockJob(String... keys) throws TimeoutException, JobPersistenceException {
		if(!jobsByKey.lock(keys)){
			rollbackJobStoreTransaction();
			throw new JobPersistenceException("Could not acquire JobLock");
		} return true;
	}
	
	/* (non-Javadoc)
	 * @see de.exxeta.thesis.scheduler.cacheConnector#lockJob(java.util.Collection)
	 */
	@Override
	public boolean lockJob(Collection<? extends String> keys) throws TimeoutException, JobPersistenceException {
		if(!jobsByKey.lock(keys)){
			rollbackJobStoreTransaction();
			throw new JobPersistenceException("Could not acquire JobLock");
		} return true;
	}
	
	/* (non-Javadoc)
	 * @see de.exxeta.thesis.scheduler.cacheConnector#lockTrigger(java.lang.String)
	 */
	@Override
	public boolean lockTrigger(String... keys) throws TimeoutException, JobPersistenceException {
		if(!triggersByKey.lock(keys)){
			rollbackJobStoreTransaction();
			throw new JobPersistenceException("Could not acquire TriggerLock");
		} return true;
	}
	
	/* (non-Javadoc)
	 * @see de.exxeta.thesis.scheduler.cacheConnector#lockTrigger(java.util.Collection)
	 */
	@Override
	public boolean lockTrigger(Collection<? extends String> keys) throws TimeoutException, JobPersistenceException {
		if(!triggersByKey.lock(keys)){
			rollbackJobStoreTransaction();
			throw new JobPersistenceException("Could not acquire TriggerLock");
		} return true;
	}
	
	/* (non-Javadoc)
	 * @see de.exxeta.thesis.scheduler.cacheConnector#lockCalendar(java.lang.String)
	 */
	@Override
	public boolean lockCalendar(String... keys) throws TimeoutException, JobPersistenceException {
		if(!calendarsByName.lock(keys)){
			rollbackJobStoreTransaction();
			throw new JobPersistenceException("Could not acquire CalendarLock");
		} return true;
	}
	 
	/* (non-Javadoc)
	 * @see de.exxeta.thesis.scheduler.cacheConnector#lockMetaData(java.lang.String)
	 */
	@Override
	public boolean lockMetaData(String... keys) throws TimeoutException, JobPersistenceException {
		if(!metaData.lock(keys)){
			rollbackJobStoreTransaction();
			throw new JobPersistenceException("Could not acquire MetaDataLock");
		} return true;
	}
	
	/* (non-Javadoc)
	 * @see de.exxeta.thesis.scheduler.cacheConnector#startJobStoreTransaction()
	 */
	@Override
	public void startJobStoreTransaction() throws JobPersistenceException{
		try {
			this.transactionManager.begin();
		} catch (NotSupportedException | SystemException e) {
			throw new JobPersistenceException("Couldn't start transaction: " + e.getMessage(), e);
		}
	}
	
	/* (non-Javadoc)
	 * @see de.exxeta.thesis.scheduler.cacheConnector#commtiJobStoreTransaction()
	 */
	@Override
	public void commitJobStoreTransaction() throws JobPersistenceException{
		try {
			this.transactionManager.commit();
		} catch (SecurityException | IllegalStateException |
				 RollbackException | HeuristicMixedException |
				 HeuristicRollbackException | SystemException e) {
			throw new JobPersistenceException("Couldn't commit transaction: " + e.getMessage(), e);
		}
	}
	
	/* (non-Javadoc)
	 * @see de.exxeta.thesis.scheduler.cacheConnector#rollbackJobStoreTransaction()
	 */
	@Override
	public void rollbackJobStoreTransaction() throws JobPersistenceException{
		try {
			this.transactionManager.rollback();
		} catch (IllegalStateException | SecurityException
				| SystemException e) {
			throw new JobPersistenceException("Couldn't rollbcak the transaction: " + e.getMessage(), e);
		}
	}
}
