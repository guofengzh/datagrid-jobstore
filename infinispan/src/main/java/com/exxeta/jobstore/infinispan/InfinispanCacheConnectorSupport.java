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

import org.infinispan.AdvancedCache;
import org.infinispan.util.concurrent.TimeoutException;

import com.exxeta.jobstore.CacheConnector;
import com.exxeta.jobstore.ClusterCacheJobStore;
import com.exxeta.jobstore.TriggerWrapper;
import org.quartz.Calendar;
import org.quartz.JobDetail;
import org.quartz.JobPersistenceException;

/**
 * A basic implementation for storing the work data
 * of a JobStore in an Infinispan cache
 * 
 * @author Felix Finkbeiner
 */
public abstract class InfinispanCacheConnectorSupport extends CacheConnector {
	
	/** Infinispan caches for storing the Jobs, Triggers and Calendars*/
	protected AdvancedCache<String, JobDetail> jobsByKey = null;
	protected AdvancedCache<String, TriggerWrapper> triggersByKey = null; 
	protected AdvancedCache<String, Calendar> calendarsByName = null;
	/** to persist and sync metadata like paused Groups */
	protected AdvancedCache<String, Set<String>> metaData = null;
	
	public InfinispanCacheConnectorSupport(){/*empty constructor for properties loading*/}
	
	//put
	/* (non-Javadoc)
	 * @see de.exxeta.thesis.scheduler.cacheConnector#putJob(java.lang.String, org.quartz.JobDetail)
	 */
	@Override
	public void putJob(String jobKey, JobDetail job){
		this.jobsByKey.put(jobKey, job);
	}
	
	/* (non-Javadoc)
	 * @see de.exxeta.thesis.scheduler.cacheConnector#putTriggerWrapper(java.lang.String, de.exxeta.thesis.scheduler.TriggerWrapper)
	 */
	@Override
	public void putTriggerWrapper(String triggerKey, TriggerWrapper triggerWrapper){
		this.triggersByKey.put(triggerKey, triggerWrapper);
	}
	
	/* (non-Javadoc)
	 * @see de.exxeta.thesis.scheduler.cacheConnector#putCalendar(java.lang.String, org.quartz.Calendar)
	 */
	@Override
	public void putCalendar(String calendarName, Calendar calendar){
		this.calendarsByName.put(calendarName, calendar);
	}
	
	/* (non-Javadoc)
	 * @see de.exxeta.thesis.scheduler.cacheConnector#putMetaData(java.lang.String, java.util.Set)
	 */
	@Override
	public void putMetaData(String metaDataKey, Set<String> metaData){
		this.metaData.put(metaDataKey, metaData);
	}
	
	//get
	/* (non-Javadoc)
	 * @see de.exxeta.thesis.scheduler.cacheConnector#getJob(java.lang.String)
	 */
	@Override
	public JobDetail getJob(String jobKey){
		return this.jobsByKey.get(jobKey);
	}
	
	/* (non-Javadoc)
	 * @see de.exxeta.thesis.scheduler.cacheConnector#getTriggerWrapper(java.lang.String)
	 */
	@Override
	public TriggerWrapper getTriggerWrapper(String triggerKey){
		return this.triggersByKey.get(triggerKey);
	}
	
	/* (non-Javadoc)
	 * @see de.exxeta.thesis.scheduler.cacheConnector#getCalendar(java.lang.String)
	 */
	@Override
	public Calendar getCalendar(String calendarName){
		return this.calendarsByName.get(calendarName);
	}
	
	/* (non-Javadoc)
	 * @see de.exxeta.thesis.scheduler.cacheConnector#getMetaData(java.lang.String)
	 */
	@Override
	public Set<String> getMetaData(String metaDataKey){
		return this.metaData.get(metaDataKey);
	}
	
	//remove
	/* (non-Javadoc)
	 * @see de.exxeta.thesis.scheduler.cacheConnector#removeJob(java.lang.String)
	 */
	@Override
	public void removeJob(String jobKey){
		this.jobsByKey.remove(jobKey);
	}
	
	/* (non-Javadoc)
	 * @see de.exxeta.thesis.scheduler.cacheConnector#removeTriggerWrapper(java.lang.String)
	 */
	@Override
	public void removeTriggerWrapper(String triggerKey){
		this.triggersByKey.remove(triggerKey);
	}
	
	/* (non-Javadoc)
	 * @see de.exxeta.thesis.scheduler.cacheConnector#removeCalendar(java.lang.String)
	 */
	@Override
	public void removeCalendar(String calendarName){
		this.calendarsByName.remove(calendarName);
	}
	
	/* (non-Javadoc)
	 * @see de.exxeta.thesis.scheduler.cacheConnector#removeMetaData(java.lang.String)
	 */
	@Override
	public void removeMetaData(String metaDataKey){
		this.metaData.remove(metaDataKey);
	}
	
	//Job count
	/* (non-Javadoc)
	 * @see de.exxeta.thesis.scheduler.cacheConnector#getNumberOfJobs()
	 */
	@Override
	public int getNumberOfJobs() throws JobPersistenceException {
		return this.jobsByKey.size();
	}

	/* (non-Javadoc)
	 * @see de.exxeta.thesis.scheduler.cacheConnector#getNumberOfTriggers()
	 */
	@Override
	public int getNumberOfTriggers() throws JobPersistenceException {
		return this.triggersByKey.size();
	}

	/* (non-Javadoc)
	 * @see de.exxeta.thesis.scheduler.cacheConnector#getNumberOfCalendars()
	 */
	@Override
	public int getNumberOfCalendars() throws JobPersistenceException {
		return this.calendarsByName.size();
	}
	
	
	//get all keys
	/* (non-Javadoc)
	 * @see de.exxeta.thesis.scheduler.cacheConnector#getAllJobKeys()
	 */
	@Override
	public Set<String> getAllJobKeys(){
		return this.jobsByKey.keySet();
	}
	
	/* (non-Javadoc)
	 * @see de.exxeta.thesis.scheduler.cacheConnector#getAllTriggerKeys()
	 */
	@Override
	public Set<String> getAllTriggerKeys(){
		return this.triggersByKey.keySet();
	}
	
	/* (non-Javadoc)
	 * @see de.exxeta.thesis.scheduler.cacheConnector#getAllCalendarNames()
	 */
	@Override
	public Set<String> getAllCalendarNames(){
		return this.calendarsByName.keySet();
	}
	
	//get all values
	/* (non-Javadoc)
	 * @see de.exxeta.thesis.scheduler.cacheConnector#getAllTriggers()
	 */
	@Override
	public Collection<TriggerWrapper> getAllTriggers(){
		return this.triggersByKey.values();
	}

	//contains
	/* (non-Javadoc)
	 * @see de.exxeta.thesis.scheduler.cacheConnector#containsJob(java.lang.String)
	 */
	@Override
	public boolean containsJob(String jobKey){
		return this.jobsByKey.containsKey(jobKey);
	}
	
	/* (non-Javadoc)
	 * @see de.exxeta.thesis.scheduler.cacheConnector#containsTrigger(java.lang.String)
	 */
	@Override
	public boolean containsTrigger(String triggerKey){
		return this.triggersByKey.containsKey(triggerKey);
	}
	
	/* (non-Javadoc)
	 * @see de.exxeta.thesis.scheduler.cacheConnector#containsCalendar(java.lang.String)
	 */
	@Override
	public boolean containsCalendar(String calendarName){
		return this.calendarsByName.containsKey(calendarName);
	}
	
	/* (non-Javadoc)
	 * @see de.exxeta.thesis.scheduler.cacheConnector#containsMetaData(java.lang.String)
	 */
	@Override
	public boolean containsMetaData(String metaDataKey){
		return this.metaData.containsKey(metaDataKey);
	}

	
	@Override
	public void createClusterRecoverListener(ClusterCacheJobStore jobStore){
		InfinispanJobStore js = (InfinispanJobStore) jobStore;
		//Add a listener for view changes to the caches
		this.triggersByKey.getCacheManager().addListener(
				new InfinispanClusterManagementListener(js));
	}
	
	public String getNodeID(){
		// The logical address format is typically: <hostname>-<integer>
		return jobsByKey.getCacheManager().getAddress().toString();
	}
	
	/* (non-Javadoc)
	 * @see de.exxeta.thesis.scheduler.cacheConnector#clearAllData()
	 */
	@Override
	public void clearAllData(){
		this.jobsByKey.clear();
		this.triggersByKey.clear();
		this.calendarsByName.clear();
		this.metaData.clear();
	}
	
	
	
	@Override
	public abstract boolean lockJob(String... keys) throws TimeoutException, JobPersistenceException;
	
	@Override
	public abstract boolean lockJob(Collection<? extends String> keys) throws TimeoutException, JobPersistenceException;
	
	@Override
	public abstract boolean lockTrigger(String... keys) throws TimeoutException, JobPersistenceException;
	
	@Override
	public abstract boolean lockTrigger(Collection<? extends String> keys) throws TimeoutException, JobPersistenceException;
	
	@Override
	public abstract boolean lockCalendar(String... keys) throws TimeoutException, JobPersistenceException;
	
	@Override
	public abstract boolean lockMetaData(String... keys) throws TimeoutException, JobPersistenceException;
	
	@Override
	public abstract void startJobStoreTransaction() throws JobPersistenceException;
	
	@Override
	public abstract void commitJobStoreTransaction() throws JobPersistenceException;
	
	@Override
	public abstract void rollbackJobStoreTransaction() throws JobPersistenceException;

}
