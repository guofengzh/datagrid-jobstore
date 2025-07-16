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

package com.exxeta.jobstore;

import java.util.Collection;
import java.util.Set;

import org.jboss.logging.Logger;
import org.quartz.Calendar;
import org.quartz.JobDetail;
import org.quartz.JobPersistenceException;
import org.quartz.ObjectAlreadyExistsException;

/**
 * 
 * @author Felix Finkbeiner
 *
 * @param <ClusterListener> is the Listener that informs the JobStore about cluster changes
 */
public abstract class CacheConnector {
	
	private static final Logger LOGGER_TRANSACTION = Logger.getLogger("com.exxeta.jobstore.ClusterCacheJobStore.Transaction");
	
	private static final ThreadLocal<Integer> classLocked = new ThreadLocal<Integer>() {
		@Override
		protected Integer initialValue() {
			return 0;
		}
	};

	//put
	public abstract void putJob(String jobKey, JobDetail job);

	public abstract void putTriggerWrapper(String triggerKey,
			TriggerWrapper triggerWrapper);

	public abstract void putCalendar(String calendarName, Calendar calendar);

	public abstract void putMetaData(String metaDataKey, Set<String> metaData);

	//get
	public abstract JobDetail getJob(String jobKey);

	public abstract TriggerWrapper getTriggerWrapper(String triggerKey);

	public abstract Calendar getCalendar(String calendarName);

	public abstract Set<String> getMetaData(String metaDataKey);

	//remove
	public abstract void removeJob(String jobKey);

	public abstract void removeTriggerWrapper(String triggerKey);

	public abstract void removeCalendar(String calendarName);

	public abstract void removeMetaData(String metaDataKey);

	//Job count
	public abstract int getNumberOfJobs() throws JobPersistenceException;

	public abstract int getNumberOfTriggers() throws JobPersistenceException;

	public abstract int getNumberOfCalendars() throws JobPersistenceException;

	//get all keys
	public abstract Set<String> getAllJobKeys();

	public abstract Set<String> getAllTriggerKeys();

	public abstract Set<String> getAllCalendarNames();

	//get all values
	public abstract Collection<TriggerWrapper> getAllTriggers();

	//contains
	public abstract boolean containsJob(String jobKey);

	public abstract boolean containsTrigger(String triggerKey);

	public abstract boolean containsCalendar(String calendarName);

	public abstract boolean containsMetaData(String metaDataKey);

	/**
	 * This listener notifies the JobStore about a lost Cluster node
	 * @param jobStore is the JobStore that should be notified when a Cluster node got lost 
	 * 		  by calling {@link com.exxeta.jobstore.ClusterCacheJobStore#recover(String)}
	 * @see com.exxeta.jobstore.infinispan.InfinispanClusterManagementListener
	 */
	public abstract void createClusterRecoverListener(ClusterCacheJobStore jobStore);
	
	/**
	 * @return a identifying name for this node
	 */
	public abstract String getNodeID();

	/**
	 * removes all the Data from all the Caches
	 */
	public abstract void clearAllData();

	/**
	 * 
	 * @param keys the keys that should be locked
	 * @return true if sucessfull 
	 * @throws TimeoutException if lock could not be acquired within a reasonable time
	 * @throws JobPersistenceException if there is a problem with the persistence layer
	 */
	public abstract boolean lockJob(String... keys) throws JobPersistenceException;

	public abstract boolean lockJob(Collection<? extends String> keys)
			throws JobPersistenceException;

	public abstract boolean lockTrigger(String... keys)
			throws JobPersistenceException;

	public abstract boolean lockTrigger(Collection<? extends String> keys)
			throws JobPersistenceException;

	public abstract boolean lockCalendar(String... keys)
			throws JobPersistenceException;

	public abstract boolean lockMetaData(String... keys)
			throws JobPersistenceException;

	/**
	 * starts the transaction
	 * @throws JobPersistenceException if the Transaction could not be started
	 */
	public abstract void startJobStoreTransaction()
			throws JobPersistenceException;

	/**
	 * commits the transaction 
	 * @throws JobPersistenceException
	 */
	public abstract void commitJobStoreTransaction()
			throws JobPersistenceException;

	/**
	 * Roles back the open Transaction
	 * @throws JobPersistenceException
	 */
	public abstract void rollbackJobStoreTransaction()
			throws JobPersistenceException;
	
	public void doInTransaction(TransactionScoped transactionScoped) throws JobPersistenceException {
		this.startTransaction();
		
		try {
			transactionScoped.doInTransaction();
		} catch(AbortTransactionException atr){
			this.rollbackTransaction();
			throw atr;
		} catch(ObjectAlreadyExistsException oae){
			this.rollbackTransaction();
			throw oae;
		}catch(JobPersistenceException jpe){
			this.rollbackTransaction();
			throw jpe;
		} catch(Throwable throwable){
			this.rollbackTransaction();
			LOGGER_TRANSACTION.error(throwable.getMessage(), throwable);
			throw new JobPersistenceException("Exception in TransactionScope", throwable);
		}
			
		this.commitTransaction();
	}
	
	private void rollbackTransaction() throws JobPersistenceException{
		if(classLocked.get().intValue() > 0){
			try {
				this.rollbackJobStoreTransaction();
			} catch (JobPersistenceException e) {
				throw new JobPersistenceException("Couldn't rollbcak the transaction: " + e.getMessage(), e);
			} finally{
				classLocked.set(0);
			}
		} else {
			LOGGER_TRANSACTION.debug("Trying to rollback a Transaction while non is opened!");
		}
	}
	
	private void commitTransaction() throws JobPersistenceException{
		if(classLocked.get().equals(1)){
			try {
				this.commitJobStoreTransaction();
			} catch (JobPersistenceException e) {
				throw new JobPersistenceException("Couldn't commit transaction: " + e.getMessage(), e);
			} finally {
				classLocked.set(classLocked.get() - 1);
			}
		} else if(classLocked.get().intValue() > 1){
			classLocked.set(classLocked.get() - 1);
		} else {
			LOGGER_TRANSACTION.error("Trying to commit a Transaction while non is opened!");
		}
	}
	
	private void startTransaction() throws JobPersistenceException{
		if(classLocked.get().equals(0)){
			
			this.startJobStoreTransaction();
			//Establish classwide Transaction
			classLocked.set(classLocked.get() + 1);
		
		} else if(classLocked.get().intValue() > 0){
			classLocked.set(classLocked.get() + 1);
		} else {
			LOGGER_TRANSACTION.error("Transaction is already opened");
		}
	}
	
	

}