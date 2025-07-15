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

package com.exxeta.jobstore.hazelcast;

import java.util.Collection;
import java.util.LinkedList;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import com.hazelcast.map.IMap;
import org.quartz.Calendar;
import org.quartz.JobDetail;
import org.quartz.JobPersistenceException;

import com.exxeta.jobstore.CacheConnector;
import com.exxeta.jobstore.ClusterCacheJobStore;
import com.exxeta.jobstore.TriggerWrapper;
import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.transaction.TransactionContext;
import com.hazelcast.transaction.TransactionException;
import com.hazelcast.transaction.TransactionOptions;
import com.hazelcast.transaction.TransactionOptions.TransactionType;

/**
 * @author Felix Finkbeiner
 */
public class HazelcastCacheConnector extends CacheConnector {
	
	
	protected HazelcastInstance instance = null;
	
	protected IMap<String, JobDetail> jobsByKey = null;
	protected IMap<String, TriggerWrapper> triggersByKey = null; 
	protected IMap<String, Calendar> calendarsByName = null;
	protected IMap<String, Set<String>> metaData = null;
	
	private static Long LOCKWAITINGTIME = 10000L;
	
	
	private ThreadLocal<TransactionContext> transactionContext = null;
	
	protected TransactionOptions option = null;
	
	private static ThreadLocal<LinkedList<KeyMapPair>> lockedKeys = new ThreadLocal<LinkedList<KeyMapPair>>() {
		@Override
		protected LinkedList<KeyMapPair> initialValue() {
			return new LinkedList<KeyMapPair>();
		}
	};
	
	private static final String jobsByKeyName = "jobsByKey";
	private static final String triggersByKeyName = "triggersByKey";
	private static final String calendarsByNameName = "calendarsByName";
	private static final String metaDataName = "metaData";
	
	
	public HazelcastCacheConnector(Config cfg) {
		
		instance = Hazelcast.newHazelcastInstance(cfg);

		jobsByKey = instance.getMap(jobsByKeyName);
		triggersByKey = instance.getMap(triggersByKeyName);
		calendarsByName = instance.getMap(calendarsByNameName);
		metaData = instance.getMap(metaDataName);
		
		option = new TransactionOptions().setTransactionType(TransactionType.TWO_PHASE);
		transactionContext = new ThreadLocal<TransactionContext>(){
			@Override
			protected TransactionContext initialValue() {
				return instance.newTransactionContext(option);
			}
		};
	}

	@Override
	public void putJob(String jobKey, JobDetail job) {
		jobsByKey.put(jobKey, job);
	}

	@Override
	public void putTriggerWrapper(String triggerKey, TriggerWrapper triggerWrapper) {
		this.triggersByKey.put(triggerKey, triggerWrapper);
	}

	@Override
	public void putCalendar(String calendarName, Calendar calendar) {
		this.calendarsByName.put(calendarName, calendar);
	}

	@Override
	public void putMetaData(String metaDataKey, Set<String> metaData) {
		this.metaData.put(metaDataKey, metaData);
	}

	@Override
	public JobDetail getJob(String jobKey) {
		return this.jobsByKey.get(jobKey);
	}

	@Override
	public TriggerWrapper getTriggerWrapper(String triggerKey) {
		return this.triggersByKey.get(triggerKey);
	}

	@Override
	public Calendar getCalendar(String calendarName) {
		return this.calendarsByName.get(calendarName);
	}

	@Override
	public Set<String> getMetaData(String metaDataKey) {
		return this.metaData.get(metaDataKey);
	}

	@Override
	public void removeJob(String jobKey) {
		this.jobsByKey.remove(jobKey);
	}

	@Override
	public void removeTriggerWrapper(String triggerKey) {
		this.triggersByKey.remove(triggerKey);
	}

	@Override
	public void removeCalendar(String calendarName) {
		this.calendarsByName.remove(calendarName);
	}

	@Override
	public void removeMetaData(String metaDataKey) {
		this.metaData.remove(metaDataKey);
	}

	@Override
	public int getNumberOfJobs() throws JobPersistenceException {
		return jobsByKey.size();
	}

	@Override
	public int getNumberOfTriggers() throws JobPersistenceException {
		return triggersByKey.size();
	}

	@Override
	public int getNumberOfCalendars() throws JobPersistenceException {
		return calendarsByName.size();
	}

	@Override
	public Set<String> getAllJobKeys() {
		return jobsByKey.keySet();
	}

	@Override
	public Set<String> getAllTriggerKeys() {
		return triggersByKey.keySet();
	}

	@Override
	public Set<String> getAllCalendarNames() {
		return calendarsByName.keySet();
	}

	@Override
	public Collection<TriggerWrapper> getAllTriggers() {
		return triggersByKey.values();
	}

	@Override
	public boolean containsJob(String jobKey) {
		return jobsByKey.containsKey(jobKey);
	}

	@Override
	public boolean containsTrigger(String triggerKey) {
		return triggersByKey.containsKey(triggerKey);
	}

	@Override
	public boolean containsCalendar(String calendarName) {
		return calendarsByName.containsKey(calendarName);
	}

	@Override
	public boolean containsMetaData(String metaDataKey) {
		return metaData.containsKey(metaDataKey);
	}

	@Override
	public void createClusterRecoverListener(ClusterCacheJobStore jobStore) {
		 instance.getCluster().addMembershipListener(new HazelcastClusterManagementListener(jobStore));
	}
	
	@Override
	public String getNodeID() {
		return instance.getCluster().getLocalMember().getSocketAddress().toString();
	}

	@Override
	public void clearAllData() {
		this.jobsByKey.clear();
		this.triggersByKey.clear();
		this.calendarsByName.clear();
		this.metaData.clear();
	}

	@Override
	public boolean lockJob(String... keys) throws JobPersistenceException {
		return this.tryLockKeys(keys, jobsByKey);
	}
	

	@Override
	public boolean lockJob(Collection<? extends String> keys) throws JobPersistenceException {
		return this.lockJob((String[]) keys.toArray(new String[keys.size()]));
	}

	@Override
	public boolean lockTrigger(String... keys) throws JobPersistenceException {
		return this.tryLockKeys(keys, triggersByKey);
	}

	@Override
	public boolean lockTrigger(Collection<? extends String> keys) throws JobPersistenceException {
		return this.lockTrigger((String[]) keys.toArray(new String[keys.size()]));
	}

	@Override
	public boolean lockCalendar(String... keys) throws JobPersistenceException {
		return this.tryLockKeys(keys, calendarsByName);
	}

	@Override
	public boolean lockMetaData(String... keys) throws JobPersistenceException {
		return this.tryLockKeys(keys, metaData);
	}

	@Override
	public void startJobStoreTransaction() throws JobPersistenceException {
		try {
			transactionContext.get().beginTransaction();
		} catch(RuntimeException re){
			throw new JobPersistenceException(re.getMessage());
		}
	}

	@Override
	public void commitJobStoreTransaction() throws JobPersistenceException {
		try {
			transactionContext.get().commitTransaction();
		} catch(TransactionException te){
			this.unlockKeys(); //TODO 
			throw new JobPersistenceException("Commit failed");
		}
		this.unlockKeys();
	}

	@Override
	public void rollbackJobStoreTransaction() throws JobPersistenceException {
		try {
			transactionContext.get().rollbackTransaction();
		} catch(TransactionException te){
			this.unlockKeys();  
			throw new JobPersistenceException("Rollback threw exception");
		}
		this.unlockKeys();
	}
	
	private void unlockKeys(){
		LinkedList<KeyMapPair> lk = lockedKeys.get();
		for(KeyMapPair pair:lk){
			pair.map.unlock(pair.key);
		}
		lockedKeys.remove();
	}
	
	
	private boolean tryLockKeys(String[] keys, IMap<String, ? extends Object> map) throws JobPersistenceException{
		boolean gotAllLocks = true;
		int i = 0;
		while(i < keys.length && gotAllLocks){
			try {
				gotAllLocks = map.tryLock(keys[i], LOCKWAITINGTIME, TimeUnit.MILLISECONDS);
			} catch (InterruptedException e) {
				gotAllLocks = false;
			} catch(NullPointerException ne){
				//specified key is null
				this.unlockKeys(keys, i, map);
				throw new JobPersistenceException("Tryed to lock a key that is Null");
			}
			i++;
		}
		if(!gotAllLocks){
			this.unlockKeys(keys, i, map);
		} else {
			LinkedList<KeyMapPair> lk = lockedKeys.get();
			for(String key:keys){
				lk.add(new KeyMapPair(key, map));
			}
		}
		return gotAllLocks;
	}
	
	private void unlockKeys(String[] keys, int toIndex, IMap<String, ? extends Object> map){
		for(int j = 0; j <= toIndex; j++){
			map.unlock(keys[j]);
		}
	}
	
	
	/**
	 * For The Mapping of the locked Key to the Map he is locked in
	 */
	class KeyMapPair{
		public String key;
		public IMap<String, ? extends Object> map;
		
		public KeyMapPair(String key, IMap<String, ? extends Object> map){
			this.key = key;
			this.map = map;
		}
	}

}
