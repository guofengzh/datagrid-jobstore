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
import org.quartz.Calendar;
import org.quartz.JobDetail;
import org.quartz.JobPersistenceException;
import org.quartz.ObjectAlreadyExistsException;

import com.exxeta.jobstore.TransactionScoped;
import com.exxeta.jobstore.TriggerWrapper;
import com.exxeta.jobstore.infinispan.InfinispanCacheConnectorSupport;

/**
 * Returns true to every Lock request 
 * for better testability with local infinispan caches
 * 
 * @author Felix Finkbeiner
 */
public class InfinispanCacheConnectorTestDummy extends InfinispanCacheConnectorSupport {
	
	public void setTriggersByKey(AdvancedCache<String, TriggerWrapper> triggersByKey){
		this.triggersByKey = triggersByKey;
	}
	
	public void setJobsByKey(AdvancedCache<String, JobDetail> jobsByKey){
		this.jobsByKey = jobsByKey;
	}
	
	public void setCalendarsByName(AdvancedCache<String, Calendar> calendarsByName){
		this.calendarsByName = calendarsByName;
	}
	
	public void setMetaData(AdvancedCache<String, Set<String>> metaData){
		this.metaData = metaData;
	}

	@Override
	public boolean lockJob(String... keys) throws TimeoutException,
			JobPersistenceException {
		return true;
	}

	@Override
	public boolean lockJob(Collection<? extends String> keys)
			throws TimeoutException, JobPersistenceException {
		return true;
	}

	@Override
	public boolean lockTrigger(String... keys) throws TimeoutException,
			JobPersistenceException {
		return true;
	}

	@Override
	public boolean lockTrigger(Collection<? extends String> keys)
			throws TimeoutException, JobPersistenceException {
		return true;
	}

	@Override
	public boolean lockCalendar(String... keys) throws TimeoutException,
			JobPersistenceException {
		return true;
	}

	@Override
	public boolean lockMetaData(String... keys) throws TimeoutException,
			JobPersistenceException {
		return true;
	}

	@Override
	public void startJobStoreTransaction() throws JobPersistenceException {
	}

	@Override
	public void commitJobStoreTransaction() throws JobPersistenceException {
	}

	@Override
	public void rollbackJobStoreTransaction() throws JobPersistenceException {
	}

	@Override
	public void doInTransaction(TransactionScoped transactionScoped)
			throws JobPersistenceException, ObjectAlreadyExistsException {
		transactionScoped.doInTransaction();
	}

}
