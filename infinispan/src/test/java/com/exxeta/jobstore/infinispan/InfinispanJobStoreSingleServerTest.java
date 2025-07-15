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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.manager.EmbeddedCacheManager;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.quartz.Calendar;
import org.quartz.JobDataMap;
import org.quartz.JobDetail;
import org.quartz.JobKey;
import org.quartz.JobPersistenceException;
import org.quartz.ObjectAlreadyExistsException;
import org.quartz.SchedulerException;
import org.quartz.Trigger;
import org.quartz.Trigger.CompletedExecutionInstruction;
import org.quartz.Trigger.TriggerState;
import org.quartz.TriggerKey;
import org.quartz.impl.matchers.GroupMatcher;
import org.quartz.spi.OperableTrigger;
import org.quartz.spi.SchedulerSignaler;
import org.quartz.spi.TriggerFiredBundle;
import org.quartz.spi.TriggerFiredResult;

import com.exxeta.jobstore.ClusterCacheJobStore;
import com.exxeta.jobstore.TimeOfSystem;
import com.exxeta.jobstore.TriggerWrapper;

/**
 * Tests the InfinispanJobStore
 * @author fefi1
 */
public class InfinispanJobStoreSingleServerTest {
	
	private static final ClusterCacheJobStore js = new InfinispanJobStore();

	private static  EmbeddedCacheManager cacheManager;
	
	private static AdvancedCache<String, JobDetail> jobCache;
	private static AdvancedCache<String, TriggerWrapper> triggerCache;
	private static AdvancedCache<String, Calendar> calendarCache;
	private static AdvancedCache<String, Set<String>> metaCache;
	
	@AfterClass
	public static void cacheManagerTeardown(){
		cacheManager.stop();
	}

	@Before
	public void setUpClusterRessources(){
		String triggerCacheName = "triggers";
		String jobCacheName = "jobs";
		String calendarCacheName = "calendars";
		String metaCacheName = "meta";
		
		InfinispanCacheConnectorTestDummy connector = new InfinispanCacheConnectorTestDummy();

		GlobalConfigurationBuilder global = GlobalConfigurationBuilder.defaultClusteredBuilder();
		cacheManager = new DefaultCacheManager(global.build());

		ConfigurationBuilder builder = new ConfigurationBuilder();
		builder.clustering().cacheMode(CacheMode.LOCAL);
		Configuration c = builder.build();
		cacheManager.defineConfiguration(triggerCacheName, c);
		cacheManager.defineConfiguration(jobCacheName, c);
		cacheManager.defineConfiguration(calendarCacheName, c);
		cacheManager.defineConfiguration(metaCacheName, c);

		//Trigger Cache
		Cache<String, TriggerWrapper> tempTriggerCache = cacheManager.getCache(triggerCacheName);
		triggerCache = tempTriggerCache.getAdvancedCache();
		connector.setTriggersByKey(triggerCache);
		
		//Job Cache
		Cache<String, JobDetail> tempJobCache = cacheManager.getCache(jobCacheName);
		jobCache = tempJobCache.getAdvancedCache();
		connector.setJobsByKey(jobCache);
		
		//Calendar Cache
		Cache<String, Calendar> tempCalendarCache = cacheManager.getCache(calendarCacheName);
		calendarCache = tempCalendarCache.getAdvancedCache();
		connector.setCalendarsByName(calendarCache);
		
		//Meta Cache
		Cache<String, Set<String>> tempMetaCache = cacheManager.getCache(metaCacheName);
		metaCache = tempMetaCache.getAdvancedCache();
		connector.setMetaData(metaCache);
		
		js.setConnector(connector);
	}
	
	@After
	public void cleanUpCluster(){
		triggerCache.clear();
		jobCache.clear();
		calendarCache.clear();
		metaCache.clear();
	}
	
	@Test
	public void testStoreJob_StoreJobDetail() {
		//Mock the Job
		JobDetail jdMock = mock(JobDetail.class);
		when(jdMock.getKey()).thenReturn(new JobKey("name", "group"));
		
		try {
			//Store the Job
			js.storeJob(jdMock, false);
		} catch (Exception e){
			fail("Unexpected Exception: ");
			e.printStackTrace();
		}
		//is JobDetail persisted?
		assertTrue(jobCache.containsKey("groupname"));
	}
	
	@Test
	public void testStoreJob_DontReplaceExisting() {
		//Mock the Job
		JobDetail jdMock = mock(JobDetail.class);
		when(jdMock.getKey()).thenReturn(new JobKey("name", "group"));
		
		try {
			//Store the Job
			js.storeJob(jdMock, false);
			//Store a Job with the same key
			js.storeJob(jdMock, false);
			fail("An expected Exception has not been thrown");
		} catch (ObjectAlreadyExistsException oae){
			//Same key was not allowed the exception has been thrown
			assertTrue(true);
		} catch (Exception e){
			fail("Unexpected Exception: ");
			e.printStackTrace();
		}
	}
	
	@Test
	public void testStoreTrigger_StoreOperableTrigger() {
		OperableTrigger newTriggerMock = mock(OperableTrigger.class);
		when(newTriggerMock.getKey()).thenReturn(new TriggerKey("name", "group"));
		try {
			//Store the Trigger
			js.storeTrigger(newTriggerMock, false);
			
		} catch (Exception e){
			fail("Unexpected Exception: ");
			e.printStackTrace();
		}
		assertTrue(triggerCache.containsKey("groupname"));
	}
	
	@Test
	public void testStoreTrigger_DontReplaceExisting() {
		OperableTrigger newTriggerMock = mock(OperableTrigger.class);
		when(newTriggerMock.getKey()).thenReturn(new TriggerKey("name", "group"));
		try {
			//Store the Trigger
			js.storeTrigger(newTriggerMock, false);
			//Store a Trigger with the same key
			js.storeTrigger(newTriggerMock, false);
			fail("An expected Exception has not been thrown");
		} catch (ObjectAlreadyExistsException oae){
			//Same key was not allowed the exception has been thrown
			assertTrue(true);
		} catch (Exception e){
			fail("Unexpected Exception: ");
			e.printStackTrace();
		}
	}
	
	@Test
	public void testStoreJobsAndTriggers_StoreJobsAndTriggers() {
		//Set up Triggers and Jobs mocks
		OperableTrigger triggerMock1 = mock(OperableTrigger.class);
		OperableTrigger triggerMock2 = mock(OperableTrigger.class);
		OperableTrigger triggerMock3 = mock(OperableTrigger.class);
		when(triggerMock1.getKey()).thenReturn(new TriggerKey("name1", "group"));
		when(triggerMock2.getKey()).thenReturn(new TriggerKey("name2", "group"));
		when(triggerMock3.getKey()).thenReturn(new TriggerKey("name3", "group"));
		
		JobDetail jobMock1 = mock(JobDetail.class);
		JobDetail jobMock2 = mock(JobDetail.class);
		when(jobMock1.getKey()).thenReturn(new JobKey("name1", "group"));
		when(jobMock2.getKey()).thenReturn(new JobKey("name2", "group"));
		//Setup data structure
		HashMap<JobDetail, Set<? extends Trigger>> triggersAndJobs = new HashMap<JobDetail, Set<? extends Trigger> >();
		//Add one Job with one trigger
		HashSet<OperableTrigger> triggerSet1 = new HashSet<OperableTrigger>();
		triggerSet1.add(triggerMock1);
		triggersAndJobs.put(jobMock1, triggerSet1);
		//Add one Job with two trigger
		HashSet<OperableTrigger> triggerSet2 = new HashSet<OperableTrigger>();
		triggerSet2.add(triggerMock2);
		triggerSet2.add(triggerMock3);
		triggersAndJobs.put(jobMock2, triggerSet2);
		try {
			//Store the structure
			js.storeJobsAndTriggers(triggersAndJobs, false);
			assertTrue(triggerCache.containsKey("groupname1"));
			assertTrue(triggerCache.containsKey("groupname2"));
			assertTrue(triggerCache.containsKey("groupname3"));
			assertTrue(jobCache.containsKey("groupname1"));
			assertTrue(jobCache.containsKey("groupname2"));
		} catch (Exception e){
			fail("Unexpected Exception: ");
			e.printStackTrace();
		}
	}
	
	@Test
	public void testRetrieveJob() {
		//Mock the Job
		JobDetail jdMock = mock(JobDetail.class);
		when(jdMock.getKey()).thenReturn(new JobKey("name", "group"));
		try {
			//Store the Job
			js.storeJob(jdMock, false);
			JobDetail retr = js.retrieveJob(new JobKey("name", "group"));
			//Test for same referenced object
			assertTrue(retr == jdMock);
		} catch (Exception e){
			fail("Unexpected Exception: ");
			e.printStackTrace();
		}
	}
	
	@Test
	public void testRetrieveTrigger() {
		OperableTrigger newTriggerMock = mock(OperableTrigger.class);
		when(newTriggerMock.getKey()).thenReturn(new TriggerKey("name", "group"));
		try {
			//Store the Trigger
			js.storeTrigger(newTriggerMock, false);
			//retrive the trigger and test for same reference
			OperableTrigger retT = js.retrieveTrigger(new TriggerKey("name", "group"));
			assertTrue(retT == newTriggerMock);
		} catch (Exception e){
			fail("Unexpected Exception: ");
			e.printStackTrace();
		}
	}
	
	@Test
	public void testReplaceTrigger() {
		//Mock two triggers
		OperableTrigger oldTriggerMock = mock(OperableTrigger.class);
		OperableTrigger newTriggerMock = mock(OperableTrigger.class);
		//setup Keys
		TriggerKey triggerKey = new TriggerKey("trigger", "group");
		JobKey jobKey = new JobKey("job", "group");
		//set same key and same job for both triggers
		when(oldTriggerMock.getKey()).thenReturn(triggerKey);
		when(oldTriggerMock.getJobKey()).thenReturn(jobKey);
		when(newTriggerMock.getKey()).thenReturn(triggerKey);
		when(newTriggerMock.getJobKey()).thenReturn(jobKey);
		
		try {
			js.storeTrigger(oldTriggerMock, false);
			js.replaceTrigger(triggerKey, newTriggerMock);
			//test if the trigger has been replaced
			OperableTrigger retT = js.retrieveTrigger(triggerKey);
			assertTrue(retT == newTriggerMock);
			assertTrue(retT != oldTriggerMock);
		} catch (Exception e){
			fail("Unexpected Exception: ");
			e.printStackTrace();
		}
	}
	
	@Test
	public void testReplaceTrigger_differenJob() {
		//Mock two triggers
		OperableTrigger oldTriggerMock = mock(OperableTrigger.class);
		OperableTrigger newTriggerMock = mock(OperableTrigger.class);
		//setup Keys
		TriggerKey triggerKey = new TriggerKey("trigger", "group");
		JobKey jobKey1 = new JobKey("job1", "group");
		JobKey jobKey2 = new JobKey("job2", "group");
		//set same key but different job for both triggers
		when(oldTriggerMock.getKey()).thenReturn(triggerKey);
		when(oldTriggerMock.getJobKey()).thenReturn(jobKey1);
		when(newTriggerMock.getKey()).thenReturn(triggerKey);
		when(newTriggerMock.getJobKey()).thenReturn(jobKey2);
		
		try {
			js.storeTrigger(oldTriggerMock, false);
			js.replaceTrigger(triggerKey, newTriggerMock);
			fail("Expected JobPersistenceException has not been thrown");
		}catch (JobPersistenceException jpe){ 
			//different Job trigger should not be replaced
			assertTrue(true);
		}catch (Exception e){
			fail("Unexpected Exception: ");
			e.printStackTrace();
		}
		try {
			OperableTrigger retT;
			retT = js.retrieveTrigger(triggerKey);
			assertTrue(retT != newTriggerMock);
			assertTrue(retT == oldTriggerMock);
		} catch (JobPersistenceException e) {
			fail("Unexpected Exception: ");
			e.printStackTrace();
		}
	}
	
	@Test
	public void testReplaceTrigger_nothingToReplace() {
		//Mock triggers
		OperableTrigger newTriggerMock = mock(OperableTrigger.class);
		TriggerKey triggerKey = new TriggerKey("trigger", "group");
		//set same key and same job for both triggers
		when(newTriggerMock.getKey()).thenReturn(triggerKey);
		when(newTriggerMock.getJobKey()).thenReturn(new JobKey("job", "group"));
		try {
			assertTrue(!js.replaceTrigger(triggerKey, newTriggerMock));
		} catch (Exception e){
			fail("Unexpected Exception: ");
			e.printStackTrace();
		}
	}
	
	@Test
	public void testClearAllSchedulingData() {
		OperableTrigger newTriggerMock = mock(OperableTrigger.class);
		TriggerKey triggerKey = new TriggerKey("trigger", "group");
		when(newTriggerMock.getKey()).thenReturn(triggerKey);
		
		JobDetail jobMock = mock(JobDetail.class);
		JobKey jobKey = new JobKey("job", "group");
		when(jobMock.getKey()).thenReturn(jobKey);
		
		Calendar calendarMock = mock(Calendar.class);
		try {
			js.schedulerStarted();
			js.storeJob(jobMock, false);
			js.storeTrigger(newTriggerMock, false);
			js.storeCalendar("calendar", calendarMock, false, false);
			assertTrue(js.retrieveJob(jobKey) != null);
			assertTrue(js.retrieveTrigger(triggerKey) != null);
			assertTrue(js.retrieveCalendar("calendar") != null);
			js.clearAllSchedulingData();
			//check if cleard
			assertTrue(js.retrieveJob(jobKey) == null);
			assertTrue(js.retrieveTrigger(triggerKey) == null);
			assertTrue(js.retrieveCalendar("calendar") == null);
		} catch (Exception e){
			fail("Unexpected Exception: ");
			e.printStackTrace();
		}
	}
	
	@Test
	public void testStoreCalendarAndRetrieve() {
		//Mock Calendar
		Calendar newCalendarMock = mock(Calendar.class);
		try {
			js.storeCalendar("calendar", newCalendarMock, false, false);
			assertTrue(js.retrieveCalendar("calendar") == newCalendarMock);
		} catch (Exception e){
			fail("Unexpected Exception: ");
			e.printStackTrace();
		}
	}
	
	@Test
	public void testStoreCalendar_replaceExisting() {
		//Mock Calendar
		Calendar oldCalendarMock = mock(Calendar.class);
		Calendar newCalendarMock = mock(Calendar.class);
		try {
			js.storeCalendar("calendar", oldCalendarMock, false, false);
			js.storeCalendar("calendar", newCalendarMock, true, false);
			assertTrue(js.retrieveCalendar("calendar") != oldCalendarMock);
			assertTrue(js.retrieveCalendar("calendar") == newCalendarMock);
		} catch (Exception e){
			fail("Unexpected Exception: ");
			e.printStackTrace();
		}
	}
	
	@Test
	public void testStoreCalendar_alreadyExistsException() {
		//Mock Calendar
		Calendar newCalendarMock = mock(Calendar.class);
		try {
			js.storeCalendar("calendar", newCalendarMock, false, false);
			js.storeCalendar("calendar", newCalendarMock, false, false);
			fail("Expected Exception has not been thrown");
		}catch (ObjectAlreadyExistsException oaee) {
			//Expected Exception has been thrown
			assertTrue(true);
		}catch (Exception e){
			fail("Unexpected Exception: ");
			e.printStackTrace();
		}
	}
	
	@Test
	public void testGetNumberOfJobs() {
		//Mock the Job
		JobDetail jdMock = mock(JobDetail.class);
		when(jdMock.getKey()).thenReturn(new JobKey("name", "group"));
		
		try {
			//Store the Job
			js.storeJob(jdMock, false);
			assertEquals(1, js.getNumberOfJobs());
		} catch (Exception e){
			fail("Unexpected Exception: ");
			e.printStackTrace();
		}
	}

	@Test
	public void testGetNumberOfTriggers() {
		//Mock Trigger
		OperableTrigger triggerMock = mock(OperableTrigger.class);
		when(triggerMock.getKey()).thenReturn(new TriggerKey("name", "group"));
		try {
			js.storeTrigger(triggerMock, false);
			assertEquals(1, js.getNumberOfTriggers());
		} catch (Exception e){
			fail("Unexpected Exception: ");
			e.printStackTrace();
		}
	}

	@Test
	public void testGetNumberOfCalendars() {
		//Mock Calendar
		Calendar calendar = mock(Calendar.class);
		try {
			js.storeCalendar("calendar", calendar, false, false);
			assertEquals(1, js.getNumberOfCalendars());
		} catch (Exception e){
			fail("Unexpected Exception: ");
			e.printStackTrace();
		}
	}
	
	@Test 
	public void testStoreCalendar_updateTriggers() {
		//Mock Calendar
		Calendar oldCalendarMock = mock(Calendar.class);
		Calendar newCalendarMock = mock(Calendar.class);
		//Mock Trigger
		OperableTrigger newTriggerMock = mock(OperableTrigger.class);
		TriggerKey key = new TriggerKey("name", "group");
		when(newTriggerMock.getCalendarName()).thenReturn("calendar");
		when(newTriggerMock.getKey()).thenReturn(key);
		try {
			js.storeTrigger(newTriggerMock, false);
			js.storeCalendar("calendar", oldCalendarMock, false, false);
			js.storeCalendar("calendar", newCalendarMock, true, true);
			Mockito.verify(newTriggerMock).updateWithNewCalendar(newCalendarMock, js.getMisfireThreshold());
		} catch (Exception e){
			fail("Unexpected Exception: ");
			e.printStackTrace();
		}
	}
	
	@Test //Package scope test
	public void testGetTriggerForCalendar(){
		OperableTrigger triggerMock1 = mock(OperableTrigger.class);
		OperableTrigger triggerMock2 = mock(OperableTrigger.class);
		OperableTrigger triggerMock3 = mock(OperableTrigger.class);
		//trigger 1, 2 use calendar1
		when(triggerMock1.getCalendarName()).thenReturn("calendar1");
		when(triggerMock2.getCalendarName()).thenReturn("calendar1");
		when(triggerMock3.getCalendarName()).thenReturn("calendar2");
		when(triggerMock1.getKey()).thenReturn(new TriggerKey("name1", "group"));
		when(triggerMock2.getKey()).thenReturn(new TriggerKey("name2", "group"));
		when(triggerMock3.getKey()).thenReturn(new TriggerKey("name3", "group"));
		try {
			js.storeTrigger(triggerMock1, false);
			js.storeTrigger(triggerMock2, false);
			js.storeTrigger(triggerMock3, false);
			Set<String> erg = js.getTriggerForCalendar("calendar1");
			assertTrue(erg.contains("groupname1"));
			assertTrue(erg.contains("groupname2"));
			assertTrue(!erg.contains("groupname3"));
		} catch (Exception e){
			fail("Unexpected Exception: ");
			e.printStackTrace();
		}
		
	}
	
	@Test
	public void testRemoveCalendar() {
		//Mock Calendar
		Calendar newCalendarMock = mock(Calendar.class);
		try {
			js.storeCalendar("calendar", newCalendarMock, false, false);
			js.removeCalendar("calendar");
			assertTrue(js.retrieveCalendar("calendar")==null);
		}catch (Exception e){
			fail("Unexpected Exception: ");
			e.printStackTrace();
		}
	}
	
	@Test
	public void testGetTriggersForJob() {
		//setup the Triggers
		OperableTrigger triggerMock1 = mock(OperableTrigger.class);
		OperableTrigger triggerMock2 = mock(OperableTrigger.class);
		OperableTrigger triggerMock3 = mock(OperableTrigger.class);
		when(triggerMock1.getKey()).thenReturn(new TriggerKey("name1", "group"));
		when(triggerMock2.getKey()).thenReturn(new TriggerKey("name2", "group"));
		when(triggerMock3.getKey()).thenReturn(new TriggerKey("name3", "group"));
		//Trigger 1, 2 are referenced to job1 and trigger 3 is referenced to job2
		when(triggerMock1.getJobKey()).thenReturn(new JobKey("job1", "group"));
		when(triggerMock2.getJobKey()).thenReturn(new JobKey("job1", "group"));
		when(triggerMock3.getJobKey()).thenReturn(new JobKey("job2", "group"));
		try {
			//Add the triggers through the JobStore
			js.storeTrigger(triggerMock1, false);
			js.storeTrigger(triggerMock2, false);
			js.storeTrigger(triggerMock3, false);
		} catch (Exception e){
			fail("Unexpected Exception in Methode not tested here: ");
			e.printStackTrace();
		}
		try {
			List<OperableTrigger> triggersList = js.getTriggersForJob(new JobKey("job1", "group"));
			
			assertTrue(triggersList.contains(triggerMock1));
			assertTrue(triggersList.contains(triggerMock2));
			assertTrue(!triggersList.contains(triggerMock3));
			
		} catch (JobPersistenceException e) {
			fail("Unexpected Exception: ");
			e.printStackTrace();
		}
	}
	
	@Test
	public void testRemoveJob_noSuchJob() {
		//Mock the Job
		JobDetail jdMock = mock(JobDetail.class);
		when(jdMock.getKey()).thenReturn(new JobKey("name", "group"));
		try {
			assertTrue(!js.removeJob(new JobKey("name", "group")));
		} catch (Exception e){
			fail("Unexpected Exception: ");
			e.printStackTrace();
		}
	}

	@Test 
	public void testRemoveJob_remove() {
		//Mock the Job
		JobDetail jdMock = mock(JobDetail.class);
		when(jdMock.getKey()).thenReturn(new JobKey("name", "group"));
		try {
			//Store the Job
			js.storeJob(jdMock, false);
			assertTrue(js.removeJob(new JobKey("name", "group")));
		} catch (Exception e){
			fail("Unexpected Exception: ");
			e.printStackTrace();
		}
	}
	
	@Test
	public void testRemoveTrigger_remove() {
		//Mock Job
		JobDetail jobMock = mock(JobDetail.class);
		JobKey jk = new JobKey("job", "group");
		when(jobMock.getKey()).thenReturn(jk);
		when(jobMock.isDurable()).thenReturn(true);
		//Mock the Trigger
		OperableTrigger triggerMock = mock(OperableTrigger.class);
		when(triggerMock.getKey()).thenReturn(new TriggerKey("name", "group"));
		when(triggerMock.getJobKey()).thenReturn(jk);
		
		
		try {
			//Store the Trigger
			js.storeJob(jobMock, false);
			js.storeTrigger(triggerMock, false);
			assertTrue(js.removeTrigger(new TriggerKey("name", "group")));
		} catch (Exception e){
			fail("Unexpected Exception: ");
			e.printStackTrace();
		}
	}
	
	@Test
	public void testRemoveTrigger_noSuchTrigger() { 
		//Mock the Trigger
		OperableTrigger jdMock = mock(OperableTrigger.class);
		when(jdMock.getKey()).thenReturn(new TriggerKey("name", "group"));
		try {
			assertTrue(!js.removeTrigger(new TriggerKey("name", "group")));
		} catch (Exception e){
			fail("Unexpected Exception: ");
			e.printStackTrace();
		}
	}
	
	@Test
	public void testGetJobGroupNames() {
		//Mock Job
		JobDetail jobMock1 = mock(JobDetail.class);
		JobDetail jobMock2 = mock(JobDetail.class);
		when(jobMock1.getKey()).thenReturn(new JobKey("job", "group1"));
		when(jobMock2.getKey()).thenReturn(new JobKey("job", "group2"));
		try{
			js.storeJob(jobMock1, false);
			js.storeJob(jobMock2, false);
			List<String> valList = js.getJobGroupNames();
			assertTrue(valList.contains("group1"));
			assertTrue(valList.contains("group2"));
			assertTrue(!valList.contains("group3"));
		} catch(Exception e){
			fail("Unexpected Exception: ");
			e.printStackTrace();
		}
	}
	
	@Test 
	public void testGetTriggerGroupNames() {
		OperableTrigger triggerMock1 = mock(OperableTrigger.class);
		OperableTrigger triggerMock2 = mock(OperableTrigger.class);
		when(triggerMock1.getKey()).thenReturn(new TriggerKey("name", "group5"));
		when(triggerMock2.getKey()).thenReturn(new TriggerKey("name", "group7"));
		try{
			js.storeTrigger(triggerMock1, false);
			js.storeTrigger(triggerMock2, false);
			List<String> valList = js.getTriggerGroupNames();
			assertTrue(valList.contains("group5"));
			assertTrue(valList.contains("group7"));
			assertTrue(!valList.contains("group4"));
		} catch(Exception e){
			fail("Unexpected Exception: ");
			e.printStackTrace();
		}
	}
	
	@Test
	public void testGetTriggerState_NONE() {
		try {
			assertTrue(js.getTriggerState(new TriggerKey("some", "key")) == TriggerState.NONE);
		} catch(Exception e){
			fail("Unexpected Exception: ");
			e.printStackTrace();
		}
	}
	
	@Test
	public void testGetTriggerState_NORMAL() {
		OperableTrigger triggerMock1 = mock(OperableTrigger.class);
		when(triggerMock1.getKey()).thenReturn(new TriggerKey("trigger", "group"));
		try {
			js.storeTrigger(triggerMock1, false);
			assertTrue(js.getTriggerState(new TriggerKey("trigger", "group")) == TriggerState.NORMAL);
		} catch(Exception e){
			fail("Unexpected Exception: ");
			e.printStackTrace();
		}
	}
	
	@Test
	public void testGetTriggerState_BLOCKED() {
		OperableTrigger triggerMock1 = mock(OperableTrigger.class);
		TriggerKey triggerKey = new TriggerKey("trigger", "group");
		when(triggerMock1.getKey()).thenReturn(triggerKey);
		try {
			js.storeTrigger(triggerMock1, false);
			InfinispanJobStoreSingleServerTest.triggerCache
				.get(ClusterCacheJobStore.generateKey(triggerKey))
				.state = TriggerWrapper.STATE_BLOCKED;
			assertTrue(js.getTriggerState(triggerKey) == TriggerState.BLOCKED);
		} catch(Exception e){
			fail("Unexpected Exception: ");
			e.printStackTrace();
		}
	}
	
	@Test
	public void testGetTriggerState_PAUSED_andPauseTrigger() {
		OperableTrigger triggerMock1 = mock(OperableTrigger.class);
		TriggerKey tk = new TriggerKey("trigger", "group");
		when(triggerMock1.getKey()).thenReturn(tk);
		try {
			js.storeTrigger(triggerMock1, false);
			js.pauseTrigger(tk);
			assertTrue(js.getTriggerState(tk) == TriggerState.PAUSED);
		} catch(Exception e){
			fail("Unexpected Exception: ");
			e.printStackTrace();
		}
	}
	
	@Test
	public void testGetTriggerKeys() {
		OperableTrigger triggerMock1 = mock(OperableTrigger.class);
		OperableTrigger triggerMock2 = mock(OperableTrigger.class);
		OperableTrigger triggerMock3 = mock(OperableTrigger.class);
		TriggerKey tk1 = new TriggerKey("name1", "group");
		TriggerKey tk2 = new TriggerKey("name2", "group");
		TriggerKey tk3 = new TriggerKey("name3", "otherGroup");
		when(triggerMock1.getKey()).thenReturn(tk1);
		when(triggerMock2.getKey()).thenReturn(tk2);
		when(triggerMock3.getKey()).thenReturn(tk3);
		try{
			js.storeTrigger(triggerMock1, false);
			js.storeTrigger(triggerMock3, false);
			js.storeTrigger(triggerMock2, false);
			Set<TriggerKey> res = js.getTriggerKeys(GroupMatcher.triggerGroupEquals("group"));
			assertTrue(res.contains(tk1));
			assertTrue(res.contains(tk2));
			assertTrue(!res.contains(tk3));
		} catch(Exception e){
			fail("Unexpected Exception: ");
			e.printStackTrace();
		}
		
	}
	
	@Test
	public void testPauseTriggers() {
		OperableTrigger triggerMock1 = mock(OperableTrigger.class);
		OperableTrigger triggerMock2 = mock(OperableTrigger.class);
		OperableTrigger triggerMock3 = mock(OperableTrigger.class);
		TriggerKey tk1 = new TriggerKey("name1", "group");
		TriggerKey tk2 = new TriggerKey("name2", "group");
		TriggerKey tk3 = new TriggerKey("name3", "otherGroup");
		when(triggerMock1.getKey()).thenReturn(tk1);
		when(triggerMock2.getKey()).thenReturn(tk2);
		when(triggerMock3.getKey()).thenReturn(tk3);
		try{
			js.storeTrigger(triggerMock1, false);
			js.storeTrigger(triggerMock3, false);
			js.storeTrigger(triggerMock2, false);
			//pause group
			js.pauseTriggers(GroupMatcher.triggerGroupEquals("group"));
			assertTrue(js.getTriggerState(tk1) == TriggerState.PAUSED);
			assertTrue(js.getTriggerState(tk2) == TriggerState.PAUSED);
			assertTrue(js.getTriggerState(tk3) != TriggerState.PAUSED);
		} catch(Exception e){
			fail("Unexpected Exception: ");
			e.printStackTrace();
		}
	}
	
	@Test
	public void testStoreTrigger_storeInPausedGroup() {
		OperableTrigger triggerMock1 = mock(OperableTrigger.class);
		OperableTrigger triggerMock2 = mock(OperableTrigger.class);
		OperableTrigger triggerMock3 = mock(OperableTrigger.class);
		TriggerKey tk1 = new TriggerKey("name1", "group");
		TriggerKey tk2 = new TriggerKey("name2", "group");
		TriggerKey tk3 = new TriggerKey("name3", "group");
		when(triggerMock1.getKey()).thenReturn(tk1);
		when(triggerMock2.getKey()).thenReturn(tk2);
		when(triggerMock3.getKey()).thenReturn(tk3);
		try{
			//add trigger 1 and 2
			js.storeTrigger(triggerMock1, false);
			js.storeTrigger(triggerMock2, false);
			//pause group
			js.pauseTriggers(GroupMatcher.triggerGroupEquals("group"));
			//add trigger 3 with the same (paused) group
			js.storeTrigger(triggerMock3, false);
			assertTrue(js.getTriggerState(tk3) == TriggerState.PAUSED);
		} catch(Exception e){
			fail("Unexpected Exception: ");
			e.printStackTrace();
		}
	}
	
	@Test
	public void testGetPausedTriggerGroups() {
		OperableTrigger triggerMock1 = mock(OperableTrigger.class);
		TriggerKey tk1 = new TriggerKey("name1", "group");
		when(triggerMock1.getKey()).thenReturn(tk1);
		try{
			js.storeTrigger(triggerMock1, false);
			js.pauseTriggers(GroupMatcher.triggerGroupEquals("group"));
			assertTrue(js.getPausedTriggerGroups().contains("group"));
			assertTrue(js.getPausedTriggerGroups().size() == 1);
		} catch(Exception e){
			fail("Unexpected Exception: ");
			e.printStackTrace();
		}
	}
	
	@Test
	public void testResumeTrigger() {
		OperableTrigger triggerMock1 = mock(OperableTrigger.class);
		TriggerKey tk1 = new TriggerKey("name1", "group");
		when(triggerMock1.getKey()).thenReturn(tk1);
		try{
			js.storeTrigger(triggerMock1, false);
			//pause group
			js.pauseTrigger(tk1);
			assertTrue(js.getTriggerState(tk1) == TriggerState.PAUSED);
			js.resumeTrigger(tk1);
			assertTrue(js.getTriggerState(tk1) == TriggerState.NORMAL);
		} catch(Exception e){
			fail("Unexpected Exception: ");
			e.printStackTrace();
		}
	}
	
	@Test
	public void testResumeTrigger_NoTrigger() {
		TriggerKey tk1 = new TriggerKey("name1", "group");
		try{
			js.resumeTrigger(tk1);
			//No such trigger no exception
			assertTrue(true);
		} catch(Exception e){
			fail("Unexpected Exception: ");
			e.printStackTrace();
		}
	}
	
	@Test
	public void testResumeTriggers() {
		OperableTrigger triggerMock1 = mock(OperableTrigger.class);
		OperableTrigger triggerMock2 = mock(OperableTrigger.class);
		OperableTrigger triggerMock3 = mock(OperableTrigger.class);
		OperableTrigger triggerMock4 = mock(OperableTrigger.class);
		TriggerKey tk1 = new TriggerKey("name1", "group1");
		TriggerKey tk2 = new TriggerKey("name2", "group1");
		TriggerKey tk3 = new TriggerKey("name3", "group2");
		TriggerKey tk4 = new TriggerKey("name4", "group3");
		when(triggerMock1.getKey()).thenReturn(tk1);
		when(triggerMock2.getKey()).thenReturn(tk2);
		when(triggerMock3.getKey()).thenReturn(tk3);
		when(triggerMock4.getKey()).thenReturn(tk4);
		try{
			js.storeTrigger(triggerMock1, false);
			js.storeTrigger(triggerMock2, false);
			js.storeTrigger(triggerMock3, false);
			js.storeTrigger(triggerMock4, false);
			js.pauseTriggers(GroupMatcher.triggerGroupEquals("group1"));
			js.pauseTriggers(GroupMatcher.triggerGroupEquals("group1"));
			js.pauseTriggers(GroupMatcher.triggerGroupEquals("group2"));
			js.pauseTriggers(GroupMatcher.triggerGroupEquals("group3"));
			js.resumeTriggers(GroupMatcher.triggerGroupEquals("group1"));
			Collection<String> resumedGroups = js.resumeTriggers(GroupMatcher.triggerGroupEquals("group2"));
			assertTrue(resumedGroups.contains("group2"));
			Set<String> pausedGroups = js.getPausedTriggerGroups();
			assertTrue(pausedGroups.contains("group3"));
			assertTrue(!pausedGroups.contains("group1"));
			assertTrue(js.getTriggerState(tk3) == TriggerState.NORMAL);
		} catch(Exception e){
			fail("Unexpected Exception: ");
			e.printStackTrace();
		}
	}
	
	@Test
	public void testPauseJob() {
		//setup the Triggers
		OperableTrigger triggerMock1 = mock(OperableTrigger.class);
		OperableTrigger triggerMock2 = mock(OperableTrigger.class);
		OperableTrigger triggerMock3 = mock(OperableTrigger.class);
		TriggerKey tk1 = new TriggerKey("name1", "group");
		TriggerKey tk2 = new TriggerKey("name2", "group");
		TriggerKey tk3 = new TriggerKey("name3", "group");
		when(triggerMock1.getKey()).thenReturn(tk1);
		when(triggerMock2.getKey()).thenReturn(tk2);
		when(triggerMock3.getKey()).thenReturn(tk3);
		//Trigger 1, 2 are referenced to job1 and trigger 3 is referenced to job2
		JobKey jk1 = new JobKey("job1", "group");
		JobKey jk2 = new JobKey("job2", "group");
		when(triggerMock1.getJobKey()).thenReturn(jk1);
		when(triggerMock2.getJobKey()).thenReturn(jk1);
		when(triggerMock3.getJobKey()).thenReturn(jk2);
		
		//pausing job1 should pause trigger 1 and 2
		try {
			js.storeTrigger(triggerMock1, false);
			js.storeTrigger(triggerMock2, false);
			js.storeTrigger(triggerMock3, false);
			js.pauseJob(jk1);
			assertTrue(js.getTriggerState(tk1) == TriggerState.PAUSED);
			assertTrue(js.getTriggerState(tk2) == TriggerState.PAUSED);
			assertTrue(js.getTriggerState(tk3) == TriggerState.NORMAL);
		} catch (ObjectAlreadyExistsException e) {
			fail("Unexpected Exception: ");
			e.printStackTrace();
		} catch (JobPersistenceException e) {
			fail("Unexpected Exception: ");
			e.printStackTrace();
		}
	}
	
	@Test
	public void testResumeJob() {
		//setup the Triggers
		OperableTrigger triggerMock1 = mock(OperableTrigger.class);
		OperableTrigger triggerMock2 = mock(OperableTrigger.class);
		OperableTrigger triggerMock3 = mock(OperableTrigger.class);
		TriggerKey tk1 = new TriggerKey("name1", "group");
		TriggerKey tk2 = new TriggerKey("name2", "group");
		TriggerKey tk3 = new TriggerKey("name3", "group");
		when(triggerMock1.getKey()).thenReturn(tk1);
		when(triggerMock2.getKey()).thenReturn(tk2);
		when(triggerMock3.getKey()).thenReturn(tk3);
		//Trigger 1, 2 are referenced to job1 and trigger 3 is referenced to job2
		JobKey jk1 = new JobKey("job1", "group");
		JobKey jk2 = new JobKey("job2", "group");
		when(triggerMock1.getJobKey()).thenReturn(jk1);
		when(triggerMock2.getJobKey()).thenReturn(jk1);
		when(triggerMock3.getJobKey()).thenReturn(jk2);
		
		//pausing job1 should pause trigger 1 and 2
		try {
			js.storeTrigger(triggerMock1, false);
			js.storeTrigger(triggerMock2, false);
			js.storeTrigger(triggerMock3, false);
			js.pauseJob(jk1);
			js.pauseJob(jk2);
			js.resumeJob(jk1);
			assertTrue(js.getTriggerState(tk1) == TriggerState.NORMAL);
			assertTrue(js.getTriggerState(tk2) == TriggerState.NORMAL);
			assertTrue(js.getTriggerState(tk3) == TriggerState.PAUSED);
		} catch (ObjectAlreadyExistsException e) {
			fail("Unexpected Exception: ");
			e.printStackTrace();
		} catch (JobPersistenceException e) {
			fail("Unexpected Exception: ");
			e.printStackTrace();
		}
	}
	
	@Test
	public void testPauseJobs() {
		//Mock the Job
		JobDetail jd1 = mock(JobDetail.class);
		JobDetail jd2 = mock(JobDetail.class);
		JobKey jk1 = new JobKey("name1", "group");
		JobKey jk2 = new JobKey("name2", "group");
		when(jd1.getKey()).thenReturn(jk1);
		when(jd2.getKey()).thenReturn(jk2);
		//Mock triggers
		OperableTrigger triggerMock1 = mock(OperableTrigger.class);
		OperableTrigger triggerMock2 = mock(OperableTrigger.class);
		TriggerKey tk1 = new TriggerKey("name1", "group");
		TriggerKey tk2 = new TriggerKey("name2", "group");
		//All trigger 1 references job 1, trigger 2 references job 2
		when(triggerMock1.getJobKey()).thenReturn(jk1);
		when(triggerMock2.getJobKey()).thenReturn(jk2);
		when(triggerMock1.getKey()).thenReturn(tk1);
		when(triggerMock2.getKey()).thenReturn(tk2);
		try {
			js.storeTrigger(triggerMock1, false);
			js.storeJob(jd1, false);
			js.storeTrigger(triggerMock2, false);
			js.storeJob(jd2, false);
			//Pause the group
			Collection<String> pausedGroups = js.pauseJobs(GroupMatcher.jobGroupEquals("group"));
			assertTrue(js.getTriggerState(tk2) == TriggerState.PAUSED);
			assertTrue(js.getTriggerState(tk1) == TriggerState.PAUSED);
			assertTrue(pausedGroups.contains("group"));
		} catch (Exception e){
			fail("Unexpected Exception: ");
			e.printStackTrace();
		}
	}
	
	@Test
	public void testResumeJobs() {
		//Mock the Job
		JobDetail jd1 = mock(JobDetail.class);
		JobKey jk1 = new JobKey("name1", "group");
		when(jd1.getKey()).thenReturn(jk1);
		//Mock triggers
		OperableTrigger triggerMock1 = mock(OperableTrigger.class);
		TriggerKey tk1 = new TriggerKey("name1", "group");
		//All trigger 1 references job 1, trigger 2 references job 2
		when(triggerMock1.getJobKey()).thenReturn(jk1);
		when(triggerMock1.getKey()).thenReturn(tk1);
		try {
			js.storeTrigger(triggerMock1, false);
			js.storeJob(jd1, false);
			//Pause the group
			js.pauseJobs(GroupMatcher.jobGroupEquals("group"));
			Collection<String> resumedGroups = js.resumeJobs(GroupMatcher.jobGroupEquals("group"));
			assertTrue(js.getTriggerState(tk1) != TriggerState.PAUSED);
			assertTrue(resumedGroups.contains("group"));
		} catch (Exception e){
			fail("Unexpected Exception: ");
			e.printStackTrace();
		}
	}
	
	@Test
	public void testResumeAll() {
		OperableTrigger triggerMock1 = mock(OperableTrigger.class);
		OperableTrigger triggerMock2 = mock(OperableTrigger.class);
		OperableTrigger triggerMock3 = mock(OperableTrigger.class);
		OperableTrigger triggerMock4 = mock(OperableTrigger.class);
		TriggerKey tk1 = new TriggerKey("name1", "group1");
		TriggerKey tk2 = new TriggerKey("name2", "group1");
		TriggerKey tk3 = new TriggerKey("name3", "group2");
		TriggerKey tk4 = new TriggerKey("name4", "group3");
		when(triggerMock1.getKey()).thenReturn(tk1);
		when(triggerMock2.getKey()).thenReturn(tk2);
		when(triggerMock3.getKey()).thenReturn(tk3);
		when(triggerMock4.getKey()).thenReturn(tk4);
		try{
			js.storeTrigger(triggerMock1, false);
			js.storeTrigger(triggerMock2, false);
			js.storeTrigger(triggerMock3, false);
			js.storeTrigger(triggerMock4, false);
			js.pauseTriggers(GroupMatcher.triggerGroupEquals("group1"));
			js.pauseTriggers(GroupMatcher.triggerGroupEquals("group1"));
			js.pauseTriggers(GroupMatcher.triggerGroupEquals("group2"));
			js.pauseTriggers(GroupMatcher.triggerGroupEquals("group3"));
			js.resumeAll();
			assertTrue(js.getTriggerState(tk1)==TriggerState.NORMAL);
			assertTrue(js.getTriggerState(tk2)==TriggerState.NORMAL);
			assertTrue(js.getTriggerState(tk3)==TriggerState.NORMAL);
			assertTrue(js.getTriggerState(tk4)==TriggerState.NORMAL);
			assertTrue(js.pauseJobs(GroupMatcher.anyJobGroup()).isEmpty());
		} catch(Exception e){
			fail("Unexpected Exception: ");
			e.printStackTrace();
		}
	}
	
	@Test
	public void testApplyMisfire_NoNextFireTime(){
		//Mock the Trigger
		OperableTrigger triggerMock1 = mock(OperableTrigger.class);
		TriggerKey tk1 = new TriggerKey("name1", "group");
		TriggerWrapper tw = new TriggerWrapper(triggerMock1);
		when(triggerMock1.getKey()).thenReturn(tk1);
		//Mock the fireTime to no next Firetime
		when(triggerMock1.getNextFireTime()).thenReturn(null);
		
		//Mock the time
		TimeOfSystem systemTimeMock = mock(TimeOfSystem.class);
		when(systemTimeMock.currentTimeInMilliSeconds()).thenReturn(0L);
		js.setSystemTime(systemTimeMock);
		
		try {
			js.storeTrigger(triggerMock1, false);
			//no misfire should be performed because there is no next FireTime
			assertFalse(applyMisfire(js, tw));
		} catch(Exception e){
			fail("Unexpected Exception: ");
			e.printStackTrace();
		}
	}
	
	private boolean applyMisfire(ClusterCacheJobStore jobStr, TriggerWrapper tw) throws NoSuchMethodException, SecurityException, IllegalAccessException, IllegalArgumentException, InvocationTargetException{
		
		Method applyMisfire = ClusterCacheJobStore.class.getDeclaredMethod("applyMisfire", TriggerWrapper.class);
		
		applyMisfire.setAccessible(true);
		
		boolean retval = (boolean) applyMisfire.invoke(jobStr, tw);
		
		return retval;
	}
	
	@Test
	public void testApplyMisfire_nextFireTimeHigherThanMissfireTime(){
		//Mock the Trigger
		OperableTrigger triggerMock1 = mock(OperableTrigger.class);
		TriggerKey tk1 = new TriggerKey("name1", "group");
		TriggerWrapper tw = new TriggerWrapper(triggerMock1);
		when(triggerMock1.getKey()).thenReturn(tk1);
		//Mock the time
		TimeOfSystem systemTimeMock = mock(TimeOfSystem.class);
		//Mock the fireTime to 100 while the current time is only 50 -> no misfire 
		when(triggerMock1.getNextFireTime()).thenReturn(new Date(100));
		when(systemTimeMock.currentTimeInMilliSeconds()).thenReturn(50L);
		
		js.setSystemTime(systemTimeMock);
		try {
			js.storeTrigger(triggerMock1, false);
			//no misfire should be performed
			assertFalse(applyMisfire(js, tw));
		} catch(Exception e){
			fail("Unexpected Exception: ");
			e.printStackTrace();
		}
	}
	
	@Test
	public void testApplyMisfire_hasMissfiredButShouldIgnoreMissfire(){
		//Mock the Trigger
		OperableTrigger triggerMock1 = mock(OperableTrigger.class);
		TriggerKey tk1 = new TriggerKey("name1", "group");
		TriggerWrapper tw = new TriggerWrapper(triggerMock1);
		when(triggerMock1.getKey()).thenReturn(tk1);
		//Mock the time
		TimeOfSystem systemTimeMock = mock(TimeOfSystem.class);
		//Mock the fireTime to 50 while the current time is 100 -> misfire 
		when(triggerMock1.getNextFireTime()).thenReturn(new Date(50));
		when(systemTimeMock.currentTimeInMilliSeconds()).thenReturn(100L);
		//Ignore missfires
		when(triggerMock1.getMisfireInstruction()).thenReturn(Trigger.MISFIRE_INSTRUCTION_IGNORE_MISFIRE_POLICY);
		js.setSystemTime(systemTimeMock);
		try {
			js.storeTrigger(triggerMock1, false);
			//no misfire should be performed
			assertFalse(applyMisfire(js, tw));
		} catch(Exception e){
			fail("Unexpected Exception: ");
			e.printStackTrace();
		}
	}
	
	@Test
	public void testApplyMisfire_missfireNoCalendar(){
		//Mock the Trigger
		OperableTrigger triggerMock1 = mock(OperableTrigger.class);
		TriggerKey tk1 = new TriggerKey("name1", "group");
		TriggerWrapper tw = new TriggerWrapper(triggerMock1);
		when(triggerMock1.getKey()).thenReturn(tk1);
		//Mock the time
		TimeOfSystem systemTimeMock = mock(TimeOfSystem.class);
		//Setup the mocks for the Trigger to misfired
		when(triggerMock1.getNextFireTime()).thenReturn(new Date(50L));
		when(systemTimeMock.currentTimeInMilliSeconds()).thenReturn(10000000L);
		when(triggerMock1.getMisfireInstruction()).thenReturn(Trigger.MISFIRE_INSTRUCTION_SMART_POLICY);
		js.setSystemTime(systemTimeMock);
		//Mock Signaler 
		SchedulerSignaler signaler = mock(SchedulerSignaler.class);
		js.setSchedulerSignaler(signaler);
		try {
			js.storeTrigger(triggerMock1, false);
			assertTrue(applyMisfire(js, tw));
			Mockito.verify(signaler).notifyTriggerListenersMisfired(triggerMock1);
		} catch(Exception e){
			fail("Unexpected Exception: ");
			e.printStackTrace();
		}
	}

	@SuppressWarnings("all") //because: .thenReturn(new Date(50L), null);
	@Test
	public void testApplyMisfire_missfireNoCalendar_TriggerComplete(){
		//Mock the Trigger
		OperableTrigger triggerMock1 = mock(OperableTrigger.class);
		TriggerKey tk1 = new TriggerKey("name1", "group");
		TriggerWrapper tw = new TriggerWrapper(triggerMock1);
		when(triggerMock1.getKey()).thenReturn(tk1);
		//Mock the time
		TimeOfSystem systemTimeMock = mock(TimeOfSystem.class);
		//Setup the mocks for the Trigger to misfire and tell the Job Store that there is no further fire time
		when(triggerMock1.getNextFireTime()).thenReturn(new Date(50L), null);
		when(systemTimeMock.currentTimeInMilliSeconds()).thenReturn(10000000L);
		when(triggerMock1.getMisfireInstruction()).thenReturn(Trigger.MISFIRE_INSTRUCTION_SMART_POLICY);
		js.setSystemTime(systemTimeMock);
		//Mock Signaler 
		SchedulerSignaler signaler = mock(SchedulerSignaler.class);
		js.setSchedulerSignaler(signaler);
		try {
			js.storeTrigger(triggerMock1, false);
			assertTrue(applyMisfire(js, tw));
			assertEquals(TriggerState.COMPLETE, js.getTriggerState(tk1));			
			Mockito.verify(signaler).notifyTriggerListenersMisfired(triggerMock1);
			Mockito.verify(signaler).notifySchedulerListenersFinalized(triggerMock1);
		} catch(Exception e){
			fail("Unexpected Exception: ");
			e.printStackTrace();
		}
	}
	
	@Test
	public void testApplyMisfire_missfireNoCalendar_UpdateCalendar(){
		//Mock the Trigger
		OperableTrigger triggerMock1 = mock(OperableTrigger.class);
		TriggerKey tk1 = new TriggerKey("name1", "group");
		TriggerWrapper tw = new TriggerWrapper(triggerMock1);
		when(triggerMock1.getKey()).thenReturn(tk1);
		//Mock the time
		TimeOfSystem systemTimeMock = mock(TimeOfSystem.class);
		//Setup the mocks for the Trigger to misfired
		when(triggerMock1.getNextFireTime()).thenReturn(new Date(50L));
		when(systemTimeMock.currentTimeInMilliSeconds()).thenReturn(10000000L);
		when(triggerMock1.getMisfireInstruction()).thenReturn(Trigger.MISFIRE_INSTRUCTION_SMART_POLICY);
		js.setSystemTime(systemTimeMock);
		//Mock Signaler 
		SchedulerSignaler signaler = mock(SchedulerSignaler.class);
		js.setSchedulerSignaler(signaler);
		//Mock Calendar
		Calendar calendarMock = mock(Calendar.class);
		String calendarName = "calendarName";
		when(triggerMock1.getCalendarName()).thenReturn(calendarName);
		try {
			js.storeTrigger(triggerMock1, false);
			js.storeCalendar(calendarName, calendarMock, false, false);
			assertTrue(applyMisfire(js, tw));
			Mockito.verify(signaler).notifyTriggerListenersMisfired(triggerMock1);
			Mockito.verify(triggerMock1).updateAfterMisfire(calendarMock);
		} catch(Exception e){
			fail("Unexpected Exception: ");
			e.printStackTrace();
		}
	}
	
	@Test
	public void testAcquireNextTriggers_NoTriggers() {
		try {
			List<OperableTrigger> res = js.acquireNextTriggers(0, 0, 0);
			assertTrue(res.isEmpty());
		} catch (JobPersistenceException e) {
			fail("Unexpected Exception: ");
			e.printStackTrace();
		}
	}
	
	@Test
	public void testAcquireNextTriggers_containsStoredTrigger() {
		OperableTrigger triggerMock = mock(OperableTrigger.class);
		TriggerKey triggerKey = new TriggerKey("name", "group");
		when(triggerMock.getKey()).thenReturn(triggerKey);
		//Mock Time for Missfire Instruction
		TimeOfSystem systemTimeMock = mock(TimeOfSystem.class);
		js.setSystemTime(systemTimeMock);
		Date currentTime = new Date(1000L);
		Date noMisfireTime = new Date(1200L);
		when(systemTimeMock.currentTimeInMilliSeconds()).thenReturn(currentTime.getTime());
		when(triggerMock.getNextFireTime()).thenReturn(noMisfireTime);
		when(triggerMock.getMisfireInstruction()).thenReturn(Trigger.MISFIRE_INSTRUCTION_SMART_POLICY);
		//Mock the Job and ignore concurrency
		JobDetail jobDetailMock = mock(JobDetail.class);
		JobKey jk1 = new JobKey("job1", "g1");
		when(jobDetailMock.getKey()).thenReturn(jk1);
		when(triggerMock.getJobKey()).thenReturn(jk1);
		when(jobDetailMock.isConcurrentExecutionDisallowed()).thenReturn(false);
		try {
			js.storeTrigger(triggerMock, false);
			js.storeJob(jobDetailMock, false);
			//get one trigger
			List<OperableTrigger> acquiredTriggers = js.acquireNextTriggers(0, 1, 1000);
			//Test if the trigger has been returned
			assertTrue(acquiredTriggers.get(0)==triggerMock);
		} catch (JobPersistenceException e) {
			fail("Unexpected Exception: ");
			e.printStackTrace();
		}
	}

	
	@Test
	public void testAcquireNextTriggers_noMoreThanCount() {
		OperableTrigger triggerMock1 = mock(OperableTrigger.class);
		OperableTrigger triggerMock2 = mock(OperableTrigger.class);
		TriggerKey triggerKey1 = new TriggerKey("name1", "group");
		TriggerKey triggerKey2 = new TriggerKey("name2", "group");
		when(triggerMock1.getKey()).thenReturn(triggerKey1);
		when(triggerMock2.getKey()).thenReturn(triggerKey2);
		//Mock Time for Missfire Instruction
		TimeOfSystem systemTimeMock = mock(TimeOfSystem.class);
		js.setSystemTime(systemTimeMock);
		Date currentTime = new Date(1000L);
		Date noMisfireTime = new Date(1200L);
		when(systemTimeMock.currentTimeInMilliSeconds()).thenReturn(currentTime.getTime());
		when(triggerMock1.getNextFireTime()).thenReturn(noMisfireTime);
		when(triggerMock2.getNextFireTime()).thenReturn(noMisfireTime);
		when(triggerMock1.getMisfireInstruction()).thenReturn(Trigger.MISFIRE_INSTRUCTION_SMART_POLICY);
		when(triggerMock2.getMisfireInstruction()).thenReturn(Trigger.MISFIRE_INSTRUCTION_SMART_POLICY);
		//Mock the Job and ignore concurrency
		JobDetail jobDetailMock = mock(JobDetail.class);
		JobKey jk1 = new JobKey("job1", "g1");
		when(jobDetailMock.getKey()).thenReturn(jk1);
		when(triggerMock1.getJobKey()).thenReturn(jk1);
		when(triggerMock2.getJobKey()).thenReturn(jk1);
		when(jobDetailMock.isConcurrentExecutionDisallowed()).thenReturn(false);
		try {
			js.storeTrigger(triggerMock1, false);
			js.storeTrigger(triggerMock2, false);
			js.storeJob(jobDetailMock, false);
			//get one trigger
			List<OperableTrigger> acquiredTriggers = js.acquireNextTriggers(0, 1, 1000);
			assertEquals(1, acquiredTriggers.size());
			//is second still Trigger acquireable?
			acquiredTriggers = js.acquireNextTriggers(0, 1, 1000);
			assertEquals(1, acquiredTriggers.size());
		} catch (JobPersistenceException e) {
			fail("Unexpected Exception: ");
			e.printStackTrace();
		}
	}
	
	@Test
	public void testAcquireNextTriggers_DoNotReturnPausedTrigger() {
		OperableTrigger triggerMock1 = mock(OperableTrigger.class);
		OperableTrigger triggerMock2 = mock(OperableTrigger.class);
		TriggerKey triggerKey1 = new TriggerKey("name1", "group");
		TriggerKey triggerKey2 = new TriggerKey("name2", "group");
		when(triggerMock1.getKey()).thenReturn(triggerKey1);
		when(triggerMock2.getKey()).thenReturn(triggerKey2);
		//Mock Time for Missfire Instruction
		TimeOfSystem systemTimeMock = mock(TimeOfSystem.class);
		js.setSystemTime(systemTimeMock);
		Date currentTime = new Date(1000L);
		Date noMisfireTime = new Date(1200L);
		when(systemTimeMock.currentTimeInMilliSeconds()).thenReturn(currentTime.getTime());
		when(triggerMock1.getNextFireTime()).thenReturn(noMisfireTime);
		when(triggerMock2.getNextFireTime()).thenReturn(noMisfireTime);
		when(triggerMock1.getMisfireInstruction()).thenReturn(Trigger.MISFIRE_INSTRUCTION_SMART_POLICY);
		when(triggerMock2.getMisfireInstruction()).thenReturn(Trigger.MISFIRE_INSTRUCTION_SMART_POLICY);
		//Mock the Job and ignore concurrency
		JobDetail jobDetailMock = mock(JobDetail.class);
		JobKey jk1 = new JobKey("job1", "g1");
		when(jobDetailMock.getKey()).thenReturn(jk1);
		when(triggerMock1.getJobKey()).thenReturn(jk1);
		when(triggerMock2.getJobKey()).thenReturn(jk1);
		when(jobDetailMock.isConcurrentExecutionDisallowed()).thenReturn(false);
		try {
			js.storeTrigger(triggerMock1, false);
			js.storeTrigger(triggerMock2, false);
			js.storeJob(jobDetailMock, false);
			js.pauseTrigger(triggerKey2);
			//get one trigger
			List<OperableTrigger> acquiredTriggers = js.acquireNextTriggers(0, 100, 1000);
			//Should contain trigger 1
			assertTrue(acquiredTriggers.contains(triggerMock1));
			//But not trigger 2 for it is paused
			assertFalse(acquiredTriggers.contains(triggerMock2));
		} catch (JobPersistenceException e) {
			fail("Unexpected Exception: ");
			e.printStackTrace();
		}
	}
	
	@Test
	public void testAcquireNextTriggers_DoNotReturnPausedTriggerGroups() {
		OperableTrigger triggerMock1 = mock(OperableTrigger.class);
		OperableTrigger triggerMock2 = mock(OperableTrigger.class);
		TriggerKey triggerKey1 = new TriggerKey("name1", "group1");
		TriggerKey triggerKey2 = new TriggerKey("name2", "group2");
		when(triggerMock1.getKey()).thenReturn(triggerKey1);
		when(triggerMock2.getKey()).thenReturn(triggerKey2);
		//Mock Time for Missfire Instruction
		TimeOfSystem systemTimeMock = mock(TimeOfSystem.class);
		js.setSystemTime(systemTimeMock);
		Date currentTime = new Date(1000L);
		Date noMisfireTime = new Date(1200L);
		when(systemTimeMock.currentTimeInMilliSeconds()).thenReturn(currentTime.getTime());
		when(triggerMock1.getNextFireTime()).thenReturn(noMisfireTime);
		when(triggerMock2.getNextFireTime()).thenReturn(noMisfireTime);
		when(triggerMock1.getMisfireInstruction()).thenReturn(Trigger.MISFIRE_INSTRUCTION_SMART_POLICY);
		when(triggerMock2.getMisfireInstruction()).thenReturn(Trigger.MISFIRE_INSTRUCTION_SMART_POLICY);
		//Mock the Job and ignore concurrency
		JobDetail jobDetailMock = mock(JobDetail.class);
		JobKey jk1 = new JobKey("job1", "g1");
		when(jobDetailMock.getKey()).thenReturn(jk1);
		when(triggerMock1.getJobKey()).thenReturn(jk1);
		when(triggerMock2.getJobKey()).thenReturn(jk1);
		when(jobDetailMock.isConcurrentExecutionDisallowed()).thenReturn(false);
		try {
			js.storeTrigger(triggerMock1, false);
			js.storeTrigger(triggerMock2, false);
			js.pauseTriggers(GroupMatcher.triggerGroupEquals("group1"));
			js.storeJob(jobDetailMock, false);
			//get one trigger
			List<OperableTrigger> acquiredTriggers = js.acquireNextTriggers(0, 100, 1000);
			//Should contain trigger 2
			assertTrue(acquiredTriggers.contains(triggerMock2));
			//But not trigger 1 for its group is paused
			assertFalse(acquiredTriggers.contains(triggerMock1));
		} catch (JobPersistenceException e) {
			fail("Unexpected Exception: ");
			e.printStackTrace();
		}
	}
	
	@Test
	public void testAcquireNextTriggers_DoNotReturnTriggersOfPausedJobs() {
		OperableTrigger triggerMock1 = mock(OperableTrigger.class);
		OperableTrigger triggerMock2 = mock(OperableTrigger.class);
		TriggerKey triggerKey1 = new TriggerKey("name1", "group1");
		TriggerKey triggerKey2 = new TriggerKey("name2", "group2");
		when(triggerMock1.getKey()).thenReturn(triggerKey1);
		when(triggerMock2.getKey()).thenReturn(triggerKey2);
		JobKey jk1 = new JobKey("job1", "g1");
		JobKey jk2 = new JobKey("job2", "g2");
		//associate JobKey 1 with Trigger 1 and JobKey 2 with Trigger 2
		when(triggerMock1.getJobKey()).thenReturn(jk1);
		when(triggerMock2.getJobKey()).thenReturn(jk2);
		//Mock Time for Missfire Instruction
		TimeOfSystem systemTimeMock = mock(TimeOfSystem.class);
		js.setSystemTime(systemTimeMock);
		Date currentTime = new Date(1000L);
		Date noMisfireTime = new Date(1200L);
		when(systemTimeMock.currentTimeInMilliSeconds()).thenReturn(currentTime.getTime());
		when(triggerMock1.getNextFireTime()).thenReturn(noMisfireTime);
		when(triggerMock2.getNextFireTime()).thenReturn(noMisfireTime);
		when(triggerMock1.getMisfireInstruction()).thenReturn(Trigger.MISFIRE_INSTRUCTION_SMART_POLICY);
		when(triggerMock2.getMisfireInstruction()).thenReturn(Trigger.MISFIRE_INSTRUCTION_SMART_POLICY);
		//Mock the Job and ignore concurrency
		JobDetail jobDetailMock1 = mock(JobDetail.class);
		JobDetail jobDetailMock2 = mock(JobDetail.class);
		when(jobDetailMock1.getKey()).thenReturn(jk1);
		when(jobDetailMock2.getKey()).thenReturn(jk2);
		when(jobDetailMock1.isConcurrentExecutionDisallowed()).thenReturn(false);
		when(jobDetailMock2.isConcurrentExecutionDisallowed()).thenReturn(false);
		try {
			js.storeTrigger(triggerMock1, false);
			js.storeTrigger(triggerMock2, false);
			js.storeJob(jobDetailMock1, false);
			js.storeJob(jobDetailMock2, false);
			//Pause job 1 -> trigger 1 should be paused too
			js.pauseJob(jk1);
			List<OperableTrigger> acquiredTriggers = js.acquireNextTriggers(0, 100, 1000);
			assertTrue(acquiredTriggers.contains(triggerMock2));
			assertFalse(acquiredTriggers.contains(triggerMock1));
		} catch (JobPersistenceException e) {
			fail("Unexpected Exception: ");
			e.printStackTrace();
		}
	}
	
	@Test
	public void testAcquireNextTriggers_DoNotReturnTriggersOfPausedJobGroups() {
		//Mock the Jobs
		JobDetail jd1 = mock(JobDetail.class);
		JobDetail jd2 = mock(JobDetail.class);
		JobKey jk1 = new JobKey("job1", "g1");
		JobKey jk2 = new JobKey("job2", "g2");
		when(jd1.getKey()).thenReturn(jk1);
		when(jd2.getKey()).thenReturn(jk2);
		//Mock the Triggers
		OperableTrigger triggerMock1 = mock(OperableTrigger.class);
		OperableTrigger triggerMock2 = mock(OperableTrigger.class);
		TriggerKey triggerKey1 = new TriggerKey("name1", "group1");
		TriggerKey triggerKey2 = new TriggerKey("name2", "group2");
		when(triggerMock1.getKey()).thenReturn(triggerKey1);
		when(triggerMock2.getKey()).thenReturn(triggerKey2);
		//associate JobKey 1 with Trigger 1 and JobKey 2 with Trigger 2
		when(triggerMock1.getJobKey()).thenReturn(jk1);
		when(triggerMock2.getJobKey()).thenReturn(jk2);
		//Mock Time for Missfire Instruction
		TimeOfSystem systemTimeMock = mock(TimeOfSystem.class);
		js.setSystemTime(systemTimeMock);
		Date currentTime = new Date(1000L);
		Date noMisfireTime = new Date(1200L);
		when(systemTimeMock.currentTimeInMilliSeconds()).thenReturn(currentTime.getTime());
		when(triggerMock1.getNextFireTime()).thenReturn(noMisfireTime);
		when(triggerMock2.getNextFireTime()).thenReturn(noMisfireTime);
		when(triggerMock1.getMisfireInstruction()).thenReturn(Trigger.MISFIRE_INSTRUCTION_SMART_POLICY);
		when(triggerMock2.getMisfireInstruction()).thenReturn(Trigger.MISFIRE_INSTRUCTION_SMART_POLICY);
		js.setMisfireThreshold(1);
		try {
			js.storeTrigger(triggerMock1, false);
			js.storeTrigger(triggerMock2, false);
			js.storeJob(jd1, false);
			js.storeJob(jd2, false);
			//Pause job group 1 -> pause trigger 1
			js.pauseJobs(GroupMatcher.jobGroupEquals("g1"));
			List<OperableTrigger> acquiredTriggers = js.acquireNextTriggers(0, 100, 1000);
			assertTrue(acquiredTriggers.contains(triggerMock2));
			assertFalse(acquiredTriggers.contains(triggerMock1));
		} catch (JobPersistenceException e) {
			fail("Unexpected Exception: ");
			e.printStackTrace();
		}
	}

	@Test
	public void testAcquireNextTriggers_findMisfiredTrigger() {
		//Mock the Triggers
		OperableTrigger triggerMock1 = mock(OperableTrigger.class);
		OperableTrigger triggerMock2 = mock(OperableTrigger.class);
		TriggerKey triggerKey1 = new TriggerKey("name1", "group1");
		TriggerKey triggerKey2 = new TriggerKey("name2", "group2");
		when(triggerMock1.getKey()).thenReturn(triggerKey1);
		when(triggerMock2.getKey()).thenReturn(triggerKey2);
		//Mock systemTime for Triggers to misfire
		TimeOfSystem systemTimeMock = mock(TimeOfSystem.class);
		js.setSystemTime(systemTimeMock);
		//Set Time for Triggers
		Date currentTime = new Date(1000L);
		Date misfireTime = new Date(800L);
		Date noMisfireTime = new Date(1200L);
		//trigger 1 will misfire
		when(triggerMock1.getNextFireTime()).thenReturn(misfireTime);
		when(triggerMock2.getNextFireTime()).thenReturn(noMisfireTime);
		when(systemTimeMock.currentTimeInMilliSeconds()).thenReturn(currentTime.getTime());
		//Set misfire policy for triggers
		when(triggerMock1.getMisfireInstruction()).thenReturn(Trigger.MISFIRE_INSTRUCTION_SMART_POLICY);
		when(triggerMock2.getMisfireInstruction()).thenReturn(Trigger.MISFIRE_INSTRUCTION_SMART_POLICY);
		//Mock Signaler 
		SchedulerSignaler signaler = mock(SchedulerSignaler.class);
		js.setSchedulerSignaler(signaler);
		js.setMisfireThreshold(1);
		//Mock the Job and ignore concurrency
		JobDetail jobDetailMock = mock(JobDetail.class);
		JobKey jk1 = new JobKey("job1", "g1");
		when(jobDetailMock.getKey()).thenReturn(jk1);
		when(triggerMock1.getJobKey()).thenReturn(jk1);
		when(triggerMock2.getJobKey()).thenReturn(jk1);
		when(jobDetailMock.isConcurrentExecutionDisallowed()).thenReturn(false);
		try {
			js.storeTrigger(triggerMock1, false);
			js.storeTrigger(triggerMock2, false);
			js.storeJob(jobDetailMock, false);
			List<OperableTrigger> acquiredTriggers = js.acquireNextTriggers(0, 100, 1000);
			Mockito.verify(signaler).notifyTriggerListenersMisfired(triggerMock1);
			assertTrue(acquiredTriggers.contains(triggerMock2));
			assertFalse(acquiredTriggers.contains(triggerMock1));
		} catch (JobPersistenceException e) {
			fail("Unexpected Exception: ");
			e.printStackTrace();
		}
	}
	
	@Test
	public void testAcquireNextTriggers_noNextFireTime() {
		//Mock the Triggers
		OperableTrigger triggerMock1 = mock(OperableTrigger.class);
		OperableTrigger triggerMock2 = mock(OperableTrigger.class);
		TriggerKey triggerKey1 = new TriggerKey("name1", "group1");
		TriggerKey triggerKey2 = new TriggerKey("name2", "group2");
		when(triggerMock1.getKey()).thenReturn(triggerKey1);
		when(triggerMock2.getKey()).thenReturn(triggerKey2);
		//Mock systemTime for Triggers for misfireInstruction
		TimeOfSystem systemTimeMock = mock(TimeOfSystem.class);
		js.setSystemTime(systemTimeMock);
		//Set Time for Triggers
		Date currentTime = new Date(1000L);
		Date noMisfireTime = new Date(1200L);
		//trigger 1 has no fireTime so it should not be returned
		when(triggerMock1.getNextFireTime()).thenReturn(null);
		when(triggerMock2.getNextFireTime()).thenReturn(noMisfireTime);
		when(systemTimeMock.currentTimeInMilliSeconds()).thenReturn(currentTime.getTime());
		//Set misfire policy for triggers
		when(triggerMock1.getMisfireInstruction()).thenReturn(Trigger.MISFIRE_INSTRUCTION_SMART_POLICY);
		when(triggerMock2.getMisfireInstruction()).thenReturn(Trigger.MISFIRE_INSTRUCTION_SMART_POLICY);
		//Mock Signaler 
		js.setMisfireThreshold(1);
		//Mock the Job and ignore concurrency
		JobDetail jobDetailMock = mock(JobDetail.class);
		JobKey jk1 = new JobKey("job1", "g1");
		when(jobDetailMock.getKey()).thenReturn(jk1);
		when(triggerMock1.getJobKey()).thenReturn(jk1);
		when(triggerMock2.getJobKey()).thenReturn(jk1);
		when(jobDetailMock.isConcurrentExecutionDisallowed()).thenReturn(false);
		try {
			js.storeTrigger(triggerMock1, false);
			js.storeTrigger(triggerMock2, false);
			js.storeJob(jobDetailMock, false);
			List<OperableTrigger> acquiredTriggers = js.acquireNextTriggers(0, 100, 1000);
			assertTrue(acquiredTriggers.contains(triggerMock2));
			assertFalse(acquiredTriggers.contains(triggerMock1));
		} catch (JobPersistenceException e) {
			fail("Unexpected Exception: ");
			e.printStackTrace();
		}
	}
	
	@Test
	public void testAcquireNextTriggers_TriggerNotInTimewindow() {
		//Mock the Triggers
		OperableTrigger triggerMockOutOfTimeWindow = mock(OperableTrigger.class);
		OperableTrigger triggerMockWithinTimeWindow = mock(OperableTrigger.class);
		TriggerKey triggerKey1 = new TriggerKey("name1", "group1");
		TriggerKey triggerKey2 = new TriggerKey("name2", "group2");
		when(triggerMockOutOfTimeWindow.getKey()).thenReturn(triggerKey1);
		when(triggerMockWithinTimeWindow.getKey()).thenReturn(triggerKey2);
		//Mock systemTime for Triggers for misfireInstruction
		TimeOfSystem systemTimeMock = mock(TimeOfSystem.class);
		js.setSystemTime(systemTimeMock);
		//Set Time for Triggers
		Date currentTime = new Date(1000L);
		Date inTimeLimit = new Date(1200L);
		Date timeLimit = new Date(1600L);
		Date outOfTimeLimit = new Date(1800L);
		//trigger 1 has no fireTime so it should not be returned
		when(triggerMockOutOfTimeWindow.getNextFireTime()).thenReturn(outOfTimeLimit);
		when(triggerMockWithinTimeWindow.getNextFireTime()).thenReturn(inTimeLimit);
		when(systemTimeMock.currentTimeInMilliSeconds()).thenReturn(currentTime.getTime());
		//Set misfire policy for triggers
		when(triggerMockOutOfTimeWindow.getMisfireInstruction()).thenReturn(Trigger.MISFIRE_INSTRUCTION_SMART_POLICY);
		when(triggerMockWithinTimeWindow.getMisfireInstruction()).thenReturn(Trigger.MISFIRE_INSTRUCTION_SMART_POLICY);
		//Mock Signaler 
		js.setMisfireThreshold(1);
		//Mock the Job and ignore concurrency
		JobDetail jobDetailMock = mock(JobDetail.class);
		JobKey jk1 = new JobKey("job1", "g1");
		when(jobDetailMock.getKey()).thenReturn(jk1);
		when(triggerMockOutOfTimeWindow.getJobKey()).thenReturn(jk1);
		when(triggerMockWithinTimeWindow.getJobKey()).thenReturn(jk1);
		when(jobDetailMock.isConcurrentExecutionDisallowed()).thenReturn(false);
		try {
			js.storeTrigger(triggerMockOutOfTimeWindow, false);
			js.storeTrigger(triggerMockWithinTimeWindow, false);
			js.storeJob(jobDetailMock, false);
			List<OperableTrigger> acquiredTriggers = js.acquireNextTriggers(timeLimit.getTime(), 100, 0);
			assertTrue(acquiredTriggers.contains(triggerMockWithinTimeWindow));
			assertFalse(acquiredTriggers.contains(triggerMockOutOfTimeWindow));
		} catch (JobPersistenceException e) {
			fail("Unexpected Exception: ");
			e.printStackTrace();
		}
	}
	
	@Test
	public void testAcquireNextTriggers_DisallowConcurrentExecution() {
		//Mock the Jobs
		JobDetail jobDetailNoConcurrentExecution = mock(JobDetail.class);
		JobKey jk1 = new JobKey("job1", "g1");
		when(jobDetailNoConcurrentExecution.getKey()).thenReturn(jk1);
		//Mock the Triggers
		OperableTrigger triggerMock1 = mock(OperableTrigger.class);
		OperableTrigger triggerMock2 = mock(OperableTrigger.class);
		TriggerKey triggerKey1 = new TriggerKey("name1", "group1");
		TriggerKey triggerKey2 = new TriggerKey("name2", "group2");
		when(triggerMock1.getKey()).thenReturn(triggerKey1);
		when(triggerMock2.getKey()).thenReturn(triggerKey2);
		//both trigger reference the same job
		when(triggerMock1.getJobKey()).thenReturn(jk1);
		when(triggerMock2.getJobKey()).thenReturn(jk1);
		//Mock Time for Missfire Instruction
		TimeOfSystem systemTimeMock = mock(TimeOfSystem.class);
		js.setSystemTime(systemTimeMock);
		Date currentTime = new Date(1000L);
		Date fireTime = new Date(1200L);
		when(systemTimeMock.currentTimeInMilliSeconds()).thenReturn(currentTime.getTime());
		when(triggerMock1.getNextFireTime()).thenReturn(fireTime);
		when(triggerMock2.getNextFireTime()).thenReturn(fireTime);
		when(triggerMock1.getMisfireInstruction()).thenReturn(Trigger.MISFIRE_INSTRUCTION_SMART_POLICY);
		when(triggerMock2.getMisfireInstruction()).thenReturn(Trigger.MISFIRE_INSTRUCTION_SMART_POLICY);
		js.setMisfireThreshold(1);
		//Only one trigger is allowed to fire the job because concurrent Execution is disallowed
		when(jobDetailNoConcurrentExecution.isConcurrentExecutionDisallowed()).thenReturn(true);
		try {
			js.storeTrigger(triggerMock1, false);
			js.storeTrigger(triggerMock2, false);
			js.storeJob(jobDetailNoConcurrentExecution, false);
			List<OperableTrigger> acquiredTriggers = js.acquireNextTriggers(0, 100, 1000);
			//returned List can only contain one Trigger
			assertEquals(1, acquiredTriggers.size());
		} catch (JobPersistenceException e) {
			fail("Unexpected Exception: ");
			e.printStackTrace();
		}
	}
	
	@Test
	public void testAcquireNextTriggers_TriggersReturnedInRightOrder() {
		//Mock the Jobs
		JobDetail jobDetailMock = mock(JobDetail.class);
		JobKey jk1 = new JobKey("job1", "g1");
		when(jobDetailMock.getKey()).thenReturn(jk1);
		//Mock the Triggers
		OperableTrigger triggerMock1 = mock(OperableTrigger.class);
		OperableTrigger triggerMock2 = mock(OperableTrigger.class);
		OperableTrigger triggerMock3 = mock(OperableTrigger.class);
		TriggerKey triggerKey1 = new TriggerKey("name1", "group1");
		TriggerKey triggerKey2 = new TriggerKey("name2", "group2");
		TriggerKey triggerKey3 = new TriggerKey("name3", "group3");
		when(triggerMock1.getKey()).thenReturn(triggerKey1);
		when(triggerMock2.getKey()).thenReturn(triggerKey2);
		when(triggerMock3.getKey()).thenReturn(triggerKey3);
		when(triggerMock1.getJobKey()).thenReturn(jk1);
		when(triggerMock2.getJobKey()).thenReturn(jk1);
		when(triggerMock3.getJobKey()).thenReturn(jk1);
		//Mock Time environment
		TimeOfSystem systemTimeMock = mock(TimeOfSystem.class);
		js.setSystemTime(systemTimeMock);
		//Time setup
		Date currentTime 	= new Date(1000L);
		Date firstFireTime 	= new Date(1200L);
		Date secondFireTime = new Date(1400L);
		Date thirdFireTime 	= new Date(1600L);
		when(systemTimeMock.currentTimeInMilliSeconds()).thenReturn(currentTime.getTime());
		when(triggerMock1.getMisfireInstruction()).thenReturn(Trigger.MISFIRE_INSTRUCTION_SMART_POLICY);
		when(triggerMock2.getMisfireInstruction()).thenReturn(Trigger.MISFIRE_INSTRUCTION_SMART_POLICY);
		when(triggerMock3.getMisfireInstruction()).thenReturn(Trigger.MISFIRE_INSTRUCTION_SMART_POLICY);
		js.setMisfireThreshold(1);
		//allow concurrency 
		when(jobDetailMock.isConcurrentExecutionDisallowed()).thenReturn(false);
		try {
			js.storeTrigger(triggerMock1, false);
			js.storeTrigger(triggerMock2, false);
			js.storeTrigger(triggerMock3, false);
			js.storeJob(jobDetailMock, false);
			//The order of the Cache is not foreseeable so we have to test different mutations
			when(triggerMock1.getNextFireTime()).thenReturn(firstFireTime);
			when(triggerMock2.getNextFireTime()).thenReturn(secondFireTime);
			when(triggerMock3.getNextFireTime()).thenReturn(thirdFireTime);
			List<OperableTrigger> acquiredTriggers = js.acquireNextTriggers(0, 100, 1000);
			assertTrue(acquiredTriggers.get(0)==triggerMock1);
			assertTrue(acquiredTriggers.get(1)==triggerMock2);
			assertTrue(acquiredTriggers.get(2)==triggerMock3);
			//second mutation different triggers have different positions
			js.schedulerStarted();
			js.clearAllSchedulingData();
			js.storeTrigger(triggerMock1, false);
			js.storeTrigger(triggerMock2, false);
			js.storeTrigger(triggerMock3, false);
			js.storeJob(jobDetailMock, false);
			when(triggerMock2.getNextFireTime()).thenReturn(firstFireTime);
			when(triggerMock1.getNextFireTime()).thenReturn(secondFireTime);
			when(triggerMock3.getNextFireTime()).thenReturn(thirdFireTime);
			acquiredTriggers = js.acquireNextTriggers(0, 100, 1000);
			assertTrue(acquiredTriggers.get(0)==triggerMock2);
			assertTrue(acquiredTriggers.get(1)==triggerMock1);
			assertTrue(acquiredTriggers.get(2)==triggerMock3);
			//third mutation different triggers have different positions
			js.clearAllSchedulingData();
			js.storeTrigger(triggerMock1, false);
			js.storeTrigger(triggerMock2, false);
			js.storeTrigger(triggerMock3, false);
			js.storeJob(jobDetailMock, false);
			when(triggerMock3.getNextFireTime()).thenReturn(firstFireTime);
			when(triggerMock1.getNextFireTime()).thenReturn(secondFireTime);
			when(triggerMock2.getNextFireTime()).thenReturn(thirdFireTime);
			acquiredTriggers = js.acquireNextTriggers(0, 100, 1000);
			assertTrue(acquiredTriggers.get(0)==triggerMock3);
			assertTrue(acquiredTriggers.get(1)==triggerMock1);
			assertTrue(acquiredTriggers.get(2)==triggerMock2);
		} catch (SchedulerException e) {
			fail("Unexpected Exception: ");
			e.printStackTrace();
		}
	}
	
	@Test
	public void testAcquireNextTriggers_TriggersReturnedInRightOrderAndRightNumberOfTriggers() {
		//Mock the Jobs
		JobDetail jobDetailMock = mock(JobDetail.class);
		JobKey jk1 = new JobKey("job1", "g1");
		when(jobDetailMock.getKey()).thenReturn(jk1);
		//Mock the Triggers
		OperableTrigger triggerMock1 = mock(OperableTrigger.class);
		OperableTrigger triggerMock2 = mock(OperableTrigger.class);
		OperableTrigger triggerMock3 = mock(OperableTrigger.class);
		TriggerKey triggerKey1 = new TriggerKey("name1", "group1");
		TriggerKey triggerKey2 = new TriggerKey("name2", "group2");
		TriggerKey triggerKey3 = new TriggerKey("name3", "group3");
		when(triggerMock1.getKey()).thenReturn(triggerKey1);
		when(triggerMock2.getKey()).thenReturn(triggerKey2);
		when(triggerMock3.getKey()).thenReturn(triggerKey3);
		when(triggerMock1.getJobKey()).thenReturn(jk1);
		when(triggerMock2.getJobKey()).thenReturn(jk1);
		when(triggerMock3.getJobKey()).thenReturn(jk1);
		//Mock Time environment
		TimeOfSystem systemTimeMock = mock(TimeOfSystem.class);
		js.setSystemTime(systemTimeMock);
		//Time setup
		Date currentTime 	= new Date(1000L);
		Date firstFireTime 	= new Date(1200L);
		Date secondFireTime = new Date(1400L);
		Date thirdFireTime 	= new Date(1600L);
		when(systemTimeMock.currentTimeInMilliSeconds()).thenReturn(currentTime.getTime());
		when(triggerMock1.getMisfireInstruction()).thenReturn(Trigger.MISFIRE_INSTRUCTION_SMART_POLICY);
		when(triggerMock2.getMisfireInstruction()).thenReturn(Trigger.MISFIRE_INSTRUCTION_SMART_POLICY);
		when(triggerMock3.getMisfireInstruction()).thenReturn(Trigger.MISFIRE_INSTRUCTION_SMART_POLICY);
		js.setMisfireThreshold(1);
		//allow concurrency 
		when(jobDetailMock.isConcurrentExecutionDisallowed()).thenReturn(false);
		try {
			js.storeTrigger(triggerMock1, false);
			js.storeTrigger(triggerMock2, false);
			js.storeTrigger(triggerMock3, false);
			js.storeJob(jobDetailMock, false);
			//The order of the Cache is not foreseeable so we have to test different mutations
			when(triggerMock1.getNextFireTime()).thenReturn(firstFireTime);
			when(triggerMock2.getNextFireTime()).thenReturn(secondFireTime);
			when(triggerMock3.getNextFireTime()).thenReturn(thirdFireTime);
			//only two triggers should be returned
			List<OperableTrigger> acquiredTriggers = js.acquireNextTriggers(0, 2, 1000);
			assertTrue(acquiredTriggers.get(0)==triggerMock1);
			assertTrue(acquiredTriggers.get(1)==triggerMock2);
			assertEquals(2, acquiredTriggers.size());
			//second mutation different triggers have different positions
			js.schedulerStarted();
			js.clearAllSchedulingData();
			js.storeTrigger(triggerMock1, false);
			js.storeTrigger(triggerMock2, false);
			js.storeTrigger(triggerMock3, false);
			js.storeJob(jobDetailMock, false);
			when(triggerMock2.getNextFireTime()).thenReturn(firstFireTime);
			when(triggerMock1.getNextFireTime()).thenReturn(secondFireTime);
			when(triggerMock3.getNextFireTime()).thenReturn(thirdFireTime);
			acquiredTriggers = js.acquireNextTriggers(0, 2, 1000);
			assertTrue(acquiredTriggers.get(0)==triggerMock2);
			assertTrue(acquiredTriggers.get(1)==triggerMock1);
			assertEquals(2, acquiredTriggers.size());
			//third mutation different triggers have different positions
			js.clearAllSchedulingData();
			js.storeTrigger(triggerMock1, false);
			js.storeTrigger(triggerMock2, false);
			js.storeTrigger(triggerMock3, false);
			js.storeJob(jobDetailMock, false);
			when(triggerMock3.getNextFireTime()).thenReturn(firstFireTime);
			when(triggerMock1.getNextFireTime()).thenReturn(secondFireTime);
			when(triggerMock2.getNextFireTime()).thenReturn(thirdFireTime);
			acquiredTriggers = js.acquireNextTriggers(0, 2, 1000);
			assertTrue(acquiredTriggers.get(0)==triggerMock3);
			assertTrue(acquiredTriggers.get(1)==triggerMock1);
			assertEquals(2, acquiredTriggers.size());
		} catch (SchedulerException e) {
			fail("Unexpected Exception: ");
			e.printStackTrace();
		} 
	}
	
	@Test
	public void testAcquireNextTriggers_NonConcurrentJobHasMultibleTriggersWhichFireAtDifferentTimesAndShouldBeReturnedInRightOrder() {
		//Mock the Jobs
		JobDetail jobDetailMock = mock(JobDetail.class);
		JobKey jk1 = new JobKey("job1", "g1");
		when(jobDetailMock.getKey()).thenReturn(jk1);
		//Mock the Triggers
		OperableTrigger triggerMock1 = mock(OperableTrigger.class);
		OperableTrigger triggerMock2 = mock(OperableTrigger.class);
		OperableTrigger triggerMock3 = mock(OperableTrigger.class);
		TriggerKey triggerKey1 = new TriggerKey("name1", "group1");
		TriggerKey triggerKey2 = new TriggerKey("name2", "group2");
		TriggerKey triggerKey3 = new TriggerKey("name3", "group3");
		when(triggerMock1.getKey()).thenReturn(triggerKey1);
		when(triggerMock2.getKey()).thenReturn(triggerKey2);
		when(triggerMock3.getKey()).thenReturn(triggerKey3);
		//All the triggers reference the same job
		when(triggerMock1.getJobKey()).thenReturn(jk1);
		when(triggerMock2.getJobKey()).thenReturn(jk1);
		when(triggerMock3.getJobKey()).thenReturn(jk1);
		//Mock Time environment
		TimeOfSystem systemTimeMock = mock(TimeOfSystem.class);
		js.setSystemTime(systemTimeMock);
		//Time setup
		Date currentTime 	= new Date(1000L);
		Date firstFireTime 	= new Date(1200L);
		Date secondFireTime = new Date(1400L);
		Date thirdFireTime 	= new Date(1600L);
		when(systemTimeMock.currentTimeInMilliSeconds()).thenReturn(currentTime.getTime());
		when(triggerMock1.getMisfireInstruction()).thenReturn(Trigger.MISFIRE_INSTRUCTION_SMART_POLICY);
		when(triggerMock2.getMisfireInstruction()).thenReturn(Trigger.MISFIRE_INSTRUCTION_SMART_POLICY);
		when(triggerMock3.getMisfireInstruction()).thenReturn(Trigger.MISFIRE_INSTRUCTION_SMART_POLICY);
		js.setMisfireThreshold(1);
		//don't allow concurrency 
		when(jobDetailMock.isConcurrentExecutionDisallowed()).thenReturn(true);
		try {
			js.storeTrigger(triggerMock1, false);
			js.storeTrigger(triggerMock2, false);
			js.storeTrigger(triggerMock3, false);
			js.storeJob(jobDetailMock, false);
			//The order of the Cache is not foreseeable so we have to test different mutations
			when(triggerMock1.getNextFireTime()).thenReturn(firstFireTime);
			when(triggerMock2.getNextFireTime()).thenReturn(secondFireTime);
			when(triggerMock3.getNextFireTime()).thenReturn(thirdFireTime);
			//only the first trigger should be returned because he will fire first
			//all the other triggers should not be returned because they start
			//the same Job and concurrency is not allowed
			List<OperableTrigger> acquiredTriggers = js.acquireNextTriggers(0, 100, 1000);
			assertTrue(acquiredTriggers.get(0)==triggerMock1);
			assertEquals(1, acquiredTriggers.size());
			//second mutation different triggers have different positions
			js.schedulerStarted();
			js.clearAllSchedulingData();
			js.storeTrigger(triggerMock1, false);
			js.storeTrigger(triggerMock2, false);
			js.storeTrigger(triggerMock3, false);
			js.storeJob(jobDetailMock, false);
			when(triggerMock2.getNextFireTime()).thenReturn(firstFireTime);
			when(triggerMock1.getNextFireTime()).thenReturn(secondFireTime);
			when(triggerMock3.getNextFireTime()).thenReturn(thirdFireTime);
			acquiredTriggers = js.acquireNextTriggers(0, 100, 1000);
			assertTrue(acquiredTriggers.get(0)==triggerMock2);
			assertEquals(1, acquiredTriggers.size());
			//third mutation different triggers have different positions
			js.clearAllSchedulingData();
			js.storeTrigger(triggerMock1, false);
			js.storeTrigger(triggerMock2, false);
			js.storeTrigger(triggerMock3, false);
			js.storeJob(jobDetailMock, false);
			when(triggerMock3.getNextFireTime()).thenReturn(firstFireTime);
			when(triggerMock1.getNextFireTime()).thenReturn(secondFireTime);
			when(triggerMock2.getNextFireTime()).thenReturn(thirdFireTime);
			acquiredTriggers = js.acquireNextTriggers(0, 100, 1000);
			assertTrue(acquiredTriggers.get(0)==triggerMock3);
			assertEquals(1, acquiredTriggers.size());
		} catch (SchedulerException e) {
			fail("Unexpected Exception: ");
			e.printStackTrace();
		}
	}
	
	@Test
	public void testAcquireNextTriggers_CantAquireTriggerTwiche() {
		OperableTrigger triggerMock = mock(OperableTrigger.class);
		TriggerKey triggerKey = new TriggerKey("name", "group");
		when(triggerMock.getKey()).thenReturn(triggerKey);
		//Mock Time 
		TimeOfSystem systemTimeMock = mock(TimeOfSystem.class);
		js.setSystemTime(systemTimeMock);
		Date currentTime = new Date(1000L);
		Date noMisfireTime = new Date(1200L);
		when(systemTimeMock.currentTimeInMilliSeconds()).thenReturn(currentTime.getTime());
		when(triggerMock.getNextFireTime()).thenReturn(noMisfireTime);
		when(triggerMock.getMisfireInstruction()).thenReturn(Trigger.MISFIRE_INSTRUCTION_SMART_POLICY);
		js.setMisfireThreshold(1);
		//Mock the Job and ignore concurrency
		JobDetail jobDetailMock = mock(JobDetail.class);
		JobKey jk1 = new JobKey("job1", "g1");
		when(jobDetailMock.getKey()).thenReturn(jk1);
		when(triggerMock.getJobKey()).thenReturn(jk1);
		when(jobDetailMock.isConcurrentExecutionDisallowed()).thenReturn(false);
		try {
			js.storeTrigger(triggerMock, false);
			js.storeJob(jobDetailMock, false);
			List<OperableTrigger> acquiredTriggers = js.acquireNextTriggers(0, 100, 1000);
			assertTrue(acquiredTriggers.get(0)==triggerMock);
			//trigger should now be acquired and not returned again
			acquiredTriggers = js.acquireNextTriggers(0, 100, 1000);
			assertEquals(0, acquiredTriggers.size());
		} catch (JobPersistenceException e) {
			fail("Unexpected Exception: ");
			e.printStackTrace();
		}
	}
	
	@Test
	public void testReleaseAcquiredTrigger() {
		OperableTrigger triggerMock = mock(OperableTrigger.class);
		TriggerKey triggerKey = new TriggerKey("name", "group");
		when(triggerMock.getKey()).thenReturn(triggerKey);
		//Mock Time 
		TimeOfSystem systemTimeMock = mock(TimeOfSystem.class);
		js.setSystemTime(systemTimeMock);
		Date currentTime = new Date(1000L);
		Date noMisfireTime = new Date(1200L);
		when(systemTimeMock.currentTimeInMilliSeconds()).thenReturn(currentTime.getTime());
		when(triggerMock.getNextFireTime()).thenReturn(noMisfireTime);
		when(triggerMock.getMisfireInstruction()).thenReturn(Trigger.MISFIRE_INSTRUCTION_SMART_POLICY);
		js.setMisfireThreshold(1);
		//Mock the Job and ignore concurrency
		JobDetail jobDetailMock = mock(JobDetail.class);
		JobKey jk1 = new JobKey("job1", "g1");
		when(jobDetailMock.getKey()).thenReturn(jk1);
		when(triggerMock.getJobKey()).thenReturn(jk1);
		when(jobDetailMock.isConcurrentExecutionDisallowed()).thenReturn(false);
		try {
			js.storeTrigger(triggerMock, false);
			js.storeJob(jobDetailMock, false);
			List<OperableTrigger> acquiredTriggers = js.acquireNextTriggers(0, 100, 1000);
			//Release the trigger
			js.releaseAcquiredTrigger(triggerMock);
			//trigger should be re-acquirable 
			acquiredTriggers = js.acquireNextTriggers(0, 100, 1000);
			assertTrue(acquiredTriggers.get(0)==triggerMock);
			assertEquals(1, acquiredTriggers.size());
		} catch (JobPersistenceException e) {
			fail("Unexpected Exception: ");
			e.printStackTrace();
		}
	}
	
	@Test
	public void testTriggersFired_NoTriggersExistNonFired() {
		List<OperableTrigger> firedTriggers = new LinkedList<OperableTrigger>();
		try {
			List<TriggerFiredResult> triggerFiredResult = js.triggersFired(firedTriggers);
			assertEquals(0, triggerFiredResult.size());
		} catch (JobPersistenceException e) {
			fail("Unexpected Exception: ");
			e.printStackTrace();
		}
	}
	
	@Test
	public void testTriggersFired_NoTriggersExist() {
		//Mock a Job
		JobDetail jobMock = mock(JobDetail.class);
		JobKey jobKey = new JobKey("job", "ofTrigger");
		when(jobMock.getKey()).thenReturn(jobKey);
		//Mock Trigger
		OperableTrigger triggerMock = mock(OperableTrigger.class);
		TriggerKey triggerKey = new TriggerKey("name", "group");
		when(triggerMock.getKey()).thenReturn(triggerKey);
		when(triggerMock.getJobKey()).thenReturn(jobKey);
		List<OperableTrigger> firedTriggers = new LinkedList<OperableTrigger>();
		firedTriggers.add(triggerMock);
		try {
			js.storeJob(jobMock, false);
			List<TriggerFiredResult> triggerFiredResult = js.triggersFired(firedTriggers);
			assertEquals(0, triggerFiredResult.size());
		} catch (JobPersistenceException e) {
			fail("Unexpected Exception: ");
			e.printStackTrace();
		}
	}
	
	@Test
	public void testTriggersFired_NotAcquired() {
		//Mock a Job
		JobDetail jobMock = mock(JobDetail.class);
		JobKey jobKey = new JobKey("job", "ofTrigger");
		when(jobMock.getKey()).thenReturn(jobKey);
		//Mock Trigger
		OperableTrigger triggerMock = mock(OperableTrigger.class);
		TriggerKey triggerKey = new TriggerKey("name", "group");
		when(triggerMock.getKey()).thenReturn(triggerKey);
		when(triggerMock.getJobKey()).thenReturn(jobKey);
		List<OperableTrigger> firedTriggers = new LinkedList<OperableTrigger>();
		firedTriggers.add(triggerMock);
		try {
			js.storeJob(jobMock, false);
			js.storeTrigger(triggerMock, false);
			//Inform about trigger fired
			List<TriggerFiredResult> triggerFiredResult = js.triggersFired(firedTriggers);
			//Size should be 0 because trigger was never acquired
			assertEquals(0, triggerFiredResult.size());
		} catch (JobPersistenceException e) {
			fail("Unexpected Exception: ");
			e.printStackTrace();
		}
	}
	
	
	@Test
	public void testTriggersFired_TriggerFiredResultReturnedJobTriggerCalendar() {
		//Mock the Job
		JobDetail jobDetailMock = mock(JobDetail.class);
		JobKey jobKey = new JobKey("job", "group");
		when(jobDetailMock.getKey()).thenReturn(jobKey);
		//Mock Calendar
		String calendarName = "calendar";
		Calendar calendarMock = mock(Calendar.class);
		//Mock Trigger
		OperableTrigger triggerMock = mock(OperableTrigger.class);
		TriggerKey triggerKey = new TriggerKey("name", "group");
		when(triggerMock.getKey()).thenReturn(triggerKey);
		//associate the trigger with the job and calendar
		when(triggerMock.getJobKey()).thenReturn(jobKey);
		when(triggerMock.getCalendarName()).thenReturn(calendarName);
		//set up time Mock
		Date currentTime = new Date(1000L);
		TimeOfSystem systemTimeMock = mock(TimeOfSystem.class);
		js.setSystemTime(systemTimeMock);
		when(systemTimeMock.currentTimeInMilliSeconds()).thenReturn(currentTime.getTime());
		
		//set Up trigger times
		Date previousFireTime = new Date(800L);
		Date scheduledFireTime = new Date(1001L);
		Date nextFireTime = new Date(1500L);
		when(triggerMock.getNextFireTime()).thenReturn(nextFireTime);
		//mock the list like behavior of trigger times
		when(triggerMock.getPreviousFireTime()).thenReturn(previousFireTime, scheduledFireTime);
		
		List<OperableTrigger> firedTriggers = new LinkedList<OperableTrigger>();
		firedTriggers.add(triggerMock);
		try {
			js.storeJob(jobDetailMock, false);
			js.storeTrigger(triggerMock, false);
			js.storeCalendar(calendarName, calendarMock, false, false);
			InfinispanJobStoreSingleServerTest.triggerCache.get(ClusterCacheJobStore.generateKey(triggerKey))
				.state = TriggerWrapper.STATE_ACQUIRED;
			//Inform about trigger fired
			List<TriggerFiredResult> triggerFiredResult = js.triggersFired(firedTriggers);
			assertEquals(1, triggerFiredResult.size());
			TriggerFiredBundle tfb = triggerFiredResult.get(0).getTriggerFiredBundle();
			//Check if results are right
			assertTrue(tfb.getTrigger() == triggerMock);						//trigger
			assertTrue(tfb.getCalendar() == calendarMock);						//calendar
			assertTrue(tfb.getFireTime().equals(currentTime));					//fireTime
			assertTrue(tfb.getScheduledFireTime().equals(scheduledFireTime)); 	//scheduledFireTime
			assertTrue(tfb.getPrevFireTime().equals(previousFireTime)); 		//prevFireTime
			assertTrue(tfb.getNextFireTime().equals(nextFireTime));				//nextFireTime
		} catch (JobPersistenceException e) {
			fail("Unexpected Exception: ");
			e.printStackTrace();
		}
	}
	
	@Test
	public void testTriggersFired_TriggerStateNoLongerAquired() {
		//Mock the Job
		JobDetail jobDetailMock = mock(JobDetail.class);
		JobKey jobKey = new JobKey("job", "group");
		when(jobDetailMock.getKey()).thenReturn(jobKey);
		//Mock Trigger
		OperableTrigger triggerMock = mock(OperableTrigger.class);
		TriggerKey triggerKey = new TriggerKey("name", "group");
		when(triggerMock.getKey()).thenReturn(triggerKey);
		when(triggerMock.getJobKey()).thenReturn(jobKey);
		//Prepare List
		List<OperableTrigger> firedTriggers = new LinkedList<OperableTrigger>();
		firedTriggers.add(triggerMock);
		//Mock SystemTime
		Date currentTime = new Date(1000L);
		TimeOfSystem systemTimeMock = mock(TimeOfSystem.class);
		js.setSystemTime(systemTimeMock);
		when(systemTimeMock.currentTimeInMilliSeconds()).thenReturn(currentTime.getTime());
		try {
			js.storeJob(jobDetailMock, false);
			js.storeTrigger(triggerMock, false);
			TriggerWrapper tw = InfinispanJobStoreSingleServerTest.triggerCache.get(ClusterCacheJobStore.generateKey(triggerKey));
			tw.state = TriggerWrapper.STATE_ACQUIRED;
			//Inform about trigger fired
			js.triggersFired(firedTriggers);
			tw = InfinispanJobStoreSingleServerTest.triggerCache.get(ClusterCacheJobStore.generateKey(triggerKey));
			assertEquals(TriggerWrapper.STATE_NORMAL, tw.state);
		} catch (JobPersistenceException e) {
			fail("Unexpected Exception: ");
			e.printStackTrace();
		}
	}
	
	
	@Test
	public void testTriggersFired_calendarGotDeleted() {
		//Mock the Job
		JobDetail jobDetailMock = mock(JobDetail.class);
		JobKey jobKey = new JobKey("job", "group");
		when(jobDetailMock.getKey()).thenReturn(jobKey);
		//Mock Trigger
		OperableTrigger triggerMock = mock(OperableTrigger.class);
		TriggerKey triggerKey = new TriggerKey("name", "group");
		when(triggerMock.getKey()).thenReturn(triggerKey);
		when(triggerMock.getJobKey()).thenReturn(jobKey);
		//return a calendar name even if there is no such calendar 
		when(triggerMock.getCalendarName()).thenReturn("calendar");
		//Prepare List
		List<OperableTrigger> firedTriggers = new LinkedList<OperableTrigger>();
		firedTriggers.add(triggerMock);
		//Mock SystemTime
		Date currentTime = new Date(1000L);
		TimeOfSystem systemTimeMock = mock(TimeOfSystem.class);
		js.setSystemTime(systemTimeMock);
		when(systemTimeMock.currentTimeInMilliSeconds()).thenReturn(currentTime.getTime());
		try {
			js.storeTrigger(triggerMock, false);
			InfinispanJobStoreSingleServerTest.triggerCache
				.get(ClusterCacheJobStore.generateKey(triggerKey))
				.state = TriggerWrapper.STATE_ACQUIRED;
			//Inform about trigger fired
			List<TriggerFiredResult> triggerFiredResult = js.triggersFired(firedTriggers);
			//should return empty list, because the calendar no longer exists
			assertEquals(0, triggerFiredResult.size());
		} catch (JobPersistenceException e) {
			fail("Unexpected Exception: ");
			e.printStackTrace();
		}
	}
	
	@Test
	public void testTriggersFired_concurrentExecutionDisallowed() {
		//Mock the Job
		JobDetail jobDetailMock = mock(JobDetail.class);
		JobKey jobKey = new JobKey("job", "group");
		when(jobDetailMock.getKey()).thenReturn(jobKey);
		//disallow concurrency
		when(jobDetailMock.isConcurrentExecutionDisallowed()).thenReturn(true);
		//Mock Triggers
		OperableTrigger triggerMock1= mock(OperableTrigger.class);
		OperableTrigger triggerMock2= mock(OperableTrigger.class);
		TriggerKey triggerKey1 = new TriggerKey("name1", "group1");
		TriggerKey triggerKey2 = new TriggerKey("name2", "group2");
		when(triggerMock1.getKey()).thenReturn(triggerKey1);
		when(triggerMock2.getKey()).thenReturn(triggerKey2);
		when(triggerMock1.getJobKey()).thenReturn(jobKey);
		when(triggerMock2.getJobKey()).thenReturn(jobKey);
		//Prepare List with trigger 1
		List<OperableTrigger> firedTriggers = new LinkedList<OperableTrigger>();
		firedTriggers.add(triggerMock1);
		//Mock SystemTime
		Date currentTime = new Date(1000L);
		TimeOfSystem systemTimeMock = mock(TimeOfSystem.class);
		js.setSystemTime(systemTimeMock);
		when(systemTimeMock.currentTimeInMilliSeconds()).thenReturn(currentTime.getTime());
		try{
			js.storeJob(jobDetailMock, false);
			js.storeTrigger(triggerMock1, false);
			js.storeTrigger(triggerMock2, false);
			//trigger 1 is acquired
			InfinispanJobStoreSingleServerTest.triggerCache
				.get(ClusterCacheJobStore.generateKey(triggerKey1))
				.state = TriggerWrapper.STATE_ACQUIRED;
			//Inform about trigger fired
			js.triggersFired(firedTriggers);
			//both triggers should be Blocked now
			assertEquals(TriggerState.BLOCKED, js.getTriggerState(triggerKey1));
			assertEquals(TriggerState.BLOCKED, js.getTriggerState(triggerKey2));
			//did the job also block?
			Set<String> blockedJobs = InfinispanJobStoreSingleServerTest.metaCache.get(ClusterCacheJobStore.METAKEY_BLOCKED_JOBS_SET);
			assertTrue(blockedJobs.contains(
					ClusterCacheJobStore.generateKey(jobKey)));
		} catch (JobPersistenceException e) {
			fail("Unexpected Exception: ");
			e.printStackTrace();
		}
	}
	
	@Test
	public void testStoreTrigger_BlockedJob() {
		//Mock the Job
		JobDetail jobDetailMock = mock(JobDetail.class);
		JobKey jobKey = new JobKey("job", "group");
		when(jobDetailMock.getKey()).thenReturn(jobKey);
		//disallow concurrency
		when(jobDetailMock.isConcurrentExecutionDisallowed()).thenReturn(true);
		//Mock Triggers
		OperableTrigger triggerMock1= mock(OperableTrigger.class);
		OperableTrigger triggerMock2= mock(OperableTrigger.class);
		TriggerKey triggerKey1 = new TriggerKey("name1", "group1");
		TriggerKey triggerKey2 = new TriggerKey("name2", "group2");
		when(triggerMock1.getKey()).thenReturn(triggerKey1);
		when(triggerMock2.getKey()).thenReturn(triggerKey2);
		when(triggerMock1.getJobKey()).thenReturn(jobKey);
		when(triggerMock2.getJobKey()).thenReturn(jobKey);
		//Prepare List with trigger 1
		List<OperableTrigger> firedTriggers = new LinkedList<OperableTrigger>();
		firedTriggers.add(triggerMock1);
		//Mock SystemTime
		Date currentTime = new Date(1000L);
		TimeOfSystem systemTimeMock = mock(TimeOfSystem.class);
		js.setSystemTime(systemTimeMock);
		when(systemTimeMock.currentTimeInMilliSeconds()).thenReturn(currentTime.getTime());
		try{
			js.storeJob(jobDetailMock, false);
			js.storeTrigger(triggerMock1, false);
			//trigger 1 is acquired
			InfinispanJobStoreSingleServerTest.triggerCache
				.get(ClusterCacheJobStore.generateKey(triggerKey1))
				.state = TriggerWrapper.STATE_ACQUIRED;
			//Inform about trigger fired
			js.triggersFired(firedTriggers);
			//trigger and job is now blocked
			js.storeTrigger(triggerMock2, false);
			assertEquals(TriggerState.BLOCKED, js.getTriggerState(triggerKey2));
		} catch (JobPersistenceException e) {
			fail("Unexpected Exception: ");
			e.printStackTrace();
		}
	}
	
	@Test
	public void testStoreTrigger_BlockedJobAndPausedGroup() {
		//Mock the Job
		JobDetail jobDetailMock = mock(JobDetail.class);
		JobKey jobKey = new JobKey("job", "group");
		when(jobDetailMock.getKey()).thenReturn(jobKey);
		//disallow concurrency
		when(jobDetailMock.isConcurrentExecutionDisallowed()).thenReturn(true);
		//Mock Triggers
		OperableTrigger triggerMock1= mock(OperableTrigger.class);
		OperableTrigger triggerMock2= mock(OperableTrigger.class);
		TriggerKey triggerKey1 = new TriggerKey("name1", "group");
		TriggerKey triggerKey2 = new TriggerKey("name2", "group");
		when(triggerMock1.getKey()).thenReturn(triggerKey1);
		when(triggerMock2.getKey()).thenReturn(triggerKey2);
		when(triggerMock1.getJobKey()).thenReturn(jobKey);
		when(triggerMock2.getJobKey()).thenReturn(jobKey);
		//Prepare List with trigger 1
		List<OperableTrigger> firedTriggers = new LinkedList<OperableTrigger>();
		firedTriggers.add(triggerMock1);
		//Mock SystemTime
		Date currentTime = new Date(1000L);
		TimeOfSystem systemTimeMock = mock(TimeOfSystem.class);
		js.setSystemTime(systemTimeMock);
		when(systemTimeMock.currentTimeInMilliSeconds()).thenReturn(currentTime.getTime());
		try{
			js.storeJob(jobDetailMock, false);
			js.storeTrigger(triggerMock1, false);
			//trigger 1 is acquired
			InfinispanJobStoreSingleServerTest.triggerCache
				.get(ClusterCacheJobStore.generateKey(triggerKey1))
				.state = TriggerWrapper.STATE_ACQUIRED;
			//Inform about trigger fired
			js.triggersFired(firedTriggers);
			js.pauseTriggers(GroupMatcher.triggerGroupEquals("group"));
			//trigger is now Paused and Blocked
			js.storeTrigger(triggerMock2, false);
			assertEquals(TriggerWrapper.STATE_PAUSED_AND_BLOCKED,
					InfinispanJobStoreSingleServerTest.triggerCache
					.get(ClusterCacheJobStore.generateKey(triggerKey2))
					.state);
		} catch (JobPersistenceException e) {
			fail("Unexpected Exception: ");
			e.printStackTrace();
		}
	}
	
	@Test
	public void testResumeTrigger_PausedAndBlockedTrigger() {
		//Mock the Job
		JobDetail jobDetailMock = mock(JobDetail.class);
		JobKey jobKey = new JobKey("job", "group");
		when(jobDetailMock.getKey()).thenReturn(jobKey);
		//disallow concurrency
		when(jobDetailMock.isConcurrentExecutionDisallowed()).thenReturn(true);
		//Mock Triggers
		OperableTrigger triggerMock1= mock(OperableTrigger.class);
		TriggerKey triggerKey1 = new TriggerKey("name1", "group");
		when(triggerMock1.getKey()).thenReturn(triggerKey1);
		when(triggerMock1.getJobKey()).thenReturn(jobKey);
		//Prepare List with trigger 1
		List<OperableTrigger> firedTriggers = new LinkedList<OperableTrigger>();
		firedTriggers.add(triggerMock1);
		//Mock SystemTime
		Date currentTime = new Date(1000L);
		TimeOfSystem systemTimeMock = mock(TimeOfSystem.class);
		js.setSystemTime(systemTimeMock);
		when(systemTimeMock.currentTimeInMilliSeconds()).thenReturn(currentTime.getTime());
		try{
			js.storeJob(jobDetailMock, false);
			js.storeTrigger(triggerMock1, false);
			//trigger 1 is acquired
			InfinispanJobStoreSingleServerTest.triggerCache
				.get(ClusterCacheJobStore.generateKey(triggerKey1))
				.state = TriggerWrapper.STATE_ACQUIRED;
			//Inform about trigger fired
			js.triggersFired(firedTriggers);
			js.pauseTriggers(GroupMatcher.triggerGroupEquals("group"));
			//trigger is now Paused and Blocked
			js.resumeTrigger(triggerKey1);
			assertEquals(TriggerWrapper.STATE_BLOCKED,
					InfinispanJobStoreSingleServerTest.triggerCache
					.get(ClusterCacheJobStore.generateKey(triggerKey1))
					.state);
		} catch (JobPersistenceException e) {
			fail("Unexpected Exception: ");
			e.printStackTrace();
		}
	}
	
	@Test
	public void testTriggersFired_PausedTriggerWithConcurrentJob() {
		//Mock the Job
		JobDetail jobDetailMock = mock(JobDetail.class);
		JobKey jobKey = new JobKey("job", "group");
		when(jobDetailMock.getKey()).thenReturn(jobKey);
		//disallow concurrency
		when(jobDetailMock.isConcurrentExecutionDisallowed()).thenReturn(true);
		//Mock Triggers
		OperableTrigger triggerMock1= mock(OperableTrigger.class);
		OperableTrigger triggerMock2= mock(OperableTrigger.class);
		TriggerKey triggerKey1 = new TriggerKey("name1", "group");
		TriggerKey triggerKey2 = new TriggerKey("name2", "group");
		when(triggerMock1.getKey()).thenReturn(triggerKey1);
		when(triggerMock2.getKey()).thenReturn(triggerKey2);
		when(triggerMock1.getJobKey()).thenReturn(jobKey);
		when(triggerMock2.getJobKey()).thenReturn(jobKey);
		//Prepare List with trigger 1
		List<OperableTrigger> firedTriggers = new LinkedList<OperableTrigger>();
		firedTriggers.add(triggerMock1);
		//Mock SystemTime
		Date currentTime = new Date(1000L);
		TimeOfSystem systemTimeMock = mock(TimeOfSystem.class);
		js.setSystemTime(systemTimeMock);
		when(systemTimeMock.currentTimeInMilliSeconds()).thenReturn(currentTime.getTime());
		try{
			js.storeJob(jobDetailMock, false);
			js.storeTrigger(triggerMock1, false);
			js.storeTrigger(triggerMock2, false);
			//trigger 1 is acquired
			InfinispanJobStoreSingleServerTest.triggerCache
				.get(ClusterCacheJobStore.generateKey(triggerKey1))
				.state = TriggerWrapper.STATE_ACQUIRED;
			//trigger 2 is paused
			js.pauseTrigger(triggerKey2);
			//Inform about trigger fired
			js.triggersFired(firedTriggers);
			//trigger is now Paused and Blocked
			assertEquals(TriggerWrapper.STATE_PAUSED_AND_BLOCKED,
					InfinispanJobStoreSingleServerTest.triggerCache
					.get(ClusterCacheJobStore.generateKey(triggerKey2))
					.state);
		} catch (JobPersistenceException e) {
			fail("Unexpected Exception: ");
			e.printStackTrace();
		}
	}
	
	@Test
	public void testTriggeredJobComplete_JobAndTriggerGotDeleted() {
		//Mock the Job
		JobDetail jobDetailMock = mock(JobDetail.class);
		JobKey jobKey = new JobKey("job", "group");
		when(jobDetailMock.getKey()).thenReturn(jobKey);
		//Mock Trigger
		OperableTrigger triggerMock= mock(OperableTrigger.class);
		TriggerKey triggerKey = new TriggerKey("name1", "group");
		when(triggerMock.getKey()).thenReturn(triggerKey);
		when(triggerMock.getJobKey()).thenReturn(jobKey);
		try {
			js.triggeredJobComplete(triggerMock, jobDetailMock, CompletedExecutionInstruction.NOOP);
		} catch(Exception e){
			fail("Unexpected Exception: ");
			e.printStackTrace();
		}
	}
	
	@Test
	public void testTriggeredJobComplete_cleanUpMetaData() {
		//Mock the Job
		JobDetail jobDetailMock = mock(JobDetail.class);
		JobKey jobKey = new JobKey("job", "group");
		when(jobDetailMock.getKey()).thenReturn(jobKey);
		//Mock Triggers
		OperableTrigger triggerMock= mock(OperableTrigger.class);
		TriggerKey triggerKey = new TriggerKey("name1", "group");
		when(triggerMock.getKey()).thenReturn(triggerKey);
		when(triggerMock.getJobKey()).thenReturn(jobKey);
		//Prepare List with trigger 1
		List<OperableTrigger> firedTriggers = new LinkedList<OperableTrigger>();
		firedTriggers.add(triggerMock);
		//Mock SystemTime
		Date currentTime = new Date(1000L);
		TimeOfSystem systemTimeMock = mock(TimeOfSystem.class);
		js.setSystemTime(systemTimeMock);
		when(systemTimeMock.currentTimeInMilliSeconds()).thenReturn(currentTime.getTime());
		//Mock Signaler 
		SchedulerSignaler signaler = mock(SchedulerSignaler.class);
		js.setSchedulerSignaler(signaler);
		//disallow concurrency
		when(jobDetailMock.isConcurrentExecutionDisallowed()).thenReturn(true);
		try{
			js.storeJob(jobDetailMock, false);
			js.storeTrigger(triggerMock, false);
			//set Trigger acquired
			InfinispanJobStoreSingleServerTest.triggerCache
				.get(ClusterCacheJobStore.generateKey(triggerKey))
				.state = TriggerWrapper.STATE_ACQUIRED;
			//Inform about trigger fired
			js.triggersFired(firedTriggers);
			//scheduler has started the job
			js.removeJob(jobKey);
			js.removeTrigger(triggerKey);
			//job and trigger got deleted
			Set<String> blockedJobs = InfinispanJobStoreSingleServerTest.metaCache.get(ClusterCacheJobStore.METAKEY_BLOCKED_JOBS_SET);
			assertTrue(blockedJobs.contains(ClusterCacheJobStore.generateKey(jobKey)));
			js.triggeredJobComplete(triggerMock, jobDetailMock, CompletedExecutionInstruction.NOOP);
			assertFalse(blockedJobs.contains(ClusterCacheJobStore.generateKey(jobKey)));
		} catch (JobPersistenceException e) {
			fail("Unexpected Exception: ");
			e.printStackTrace();
		}
	}
	
	@Test
	public void testTriggeredJobComplete_PersistDataAfterExecution() {
		JobDataMap jobDataMapMock = mock(JobDataMap.class);
		//Mock the Job
		JobDetail storedJobDetailMock 	 = mock(JobDetail.class, Mockito.RETURNS_DEEP_STUBS);
		JobDetail completedJobDetailMock = mock(JobDetail.class);
		JobKey jobKey = new JobKey("job", "group");
		//both jobs get the same key but only the new one will have a JobDataMap
		when(storedJobDetailMock.getKey()).thenReturn(jobKey);
		when(storedJobDetailMock.getJobBuilder().setJobData(jobDataMapMock).build()).thenReturn(storedJobDetailMock);
		when(completedJobDetailMock.getKey()).thenReturn(jobKey);
		when(completedJobDetailMock.getJobDataMap()).thenReturn(jobDataMapMock);
		//persist JobDataMap
		when(storedJobDetailMock.isPersistJobDataAfterExecution()).thenReturn(true);
		//Mock Trigger
		OperableTrigger triggerMock= mock(OperableTrigger.class);
		TriggerKey triggerKey = new TriggerKey("name1", "group");
		when(triggerMock.getKey()).thenReturn(triggerKey);
		when(triggerMock.getJobKey()).thenReturn(jobKey);
		try {
			//only add job so trigger seems to be deleted and will not interfere
			js.storeJob(storedJobDetailMock, false);
			//the data map of the new job should be persisted in the old job
			js.triggeredJobComplete(triggerMock, completedJobDetailMock, CompletedExecutionInstruction.NOOP);
			Mockito.verify(storedJobDetailMock, Mockito.times(2)).getJobBuilder();
			Mockito.verify(jobDataMapMock).clearDirtyFlag();
		} catch(Exception e){
			fail("Unexpected Exception: ");
			e.printStackTrace();
		}
	}
	
	@Test
	public void testTriggeredJobComplete_PersistDataAfterExecution_ButMapIsNull() {
		//Mock the Job
		JobDetail storedJobDetailMock 	 = mock(JobDetail.class, Mockito.RETURNS_DEEP_STUBS);
		JobDetail completedJobDetailMock = mock(JobDetail.class);
		JobKey jobKey = new JobKey("job", "group");
		when(storedJobDetailMock.getKey()).thenReturn(jobKey);
		when(storedJobDetailMock.getJobBuilder().setJobData(null).build()).thenReturn(storedJobDetailMock);
		when(completedJobDetailMock.getKey()).thenReturn(jobKey);
		when(completedJobDetailMock.getJobDataMap()).thenReturn(null);
		//persist JobDataMap
		when(storedJobDetailMock.isPersistJobDataAfterExecution()).thenReturn(true);
		//Mock Trigger
		OperableTrigger triggerMock= mock(OperableTrigger.class);
		TriggerKey triggerKey = new TriggerKey("name1", "group");
		when(triggerMock.getKey()).thenReturn(triggerKey);
		when(triggerMock.getJobKey()).thenReturn(jobKey);
		try {
			//only add job so trigger seems to be deleted and will not interfere
			js.storeJob(storedJobDetailMock, false);
			//the data map of the new job should be persisted in the old job
			js.triggeredJobComplete(triggerMock, completedJobDetailMock, CompletedExecutionInstruction.NOOP);
		} catch(Exception e){
			fail("Unexpected Exception: ");
			e.printStackTrace();
		}
	}
	
	@Test
	public void testTriggeredJobComplete_DoNotPersist() {
		JobDataMap jobDataMapMock = mock(JobDataMap.class);
		//Mock the Job
		JobDetail storedJobDetailMock 	 = mock(JobDetail.class);
		JobDetail completedJobDetailMock = mock(JobDetail.class);
		JobKey jobKey = new JobKey("job", "group");
		//both jobs get the same key but only the new one will have a JobDataMap
		when(storedJobDetailMock.getKey()).thenReturn(jobKey);
		when(completedJobDetailMock.getKey()).thenReturn(jobKey);
		when(completedJobDetailMock.getJobDataMap()).thenReturn(jobDataMapMock);
		//don't persist JobDataMap
		when(storedJobDetailMock.isPersistJobDataAfterExecution()).thenReturn(false);
		//Mock Trigger
		OperableTrigger triggerMock= mock(OperableTrigger.class);
		TriggerKey triggerKey = new TriggerKey("name1", "group");
		when(triggerMock.getKey()).thenReturn(triggerKey);
		when(triggerMock.getJobKey()).thenReturn(jobKey);
		try {
			//only add job so trigger seems to be deleted and will not interfere
			js.storeJob(storedJobDetailMock, false);
			//the data map of the new job should be persisted in the old job
			js.triggeredJobComplete(triggerMock, completedJobDetailMock, CompletedExecutionInstruction.NOOP);
			Mockito.verify(storedJobDetailMock, Mockito.never()).getJobBuilder();
			Mockito.verify(jobDataMapMock, Mockito.never()).clearDirtyFlag();
		} catch(Exception e){
			fail("Unexpected Exception: ");
			e.printStackTrace();
		}
	}
	
	@Test
	public void testTriggeredJobComplete_concurrentExecutionDisallowed() {
		//Mock the Job
		JobDetail jobDetailMock = mock(JobDetail.class);
		JobKey jobKey = new JobKey("job", "group");
		when(jobDetailMock.getKey()).thenReturn(jobKey);
		//Mock Triggers
		OperableTrigger triggerMock1= mock(OperableTrigger.class);
		OperableTrigger triggerMock2= mock(OperableTrigger.class);
		OperableTrigger triggerMock3= mock(OperableTrigger.class);
		TriggerKey triggerKey1 = new TriggerKey("name1", "group");
		TriggerKey triggerKey2 = new TriggerKey("name2", "group");
		TriggerKey triggerKey3 = new TriggerKey("name3", "group");
		when(triggerMock1.getKey()).thenReturn(triggerKey1);
		when(triggerMock2.getKey()).thenReturn(triggerKey2);
		when(triggerMock3.getKey()).thenReturn(triggerKey3);
		when(triggerMock1.getJobKey()).thenReturn(jobKey);
		when(triggerMock2.getJobKey()).thenReturn(jobKey);
		when(triggerMock3.getJobKey()).thenReturn(jobKey);
		//Prepare List with trigger 1
		List<OperableTrigger> firedTriggers = new LinkedList<OperableTrigger>();
		firedTriggers.add(triggerMock1);
		//Mock SystemTime
		Date currentTime = new Date(1000L);
		TimeOfSystem systemTimeMock = mock(TimeOfSystem.class);
		js.setSystemTime(systemTimeMock);
		when(systemTimeMock.currentTimeInMilliSeconds()).thenReturn(currentTime.getTime());
		//forbid concurrency
		when(jobDetailMock.isConcurrentExecutionDisallowed()).thenReturn(true);
		//Mock Signaler 
		SchedulerSignaler signaler = mock(SchedulerSignaler.class);
		js.setSchedulerSignaler(signaler);
		try {
			js.storeTrigger(triggerMock1, false);
			js.storeTrigger(triggerMock2, false);
			js.storeTrigger(triggerMock3, false);
			js.pauseTrigger(triggerKey3);
			js.storeJob(jobDetailMock, false);
			//set Trigger acquired
			InfinispanJobStoreSingleServerTest.triggerCache
				.get(ClusterCacheJobStore.generateKey(triggerKey1))
				.state = TriggerWrapper.STATE_ACQUIRED;
			//Inform about trigger fired
			js.triggersFired(firedTriggers);
			//triggers are now blocked
			js.triggeredJobComplete(triggerMock1, jobDetailMock, CompletedExecutionInstruction.NOOP);
			//trigger 2 should be normal again
			assertEquals(TriggerWrapper.STATE_NORMAL,
				InfinispanJobStoreSingleServerTest.triggerCache
				.get(ClusterCacheJobStore.generateKey(triggerKey2))
				.state);
			//trigger 3 should be paused again
			assertEquals(TriggerWrapper.STATE_PAUSED,
				InfinispanJobStoreSingleServerTest.triggerCache
				.get(ClusterCacheJobStore.generateKey(triggerKey3))
				.state);
			//delete metadata
			Set<String> blockedJobs = InfinispanJobStoreSingleServerTest.metaCache
					.get(ClusterCacheJobStore.METAKEY_BLOCKED_JOBS_SET);
			assertFalse(blockedJobs.contains(ClusterCacheJobStore.generateKey(jobKey)));
			//tell the scheduler
			Mockito.verify(signaler).signalSchedulingChange(0L);
		} catch(Exception e){
			fail("Unexpected Exception: ");
			e.printStackTrace();
		}
	}
	
	@Test
	public void testTriggeredJobComplete_triggerInstructionDelete(){
		//Mock the Job
		JobDetail jobDetailMock = mock(JobDetail.class);
		JobKey jobKey = new JobKey("job", "group");
		when(jobDetailMock.getKey()).thenReturn(jobKey);
		//Mock Trigger
		OperableTrigger storedTriggerMock= mock(OperableTrigger.class);
		OperableTrigger completeTriggerMock= mock(OperableTrigger.class);
		TriggerKey triggerKey = new TriggerKey("name1", "group");
		when(storedTriggerMock.getKey()).thenReturn(triggerKey);
		when(completeTriggerMock.getKey()).thenReturn(triggerKey);
		when(storedTriggerMock.getJobKey()).thenReturn(jobKey);
		when(completeTriggerMock.getJobKey()).thenReturn(jobKey);
		//Mock Signaler 
		SchedulerSignaler signaler = mock(SchedulerSignaler.class);
		js.setSchedulerSignaler(signaler);
		try {
			//Delete Trigger
			js.storeTrigger(storedTriggerMock, false);
			js.triggeredJobComplete(storedTriggerMock, jobDetailMock, 
					CompletedExecutionInstruction.DELETE_TRIGGER);
			assertTrue(js.retrieveTrigger(triggerKey) == null); 
			
			//Trigger got rescheduled and should not be removed
			js.storeTrigger(storedTriggerMock, true);
			//the Stored trigger fires at a new Date -> got rescheduled
			when(storedTriggerMock.getNextFireTime()).thenReturn(new Date());
			js.triggeredJobComplete(completeTriggerMock, jobDetailMock, 
					CompletedExecutionInstruction.DELETE_TRIGGER);
			assertTrue(js.retrieveTrigger(triggerKey) == storedTriggerMock);
			
			//Trigger should fire again signaler should be notified
			when(completeTriggerMock.getNextFireTime()).thenReturn(new Date());
			js.triggeredJobComplete(completeTriggerMock, jobDetailMock, 
					CompletedExecutionInstruction.DELETE_TRIGGER);
			Mockito.verify(signaler).signalSchedulingChange(0L);
			
		} catch(Exception e){
			fail("Unexpected Exception: ");
			e.printStackTrace();
		}
	}
	
	@Test
	public void testTriggeredJobComplete_triggerSetTriggerComplete(){
		//Mock the Job
		JobDetail jobDetailMock = mock(JobDetail.class);
		JobKey jobKey = new JobKey("job", "group");
		when(jobDetailMock.getKey()).thenReturn(jobKey);
		//Mock Trigger
		OperableTrigger storedTriggerMock= mock(OperableTrigger.class);
		TriggerKey triggerKey = new TriggerKey("name", "group");
		when(storedTriggerMock.getKey()).thenReturn(triggerKey);
		when(storedTriggerMock.getJobKey()).thenReturn(jobKey);
		//Mock Signaler 
		SchedulerSignaler signaler = mock(SchedulerSignaler.class);
		js.setSchedulerSignaler(signaler);
		try {
			js.storeTrigger(storedTriggerMock, false);
			js.triggeredJobComplete(storedTriggerMock, jobDetailMock, 
				CompletedExecutionInstruction.SET_TRIGGER_COMPLETE);
			assertEquals(TriggerState.COMPLETE, js.getTriggerState(triggerKey));
			Mockito.verify(signaler).signalSchedulingChange(0L);
		} catch(Exception e){
			fail("Unexpected Exception: ");
			e.printStackTrace();
		}
	}
	
	@Test
	public void testTriggeredJobComplete_triggerSetTriggerError(){
		//Mock the Job
		JobDetail jobDetailMock = mock(JobDetail.class);
		JobKey jobKey = new JobKey("job", "group");
		when(jobDetailMock.getKey()).thenReturn(jobKey);
		//Mock Trigger
		OperableTrigger storedTriggerMock= mock(OperableTrigger.class);
		TriggerKey triggerKey = new TriggerKey("name", "group");
		when(storedTriggerMock.getKey()).thenReturn(triggerKey);
		when(storedTriggerMock.getJobKey()).thenReturn(jobKey);
		//Mock Signaler 
		SchedulerSignaler signaler = mock(SchedulerSignaler.class);
		js.setSchedulerSignaler(signaler);
		try {
			js.storeTrigger(storedTriggerMock, false);
			js.triggeredJobComplete(storedTriggerMock, jobDetailMock, 
				CompletedExecutionInstruction.SET_TRIGGER_ERROR);
			assertEquals(TriggerState.ERROR, js.getTriggerState(triggerKey));
			Mockito.verify(signaler).signalSchedulingChange(0L);
		} catch(Exception e){
			fail("Unexpected Exception: ");
			e.printStackTrace();
		}
	}
	
	@Test
	public void testTriggeredJobComplete_triggerSetAllTriggerError(){
		//Mock the Job
		JobDetail jobDetailMock = mock(JobDetail.class);
		JobKey jobKey = new JobKey("job", "group");
		when(jobDetailMock.getKey()).thenReturn(jobKey);
		//Mock Trigger
		OperableTrigger triggerMock1= mock(OperableTrigger.class);
		TriggerKey triggerKey1 = new TriggerKey("name1", "group");
		when(triggerMock1.getKey()).thenReturn(triggerKey1);
		when(triggerMock1.getJobKey()).thenReturn(jobKey);
		OperableTrigger triggerMock2= mock(OperableTrigger.class);
		TriggerKey triggerKey2 = new TriggerKey("name2", "group");
		when(triggerMock2.getKey()).thenReturn(triggerKey2);
		when(triggerMock2.getJobKey()).thenReturn(jobKey);
		//Mock Signaler 
		SchedulerSignaler signaler = mock(SchedulerSignaler.class);
		js.setSchedulerSignaler(signaler);
		try {
			js.storeTrigger(triggerMock1, false);
			js.storeTrigger(triggerMock2, false);
			js.triggeredJobComplete(triggerMock1, jobDetailMock, 
				CompletedExecutionInstruction.SET_ALL_JOB_TRIGGERS_ERROR);
			assertEquals(TriggerState.ERROR, js.getTriggerState(triggerKey1));
			assertEquals(TriggerState.ERROR, js.getTriggerState(triggerKey2));
			Mockito.verify(signaler).signalSchedulingChange(0L);
		} catch(Exception e){
			fail("Unexpected Exception: ");
			e.printStackTrace();
		}
	}
	
	@Test
	public void testTriggeredJobComplete_triggerSetAllTriggerComplete(){
		//Mock the Job
		JobDetail jobDetailMock = mock(JobDetail.class);
		JobKey jobKey = new JobKey("job", "group");
		when(jobDetailMock.getKey()).thenReturn(jobKey);
		//Mock Trigger
		OperableTrigger triggerMock1= mock(OperableTrigger.class);
		TriggerKey triggerKey1 = new TriggerKey("name1", "group");
		when(triggerMock1.getKey()).thenReturn(triggerKey1);
		when(triggerMock1.getJobKey()).thenReturn(jobKey);
		OperableTrigger triggerMock2= mock(OperableTrigger.class);
		TriggerKey triggerKey2 = new TriggerKey("name2", "group");
		when(triggerMock2.getKey()).thenReturn(triggerKey2);
		when(triggerMock2.getJobKey()).thenReturn(jobKey);
		//Mock Signaler 
		SchedulerSignaler signaler = mock(SchedulerSignaler.class);
		js.setSchedulerSignaler(signaler);
		try {
			js.storeTrigger(triggerMock1, false);
			js.storeTrigger(triggerMock2, false);
			js.triggeredJobComplete(triggerMock1, jobDetailMock, 
				CompletedExecutionInstruction.SET_ALL_JOB_TRIGGERS_COMPLETE);
			assertEquals(TriggerState.COMPLETE, js.getTriggerState(triggerKey1));
			assertEquals(TriggerState.COMPLETE, js.getTriggerState(triggerKey2));
			Mockito.verify(signaler).signalSchedulingChange(0L);
		} catch(Exception e){
			fail("Unexpected Exception: ");
			e.printStackTrace();
		}
	}
}