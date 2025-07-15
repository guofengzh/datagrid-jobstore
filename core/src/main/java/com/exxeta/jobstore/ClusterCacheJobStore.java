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

import static org.quartz.SimpleScheduleBuilder.simpleSchedule;
import static org.quartz.TriggerBuilder.newTrigger;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicLong;

import org.jboss.logging.Logger;
import org.quartz.Calendar;
import org.quartz.JobDataMap;
import org.quartz.JobDetail;
import org.quartz.JobKey;
import org.quartz.JobPersistenceException;
import org.quartz.ObjectAlreadyExistsException;
import org.quartz.Scheduler;
import org.quartz.SchedulerConfigException;
import org.quartz.SchedulerException;
import org.quartz.SimpleTrigger;
import org.quartz.Trigger;
import org.quartz.Trigger.CompletedExecutionInstruction;
import org.quartz.Trigger.TriggerState;
import org.quartz.Trigger.TriggerTimeComparator;
import org.quartz.TriggerKey;
import org.quartz.impl.matchers.GroupMatcher;
import org.quartz.spi.ClassLoadHelper;
import org.quartz.spi.JobStore;
import org.quartz.spi.OperableTrigger;
import org.quartz.spi.SchedulerSignaler;
import org.quartz.spi.TriggerFiredBundle;
import org.quartz.spi.TriggerFiredResult;
import org.quartz.utils.Key;

/**
 * <p>
 * This <code>{@link org.quartz.spi.JobStore}</code> ueses 
 * Infinispan as persistence layer for Job's and Trigger's 
 * Data. 
 * 
 * @author Felix Finkbeiner
 */
public abstract class ClusterCacheJobStore implements JobStore {
	
	//Meta Data Keys
	public static final String METAKEY_PAUSED_TRIGGER_GROUPS_SET = "pausedTriggerGroups";
	public static final String METAKEY_PAUSED_JOB_GROUPS_SET = "pausedJobGroups";
	public static final String METAKEY_BLOCKED_JOBS_SET = "blockedJobs";
	public static final String METAKEY_NODE_ID_SET = "nodeIds";
	
	/**Connector to the Cache that holds the Data*/
	protected CacheConnector connector;
	
	//This interfaces is needed for time dependent testing
	protected TimeOfSystem systemTime = null;

	//JobStore function
	private volatile boolean shutdown = false;
	/**Delay within the trigger is not counted as misfired*/
	protected long misfireThreshold = 5000l;
	/**Time that is waited for retrying critical operations*/
	protected long retryTime = 15000L;
	/** Allows to cummunicate back to the Scheduler */
	protected SchedulerSignaler schedulerSignaler = null;
	
	//Cluster 
	private String nodeId = null;

	//Logging
	private static final Logger LOGGER_RUNNING_CHANGE  			= Logger.getLogger("com.exxeta.jobstore.ClusterCacheJobStore.RunningInfo");
	private static final Logger LOGGER_RUNNING_CHANGE_MISFIRE  	= Logger.getLogger("com.exxeta.jobstore.ClusterCacheJobStore.RunningInfo.misfire");
	private static final Logger LOGGER_JOBSTORE_CHANGE 			= Logger.getLogger("com.exxeta.jobstore.ClusterCacheJobStore.JobStoreInfo");
	private static final Logger LOGGER_RECOVERY 	  			= Logger.getLogger("com.exxeta.jobstore.ClusterCacheJobStore.Recovery");
	
	/**
	 * This Method should create a {@link com.exxeta.jobstore.CacheConnector}
	 * and set it as {@link #connector}
	 */
	protected abstract void createCacheConnector() throws SchedulerConfigException;
	
	
	@Override
	public void initialize(ClassLoadHelper loadHelper,
			SchedulerSignaler signaler) throws SchedulerConfigException {

		this.createCacheConnector();
		
		this.schedulerSignaler = signaler;
		
		this.systemTime = new TimeOfSystem() {
			@Override
			public long currentTimeInMilliSeconds() { 
				return System.currentTimeMillis(); 
			}
		};
		
		nodeId = connector.getNodeID();
		LOGGER_JOBSTORE_CHANGE.debug("New Jobstore Initialized with node ID: "+nodeId);
		if(nodeId == null || nodeId.equals("")){
			throw new SchedulerConfigException("The Node Name is Null or an empty string please specify -Djboss.node.name");
		}
	}
	
	@Override
	public void storeJob(final JobDetail newJob, final boolean replaceExisting)
			throws ObjectAlreadyExistsException, JobPersistenceException {
		
		connector.doInTransaction(new TransactionScoped() {
			
			@Override
			public void doInTransaction() throws JobPersistenceException {
				
				JobKey jko = newJob.getKey();
				String newJobKey = ClusterCacheJobStore.generateKey(jko);
				connector.lockJob(newJobKey);
				
				if(!replaceExisting && checkExists(jko)){
					//Existing job should not be replaced
					//release lock
					throw new ObjectAlreadyExistsException(newJob);			
				}
				connector.putJob(newJobKey, newJob);	
				
				LOGGER_JOBSTORE_CHANGE.info("Stored Job: "+jko.getName()+" Group: "+jko.getGroup());
				
			}
		});
	}
	
	@Override
	public boolean removeTrigger(TriggerKey triggerKey)
			throws JobPersistenceException {
		
		LOGGER_JOBSTORE_CHANGE.debug("Removing Trigger: "+ClusterCacheJobStore.generateKey(triggerKey));
		final String triggerToRemoveKey = ClusterCacheJobStore.generateKey(triggerKey);

		try {
			connector.doInTransaction(new TransactionScoped() {
				@Override
				public void doInTransaction() throws JobPersistenceException {
					
					connector.lockTrigger(triggerToRemoveKey);
					TriggerWrapper triggerToRemove = connector.getTriggerWrapper(triggerToRemoveKey);
					
					if(triggerToRemove != null){
						connector.removeTriggerWrapper(triggerToRemoveKey);
						//get the possibly orphan job
						String deletedTriggersJobKey = ClusterCacheJobStore.generateKey(triggerToRemove.trigger.getJobKey());
						connector.lockJob(deletedTriggersJobKey);
						JobDetail deletedTriggersJob = connector.getJob(deletedTriggersJobKey);
						
						if(deletedTriggersJob != null && !deletedTriggersJob.isDurable()){
							//test if job is orphan
							List<OperableTrigger> triggerList = getTriggersForJob(deletedTriggersJob.getKey());
							if(triggerList.size() == 0){
								//job is orphan tell the scheduler about removal
								if(removeJob(deletedTriggersJob.getKey())){
									schedulerSignaler.notifySchedulerListenersJobDeleted(deletedTriggersJob.getKey());
								}
							}
						}
					} else {
						//Trigger not found
						throw new AbortTransactionException("trigger not found");
					}
					
				}
			});
		} catch (AbortTransactionException ate){
			return false;
		}
		return true;
	}
	
	/**
	 * Removes the Job, all Triggers referenced by it
	 * and all Groups that are empty after removal
	 */
	@Override
	public boolean removeJob(final JobKey jobKey) throws JobPersistenceException {
		
		LOGGER_JOBSTORE_CHANGE.debug("Job Deleted: "+ClusterCacheJobStore.generateKey(jobKey));
		
		class Bool { public boolean value; }
		
		final Bool foundJobOrTriggerToDelete = new Bool();
		foundJobOrTriggerToDelete.value = false;
		
		connector.doInTransaction(new TransactionScoped() {
			@Override
			public void doInTransaction() throws JobPersistenceException {

				//remove all the triggers referenced with the Job
				List<OperableTrigger> triggersOfJob = getTriggersForJob(jobKey);
				for(OperableTrigger trigger : triggersOfJob){
					removeTrigger(trigger.getKey()); 
					foundJobOrTriggerToDelete.value = true;
				}
				
				//Delete Job
				if(checkExists(jobKey)){ 
					String jobToDeleteKey =ClusterCacheJobStore.generateKey(jobKey);
					//return value of remove is deactivated
					connector.lockJob(jobToDeleteKey);
					connector.removeJob(jobToDeleteKey);
					foundJobOrTriggerToDelete.value = true;
				}
				
			}
		});
		
		return foundJobOrTriggerToDelete.value;
		
	}
	
	/**
	 * @return all the Triggers that fire the given job
	 */
	@Override
	public List<OperableTrigger> getTriggersForJob(final JobKey jobKey)
			throws JobPersistenceException {
		//Does not use Transaction
		List<OperableTrigger> jobsTriggers = new LinkedList<OperableTrigger>();
		
		for(String triggerKey: connector.getAllTriggerKeys()){
			OperableTrigger trigger = connector.getTriggerWrapper(triggerKey).trigger;
			if(trigger.getJobKey().equals(jobKey)){
				jobsTriggers.add(trigger);
			}
		}
		
		return jobsTriggers;
	}
	
	/**
	 * Same as getTriggersForJob but with the Wrapper
	 * @param jobKey
	 * @return
	 */
	protected List<TriggerWrapper> getTriggerWrappersForJob(final JobKey jobKey){
		//does also not use transactions
		List<TriggerWrapper> jobsTriggers = new LinkedList<TriggerWrapper>();
		
		for(String triggerKey: connector.getAllTriggerKeys()){ 
			TriggerWrapper tw = connector.getTriggerWrapper(triggerKey);
			if(tw.trigger.getJobKey().equals(jobKey)){
				jobsTriggers.add(tw);
			}
		}
		return jobsTriggers;
	}
	
	@Override
	public void storeJobsAndTriggers(
			final Map<JobDetail, Set<? extends Trigger>> triggersAndJobs,
			final boolean replace) throws ObjectAlreadyExistsException,
			JobPersistenceException {
		
		connector.doInTransaction(new TransactionScoped() {
			@Override
			public void doInTransaction() throws JobPersistenceException {

				//add all the jobs and triggers
				for(Map.Entry<JobDetail, Set<? extends Trigger>> entry: triggersAndJobs.entrySet()){
					//if object already exists and storeJob/storeTrigger roles back all changes roll back
					storeJob(entry.getKey(), replace);
					for(Trigger trigger: entry.getValue()){
						storeTrigger((OperableTrigger) trigger, replace);
					}
				}

			}
		});
		
	}
	
	@Override
	public void storeTrigger(OperableTrigger newTrigger, boolean replaceExisting)
			throws ObjectAlreadyExistsException, JobPersistenceException {
		this.storeTrigger(newTrigger, replaceExisting, false);
	}
	
	private void storeTrigger(OperableTrigger newTrigger, boolean replaceExisting, boolean ignoreGroups)
			 throws ObjectAlreadyExistsException, JobPersistenceException{
		this.storeTrigger(newTrigger, replaceExisting, ignoreGroups, false);
	}
	
	
	private void storeTrigger(final OperableTrigger newTrigger, final boolean replaceExisting, final boolean ignoreGroups, final boolean recovering)
			 throws ObjectAlreadyExistsException, JobPersistenceException{
		final TriggerKey tko = newTrigger.getKey();
		final String newTriggerKey = ClusterCacheJobStore.generateKey(tko);
		
		connector.doInTransaction(new TransactionScoped() {
			@Override
			public void doInTransaction() throws JobPersistenceException {

				connector.lockTrigger(newTriggerKey);
				
				if(!replaceExisting && checkExists(tko)){
					//Trigger key already exists but should not be replaced
					throw new ObjectAlreadyExistsException(newTrigger);
				}
				//Wrap the trigger for state
				TriggerWrapper tw = new TriggerWrapper(newTrigger);
				tw.recovering = recovering;
				
				if(!ignoreGroups){
					//is the group of the trigger paused then pause the new trigger
					//lock the meta data so group does not get unpaused while storing
					connector.lockMetaData(METAKEY_PAUSED_TRIGGER_GROUPS_SET);
					Set<String> pausedGroups = connector.getMetaData(METAKEY_PAUSED_TRIGGER_GROUPS_SET);
					if(pausedGroups != null){
						if(pausedGroups.contains(tko.getGroup())){
							tw.state = TriggerWrapper.STATE_PAUSED;
						}
					}
					//Block the trigger if its job is blocked
					connector.lockMetaData(METAKEY_BLOCKED_JOBS_SET);
					Set<String> blockedJobs = connector.getMetaData(METAKEY_BLOCKED_JOBS_SET);
					if(blockedJobs != null){
						if(blockedJobs.contains(
								ClusterCacheJobStore.generateKey(newTrigger.getJobKey()))){
							if(tw.state == TriggerWrapper.STATE_PAUSED){
								tw.state = TriggerWrapper.STATE_PAUSED_AND_BLOCKED;
							} else  {
								tw.state = TriggerWrapper.STATE_BLOCKED;
							}
						}
					}
				}
				
				connector.putTriggerWrapper(newTriggerKey, tw);
				
			}
		});
		
		LOGGER_JOBSTORE_CHANGE.info("Stored Trigger: "+tko.getName()+" Group: "+tko.getGroup());
	}
	
	@Override
	public JobDetail retrieveJob(JobKey jobKey) throws JobPersistenceException {
		return connector.getJob(ClusterCacheJobStore.generateKey(jobKey));
	}
	
	@Override
	public boolean checkExists(JobKey jobKey) throws JobPersistenceException {
		return connector.containsJob(ClusterCacheJobStore.generateKey(jobKey));
	}

	@Override
	public boolean checkExists(TriggerKey triggerKey) throws JobPersistenceException {
		return connector.containsTrigger(ClusterCacheJobStore.generateKey(triggerKey));
	}
	
	/** Get the names of the Trigger groups 
	 * @return A list with the group names or an empty list if there are no groups
	 */
	@Override
	public List<String> getTriggerGroupNames() throws JobPersistenceException {
		//Oder evtl in meta daten speichern?
		Set<String> groups = new HashSet<String>();
		for(String triggerKey: connector.getAllTriggerKeys()){
			groups.add(connector.getTriggerWrapper(triggerKey).trigger.getKey().getGroup());
		}
		ArrayList<String> groupsList = new ArrayList<String>();
		groupsList.addAll(groups);
		return groupsList;
	}
	
	/** Get the names of the Job groups
	 * @return A list with the group names or an empty list if there are no groups
	 */
	@Override
	public List<String> getJobGroupNames() throws JobPersistenceException {
		Set<String> groups = new HashSet<String>();
		for(String jobKey: connector.getAllJobKeys()){
			groups.add(connector.getJob(jobKey).getKey().getGroup());
		}
		ArrayList<String> groupsList = new ArrayList<String>();
		groupsList.addAll(groups);
		return groupsList;
	}
	
	@Override
	public boolean removeJobs(List<JobKey> jobKeys)
			throws JobPersistenceException {
		boolean allFound = true;
		for(JobKey key: jobKeys) {
			allFound = removeJob(key) && allFound;
		}
        return allFound;
	}

	@Override
	public boolean removeTriggers(List<TriggerKey> triggerKeys)
			throws JobPersistenceException {
		boolean allFound = true;
		for(TriggerKey key: triggerKeys) {
			allFound = removeTrigger(key) && allFound;
		}
        return allFound;
	}

	/**
	 * If the trigger has been found it will be replaced by the new trigger
	 * @return true if the given trigger has been found and replaced, false if the trigger has not been found and no storing of the new trigger has been done
	 * @throws JobPersistenceException when the new trigger starts a different job than the old one
	 */
	@Override
	public boolean replaceTrigger(final TriggerKey triggerKey, final OperableTrigger newTrigger) 
			throws JobPersistenceException {
		
		
		class Bool { public boolean value; }
		
		final Bool triggerFound = new Bool();
		final String oldTriggerKey = ClusterCacheJobStore.generateKey(triggerKey);
		
		connector.doInTransaction(new TransactionScoped() {
			@Override
			public void doInTransaction() throws JobPersistenceException {
				connector.lockTrigger(oldTriggerKey);
				//get the old Trigger
				TriggerWrapper oldTriggerWrapper = connector.getTriggerWrapper(oldTriggerKey);
				
				if(oldTriggerWrapper != null){
					//old trigger exists
					if(!oldTriggerWrapper.trigger.getJobKey().equals(newTrigger.getJobKey())){
						//related Jobs are not equal
						throw new JobPersistenceException("New trigger is not related to the same job as the old trigger. " + triggerKey.toString());
					}
					//if trigger can't be replaced transaction rolles back
					storeTrigger(newTrigger, true);
					
					triggerFound.value = true;
				} else {
					triggerFound.value = false;
				}
			}
		});
			
		return triggerFound.value;
	}

	@Override
	public OperableTrigger retrieveTrigger(TriggerKey triggerKey)
			throws JobPersistenceException {
		TriggerWrapper tw = connector.getTriggerWrapper(ClusterCacheJobStore.generateKey(triggerKey));
		if(tw != null){
			return tw.trigger;
		} else{
			return null;
		}
	}
	
	/** Delete all scheduling data
	 */
	@Override
	public void clearAllSchedulingData() throws JobPersistenceException {
		//Caution clear hardly depends on the cache configuration
		//clearing one cache can also (unintended) clear all the other caches
		//it should only be used when all the caches are needed to be emptied 
		
		connector.doInTransaction(new TransactionScoped() {
			@Override
			public void doInTransaction() throws JobPersistenceException {

				connector.lockMetaData(METAKEY_NODE_ID_SET);
				Set<String> nodes = connector.getMetaData(METAKEY_NODE_ID_SET);
				connector.clearAllData();
				connector.putMetaData(METAKEY_NODE_ID_SET, nodes);
				
			}
		});
		LOGGER_JOBSTORE_CHANGE.info("All Scheduling Data has been cleared!");
	}

	 /**
     * <p>
     * Store the given <code>{@link org.quartz.Calendar}</code>.
     * </p>
     *
     * @param calendar
     *          The <code>Calendar</code> to be stored.
     * @param replaceExisting
     *          If <code>true</code>, any <code>Calendar</code> existing
     *          in the <code>JobStore</code> with the same name & group
     *          should be over-written.
     * @param updateTriggers
     *          If <code>true</code>, any <code>Trigger</code>s existing
     *          in the <code>JobStore</code> that reference an existing
     *          Calendar with the same name have their next fire time
     *          re-computed with the new <code>Calendar</code>.
     * @throws ObjectAlreadyExistsException
     *           if a <code>Calendar</code> with the same name already
     *           exists, and replaceExisting is set to false.
     */
	@Override
	public void storeCalendar(final String name, final Calendar calendar,
			final boolean replaceExisting, final boolean updateTriggers)
			throws ObjectAlreadyExistsException, JobPersistenceException {
		
		connector.doInTransaction(new TransactionScoped() {
			
			@Override
			public void doInTransaction() throws JobPersistenceException {

				connector.lockCalendar(name);
				Calendar oldCalendar = connector.getCalendar(name);
				if(oldCalendar != null && !replaceExisting){
					//don't replace the existing
					throw new ObjectAlreadyExistsException("The following Calendar does already exist: " +name);
				}  
				if(oldCalendar != null && updateTriggers){
					//get all triggers with the calender name
					Set<String> triggersOfCalendar = getTriggerForCalendar(name);
					
					if(triggersOfCalendar.size() > 0){
						connector.lockTrigger(triggersOfCalendar);
					}
					
					for(String triggerKey: triggersOfCalendar){
						
						OperableTrigger triggerToUpdate = connector.getTriggerWrapper(triggerKey).trigger;
						
						if(triggerToUpdate.getCalendarName().equals(name)){
							//Trigger name did not get changed since getting it
							//update the trigger and repersistate it
							triggerToUpdate.updateWithNewCalendar(calendar, getMisfireThreshold());
							connector.putTriggerWrapper(triggerKey, new TriggerWrapper(triggerToUpdate));
						}
					}
				}
				//replace the old calendar
				connector.putCalendar(name, calendar);
				
			}
		});
		LOGGER_JOBSTORE_CHANGE.info("Stored Calendar: "+name);
	}
	
	/**
	 * @param name of the <code>{@link org.quartz.Calendar}</code> to which the trigger references should exist
	 * @return all the trigger keys for a the given <code>{@link org.quartz.Calendar}</code> name or an empty set
	 */
	public Set<String> getTriggerForCalendar(String name){
		
		Set<String> calendarsTrigger = new HashSet<String>();
		for(String triggerKey: connector.getAllTriggerKeys()){
			OperableTrigger ot = connector.getTriggerWrapper(triggerKey).trigger;
			String calName = ot.getCalendarName();
			if(calName.equals(name)){
				calendarsTrigger.add(triggerKey);
			}
		}
		return calendarsTrigger;
	}
	
	/**
	 * @throws JobPersistenceException when the calendar is still referenced by a trigger
	 */
	@Override
	public boolean removeCalendar(final String calName)
			throws JobPersistenceException {
		
		boolean isRemoved = false;
		
		for(String triggerKey: connector.getAllTriggerKeys()){
			String cn = connector.getTriggerWrapper(triggerKey).trigger.getCalendarName();
			if(cn.equals(calName)){
				//calendar is still referenced by a trigger
				throw new JobPersistenceException("Calendar: " + calName + " can not be removed, it still is referenced by a Trigger");
			}
		}

		if(connector.containsCalendar(calName)){
			connector.doInTransaction(new TransactionScoped() {
				@Override
				public void doInTransaction() throws JobPersistenceException {
					connector.lockCalendar(calName);
					connector.removeCalendar(calName);
				}
			});
			isRemoved = true;
		}
			
		return isRemoved;
	}
	
	/**@return the Calendar with the given name or null if it does not exist*/
	@Override
	public Calendar retrieveCalendar(String calName)
			throws JobPersistenceException {
		return connector.getCalendar(calName);
	}
	
	@Override
	public Set<JobKey> getJobKeys(GroupMatcher<JobKey> matcher)
			throws JobPersistenceException {
		
		Set<JobKey> jobKeyRet = new HashSet<JobKey>();
		
		for(String jobKey: connector.getAllJobKeys()){
			JobKey jobKeyObj = connector.getJob(jobKey).getKey();
			if(matcher.isMatch(jobKeyObj)){
				jobKeyRet.add(jobKeyObj);
			}
		}
		return jobKeyRet;
	}

	@Override
	public Set<TriggerKey> getTriggerKeys(GroupMatcher<TriggerKey> matcher)
			throws JobPersistenceException {
		Set<TriggerKey> triggerKeyRet = new HashSet<TriggerKey>();
		
		for(String triggerKey: connector.getAllTriggerKeys()){
			TriggerKey triggerKeyObj = connector.getTriggerWrapper(triggerKey).trigger.getKey();
			if(matcher.isMatch(triggerKeyObj)){
				triggerKeyRet.add(triggerKeyObj);
			}
		}
		return triggerKeyRet;
	}

	@Override
	public List<String> getCalendarNames() throws JobPersistenceException {
		LinkedList<String> calNames = new LinkedList<String>();
		calNames.addAll(connector.getAllCalendarNames());
		return calNames;
	}
	
	@Override
	public TriggerState getTriggerState(TriggerKey triggerKey)
			throws JobPersistenceException {
		
		TriggerWrapper tw = connector.getTriggerWrapper(ClusterCacheJobStore.generateKey(triggerKey));
		if(tw == null){
			return TriggerState.NONE;
		}
		if (tw.state == TriggerWrapper.STATE_COMPLETE) {
            return TriggerState.COMPLETE;
        }
        if (tw.state == TriggerWrapper.STATE_PAUSED) {
            return TriggerState.PAUSED;
        }
        if (tw.state == TriggerWrapper.STATE_PAUSED_AND_BLOCKED) {
            return TriggerState.PAUSED;
        }
        if (tw.state == TriggerWrapper.STATE_BLOCKED) {
            return TriggerState.BLOCKED;
        }
        if (tw.state == TriggerWrapper.STATE_ERROR) {
            return TriggerState.ERROR;
        }
        return TriggerState.NORMAL;
	}

	@Override
	public void pauseTrigger(TriggerKey triggerKey)
			throws JobPersistenceException {
		final String triggerkey = ClusterCacheJobStore.generateKey(triggerKey);
		
		connector.doInTransaction(new TransactionScoped() {
			@Override
			public void doInTransaction() throws JobPersistenceException {
				connector.lockTrigger(triggerkey);
				TriggerWrapper tw = connector.getTriggerWrapper(triggerkey);
				
				if(tw != null && tw.state != TriggerWrapper.STATE_COMPLETE){
					
					if(tw.state == TriggerWrapper.STATE_BLOCKED){
						tw.state = TriggerWrapper.STATE_PAUSED_AND_BLOCKED;
					} else {
						tw.state = TriggerWrapper.STATE_PAUSED;
					}
					connector.putTriggerWrapper(triggerkey, tw);
					
				}
				//trigger does not exist or is already
				//complete there for pausing is useless
			}
		});
	}

	@Override
	public Collection<String> pauseTriggers(GroupMatcher<TriggerKey> matcher)
			throws JobPersistenceException {
		/*
		The JobStore should "remember" the groups paused, and impose the
	    pause on any new triggers that are added to one of these groups while the group is
	    paused.
		 */
		final Set<String> groupsPausedRet = new HashSet<String>();
		
		//Get the Triggers in the Group
		final Set<TriggerKey> triggersToPause = this.getTriggerKeys(matcher);
		if(triggersToPause.size() == 0){
			return new LinkedList<String>();
		}
		
		connector.doInTransaction(new TransactionScoped() {
			@Override
			public void doInTransaction() throws JobPersistenceException {
				connector.lockTrigger(generateKeySet(triggersToPause));
				connector.lockMetaData(METAKEY_PAUSED_TRIGGER_GROUPS_SET);

				for(TriggerKey triggerKey : triggersToPause){
					//remember groups that got paused
					groupsPausedRet.add(triggerKey.getGroup());
					pauseTrigger(triggerKey);
				}
			
				//Save Paused Groups as Metadata
				Set<String> pausedGroups = connector.getMetaData(METAKEY_PAUSED_TRIGGER_GROUPS_SET);
				if(pausedGroups != null){
					pausedGroups.addAll(groupsPausedRet);
					connector.putMetaData(METAKEY_PAUSED_TRIGGER_GROUPS_SET, pausedGroups);
				} else {
					connector.putMetaData(METAKEY_PAUSED_TRIGGER_GROUPS_SET, groupsPausedRet);
				}
			}
		});
		
		//return the newly paused groups
		List<String> returnList = new LinkedList<String>();
		returnList.addAll(groupsPausedRet);
		return returnList;
	}
	
	@Override
	public Set<String> getPausedTriggerGroups() throws JobPersistenceException {
		Set<String> pausedGroups = connector.getMetaData(METAKEY_PAUSED_TRIGGER_GROUPS_SET);
		if(pausedGroups != null){
			return pausedGroups;
		} else {
			return new HashSet<String>();
		}
	}

	@Override
	public void resumeTrigger(TriggerKey triggerKey)
			throws JobPersistenceException {
		final String triggerToResumeKey = ClusterCacheJobStore.generateKey(triggerKey);
		
		connector.doInTransaction(new TransactionScoped() {
			@Override
			public void doInTransaction() throws JobPersistenceException {
				
				connector.lockTrigger(triggerToResumeKey);
				//un-pause
				TriggerWrapper tw = connector.getTriggerWrapper(triggerToResumeKey);
				if(tw != null){
					if(	tw.state == TriggerWrapper.STATE_PAUSED ||
							tw.state == TriggerWrapper.STATE_PAUSED_AND_BLOCKED){
						//only paused trigger can be resumed
						if(tw.state == TriggerWrapper.STATE_PAUSED_AND_BLOCKED){
							tw.state = TriggerWrapper.STATE_BLOCKED;
						} else {
							// if not blocked:
							tw.state = TriggerWrapper.STATE_NORMAL;
						}
						connector.putTriggerWrapper(triggerToResumeKey, tw);
					}
				}
			}
		});
	}
	
	/**
	 * Checks if a trigger has misfired und sets Instructions
	 * @return true if a misfire insruction has been performed
	 * @throws JobPersistenceException 
	 */
	boolean applyMisfire(final TriggerWrapper twToCheck) throws JobPersistenceException{
		
		final String twKey = generateKey(twToCheck.trigger.getKey());
		class Bool{public boolean value;}
		final Bool retVal = new Bool();
		
		connector.doInTransaction(new TransactionScoped() {
			@Override
			public void doInTransaction() throws JobPersistenceException {
				retVal.value = aplMsfr();
			}
			
			private boolean aplMsfr() throws JobPersistenceException{
				
				connector.lockTrigger(twKey);
				TriggerWrapper tw = connector.getTriggerWrapper(twKey);
				
				long misfireTime = systemTime.currentTimeInMilliSeconds();
		        if (getMisfireThreshold() > 0) {
		            misfireTime -= getMisfireThreshold();
		        }
		        
		        Date tnft = tw.trigger.getNextFireTime();
		        if (tnft == null ||	tnft.getTime() > misfireTime 
		                || tw.trigger.getMisfireInstruction() == Trigger.MISFIRE_INSTRUCTION_IGNORE_MISFIRE_POLICY) { 
		        	//If no misfire instruction has to be performed
		        	return false; 
		        }
				//Trigger has misfired
		        
		        
		        Calendar cal = null;
		        if (tw.trigger.getCalendarName() != null) {
		        	try {
		        		cal = retrieveCalendar(tw.trigger.getCalendarName());
		        	}catch(JobPersistenceException e){
		        		LOGGER_RUNNING_CHANGE.error(e.getMessage());
		        		cal = null;
		        	}
		        }
		        
		        schedulerSignaler.notifyTriggerListenersMisfired(tw.trigger);
				tw.trigger.updateAfterMisfire(cal);
				connector.putTriggerWrapper(twKey, tw);
		    	
		        if(tw.trigger.getNextFireTime() == null){
							
		        	//Trigger will never fire again
		        	tw.state = TriggerWrapper.STATE_COMPLETE;
		    		//save the state
		    		connector.putTriggerWrapper(ClusterCacheJobStore.generateKey(tw.trigger.getKey()), tw);
						
		        	schedulerSignaler.notifySchedulerListenersFinalized(tw.trigger);
		        }
		        
		        LOGGER_RUNNING_CHANGE_MISFIRE.debug("Trigger misfired!: "+tw.trigger.getKey().getName()
		        			 +tw.trigger.getKey().getGroup()+" state: "+tw.state);
		        
		        return true;
				
			}
			
		});
		
		return retVal.value;
	}

	@Override
	public Collection<String> resumeTriggers(GroupMatcher<TriggerKey> matcher)
			throws JobPersistenceException {
		final Set<String> returnGroups = new HashSet<String>();

		final Set<TriggerKey> triggerKeysToResume = this.getTriggerKeys(matcher);
		
		if(triggerKeysToResume.size() == 0){
			return new LinkedList<String>();
		}
		
		final Set<String> triggerKeySet = this.generateKeySet(triggerKeysToResume);

		connector.doInTransaction(new TransactionScoped() {
			@Override
			public void doInTransaction() throws JobPersistenceException {

				connector.lockMetaData(METAKEY_PAUSED_TRIGGER_GROUPS_SET);
				connector.lockTrigger(triggerKeySet);
				
				for(TriggerKey triggerkey: triggerKeysToResume){
					returnGroups.add(triggerkey.getGroup());
					TriggerWrapper tw = connector.getTriggerWrapper(ClusterCacheJobStore.generateKey(triggerkey));
					if(!connector.containsMetaData(METAKEY_PAUSED_JOB_GROUPS_SET) || !connector.getMetaData(METAKEY_PAUSED_JOB_GROUPS_SET).contains(tw.trigger.getJobKey().getGroup())){
						//if the job group of the triggers job is not paused
						resumeTrigger(triggerkey);
					}
				}
				
				Set<String> pausedTriggerGroups = connector.getMetaData(METAKEY_PAUSED_TRIGGER_GROUPS_SET);
				for(String group : returnGroups){
					pausedTriggerGroups.remove(group);
				}
				connector.putMetaData(METAKEY_PAUSED_TRIGGER_GROUPS_SET, pausedTriggerGroups);
			}
		});
		
		LinkedList<String> returnGroupsList = new LinkedList<String>();
		returnGroupsList.addAll(returnGroups);
		return returnGroupsList;
	}
	
	@Override
	public void pauseJob(JobKey jobKey) throws JobPersistenceException {
		//either pause all triggers or non of them
		final List<OperableTrigger> triggersOfJob = getTriggersForJob(jobKey);
		final Set<String> triggersOfJobKeys = this.generateKeySet(triggersOfJob);
		
		connector.doInTransaction(new TransactionScoped() {
			@Override
			public void doInTransaction() throws JobPersistenceException {
				connector.lockTrigger(triggersOfJobKeys);
				
				for (OperableTrigger trigger: triggersOfJob) {
					pauseTrigger(trigger.getKey());
				}
			}
		});
	}
	
	@Override
	public void resumeJob(JobKey jobKey) throws JobPersistenceException {
		final List<OperableTrigger> triggersOfJob = getTriggersForJob(jobKey);
		final Set<String> triggersOfJobKeys = this.generateKeySet(triggersOfJob);
		
		connector.doInTransaction(new TransactionScoped() {
			@Override
			public void doInTransaction() throws JobPersistenceException {
				connector.lockTrigger(triggersOfJobKeys);
				for (OperableTrigger trigger: triggersOfJob) {
					resumeTrigger(trigger.getKey());
				}
			}
		});
	}

	@Override
	public Collection<String> pauseJobs(final GroupMatcher<JobKey> groupMatcher)
			throws JobPersistenceException {
		final Set<String> recentlyPausedJobGroups = new HashSet<String>();
		
		final Set<String> jobKeys = connector.getAllJobKeys();
		
		connector.doInTransaction(new TransactionScoped() {
			@Override
			public void doInTransaction() throws JobPersistenceException {
				connector.lockJob(jobKeys);
				connector.lockMetaData(METAKEY_PAUSED_JOB_GROUPS_SET);
				
				for(String jobKey : jobKeys){
					JobDetail job = connector.getJob(jobKey);
					if(groupMatcher.isMatch(job.getKey())){
						//job should be paused
						pauseJob(job.getKey());
						//remember paused groups
						recentlyPausedJobGroups.add(job.getKey().getGroup());
					}
				}
				
				Set<String> formerPausedJobGroups = connector.getMetaData(METAKEY_PAUSED_JOB_GROUPS_SET);
				if(formerPausedJobGroups == null){
					formerPausedJobGroups = recentlyPausedJobGroups;
				} else {
					formerPausedJobGroups.addAll(recentlyPausedJobGroups);
				}
				connector.putMetaData(METAKEY_PAUSED_JOB_GROUPS_SET, formerPausedJobGroups);
			}
		});
		return recentlyPausedJobGroups;
	}

	@Override
	public Collection<String> resumeJobs(final GroupMatcher<JobKey> matcher)
			throws JobPersistenceException {
		final Set<String> recentlyResumedGroups = new HashSet<String>();
		final Set<JobKey> jobKeys = this.getJobKeys(matcher);

		connector.doInTransaction(new TransactionScoped() {
			@Override
			public void doInTransaction() throws JobPersistenceException {
				connector.lockJob(generateKeySet(jobKeys));
				connector.lockMetaData(METAKEY_PAUSED_JOB_GROUPS_SET);
				
				//Resume all jobs
				for(JobKey jobKey : jobKeys){
					resumeJob(jobKey);
				}
				
				//Manage metadata remove paused groups
				Set<String> pausedGroups = connector.getMetaData(METAKEY_PAUSED_JOB_GROUPS_SET);
				
				for(String pausedGroup:connector.getMetaData(METAKEY_PAUSED_JOB_GROUPS_SET)){
					if(matcher.getCompareWithOperator().evaluate(pausedGroup, matcher.getCompareToValue())){
						recentlyResumedGroups.add(pausedGroup);
						pausedGroups.remove(pausedGroup);
					}
				}
				connector.putMetaData(METAKEY_PAUSED_JOB_GROUPS_SET, pausedGroups);
			}
		});
		
		return recentlyResumedGroups;
	}

	@Override
	public void pauseAll() throws JobPersistenceException {
		connector.doInTransaction(new TransactionScoped() {
			@Override
			public void doInTransaction() throws JobPersistenceException {
				List<String> names = getTriggerGroupNames();
				for (String name: names) {
					pauseTriggers(GroupMatcher.triggerGroupEquals(name));
				}
			}
		});
	}

	@Override
	public void resumeAll() throws JobPersistenceException {
		connector.doInTransaction(new TransactionScoped() {
			@Override
			public void doInTransaction() throws JobPersistenceException {
				connector.lockMetaData(METAKEY_PAUSED_JOB_GROUPS_SET);
				resumeTriggers(GroupMatcher.anyTriggerGroup());
				connector.removeMetaData(METAKEY_PAUSED_JOB_GROUPS_SET);
			}
		});
	}
	
	
	private static final AtomicLong fireInstanceTriggerCounter = new AtomicLong(System.currentTimeMillis());
	
	private TreeSet<OperableTrigger> acquireTrigger(final TriggerWrapper triggerWrapper, final TreeSet<OperableTrigger> triggerTree)
												 throws JobPersistenceException{
		final String triggerToAcquireKey = ClusterCacheJobStore.generateKey(triggerWrapper.trigger.getKey());
		class Bool{public boolean value;}
		final Bool triggerWrapperNoLongerAcquireable = new Bool();
		triggerWrapperNoLongerAcquireable.value = false;
		connector.doInTransaction(new TransactionScoped() {
			@Override
			public void doInTransaction() throws JobPersistenceException {
				connector.lockTrigger(triggerToAcquireKey);
				
				//setze FireInstanceId
				triggerWrapper.trigger.setFireInstanceId(String.valueOf(fireInstanceTriggerCounter.incrementAndGet()));
				TriggerWrapper tw = connector.getTriggerWrapper(triggerToAcquireKey);
				if(tw == null || tw.state != TriggerWrapper.STATE_NORMAL){
					//race condition trigger state needs to be checked again in lock
					triggerWrapperNoLongerAcquireable.value = true;
				} else {
					triggerWrapper.state = TriggerWrapper.STATE_ACQUIRED;
					triggerWrapper.fireingInstance = nodeId;
					
					//save the new State
					connector.putTriggerWrapper(triggerToAcquireKey, triggerWrapper);
				}
			}
		});
		
		if(!triggerWrapperNoLongerAcquireable.value){
			triggerTree.add(triggerWrapper.trigger);
		}
		return triggerTree;
	}
	
	@Override
	public List<OperableTrigger> acquireNextTriggers(long noLaterThan,
			int maxCount, long timeWindow) throws JobPersistenceException {
		
		TreeSet<OperableTrigger> nextTriggersThatFire = new TreeSet<OperableTrigger>(new TriggerTimeComparator());
		
		Set<String> triggerKeys = connector.getAllTriggerKeys();
		
		if(LOGGER_RUNNING_CHANGE.isDebugEnabled()){
			StringBuffer logmessage = new StringBuffer(); 
			logmessage.append("All the Trigger Keys in the JobStore:");
			for(String trigger:triggerKeys){
				logmessage.append("\n-"+trigger+" State: "+connector.getTriggerWrapper(trigger).getTriggerStateName());
			}
			LOGGER_RUNNING_CHANGE.debug(logmessage);
		}
		
		//remember if non Concurrent job will already be fired by other trigger
		Map<JobKey, OperableTrigger> concurrentExecutionDissallowedJobs = new HashMap<JobKey, OperableTrigger>();
		//Get all the triggers
		for(String triggerKey:triggerKeys){
			
			TriggerWrapper tw = connector.getTriggerWrapper(triggerKey);
			
			
			if(	tw != null &&
				//trigger not paused or acquired
				tw.state == TriggerWrapper.STATE_NORMAL &&
				//trigger will fire in the future
				tw.trigger.getNextFireTime() != null &&
				//trigger did not misfire
				!this.applyMisfire(tw) &&
				//trigger is within time limit
				(noLaterThan == 0 || (tw.trigger.getNextFireTime().getTime() < noLaterThan + timeWindow))
					){
				
				JobKey jobKey = tw.trigger.getJobKey();
				JobDetail job = connector.getJob(ClusterCacheJobStore.generateKey(jobKey));
				
				if(job.isConcurrentExecutionDisallowed()) {
					if(concurrentExecutionDissallowedJobs.containsKey(jobKey)){
						//Job is already referenced by fireing trigger
						//only save the trigger that will be fired next
						OperableTrigger thusFarPersistetJob = concurrentExecutionDissallowedJobs.get(jobKey);
						TriggerTimeComparator triggerComperator = new TriggerTimeComparator();
						
						if(triggerComperator.compare(thusFarPersistetJob, tw.trigger) > 0){
							//the current Trigger is fired earlyer than the thus far acquired trigger
							nextTriggersThatFire.remove(thusFarPersistetJob);
							this.releaseAcquiredTrigger(thusFarPersistetJob);
							nextTriggersThatFire = this.acquireTrigger(tw, nextTriggersThatFire);
							concurrentExecutionDissallowedJobs.put(jobKey, tw.trigger);
						}
						
					} else {
						//job is not referenced by fireing trigger
						concurrentExecutionDissallowedJobs.put(jobKey, tw.trigger);
						nextTriggersThatFire = this.acquireTrigger(tw, nextTriggersThatFire);
					}
					
				} else {
					//trigger can be added to resultset
					nextTriggersThatFire = this.acquireTrigger(tw, nextTriggersThatFire);
				}
			}
		}
		
		LinkedList<OperableTrigger> returnList = new LinkedList<OperableTrigger>();
		
		if(nextTriggersThatFire.size() > maxCount){
			for(int i = 0; i < maxCount; i++){
				returnList.addLast(nextTriggersThatFire.pollFirst());
			}
			for(OperableTrigger unacquire:nextTriggersThatFire){
				this.releaseAcquiredTrigger(unacquire);
			}
		} else {
			returnList.addAll(nextTriggersThatFire);
		}
		
		if(LOGGER_RUNNING_CHANGE.isDebugEnabled()){
			StringBuffer logmessage = new StringBuffer(); 
			logmessage.append("Following Triggers are now acquired by this node:");
			for(OperableTrigger trigger:nextTriggersThatFire){
				logmessage.append("\n- "+trigger.getKey().getName()+trigger.getKey().getGroup()+"\n"+connector.getTriggerWrapper(generateKey(trigger.getKey())).getTriggerInfo());
			}
			LOGGER_RUNNING_CHANGE.debug(logmessage);
		}
		
		return returnList;
	}

	private void removeBlockedJobMetaData(final String jobKey) throws JobPersistenceException{
		connector.doInTransaction(new TransactionScoped() {
			@Override
			public void doInTransaction() throws JobPersistenceException {
				connector.lockMetaData(METAKEY_BLOCKED_JOBS_SET);
				
				Set<String> blockedJobs = connector.getMetaData(METAKEY_BLOCKED_JOBS_SET);
				if(blockedJobs != null && blockedJobs.remove(jobKey)){
					connector.putMetaData(METAKEY_BLOCKED_JOBS_SET, blockedJobs);
				}
			}
		});
	}
	
	@Override
	public void releaseAcquiredTrigger(final OperableTrigger trigger) {
		
		LOGGER_RUNNING_CHANGE.debug("Trigger has been released: "+trigger.getKey().getName());
		
		//Retry releasing the Trigger until success
		this.retryInfinite(new Retry() {
			@Override
			public void tryOut() throws JobPersistenceException {
				final String triggerKey = ClusterCacheJobStore.generateKey(trigger.getKey());

				connector.doInTransaction(new TransactionScoped() {
					@Override
					public void doInTransaction() throws JobPersistenceException {
						connector.lockTrigger(triggerKey);
						TriggerWrapper tw = connector.getTriggerWrapper(triggerKey);
						if(tw != null && tw.state == TriggerWrapper.STATE_ACQUIRED){
							tw.state = TriggerWrapper.STATE_NORMAL;
							tw.fireingInstance = null;
							connector.putTriggerWrapper(triggerKey, tw);
						}
					}
				});
				LOGGER_RUNNING_CHANGE.debug("Trigger Released: "+triggerKey);
			}
		}, "release acquired Trigger");
	}
	
	protected void retryInfinite(Retry retryable, String retryRoutineDescription){
		for(int retrys = 1; !shutdown; retrys++){
			try {
				retryable.tryOut();
				return;
			} catch (JobPersistenceException e){
				if(retrys % 4 == 0) {
                    schedulerSignaler.notifySchedulerListenersError("An error occurred at retrying: "+retryRoutineDescription, e);
                }
			}
			try {
				Thread.sleep(this.getRetryTime()); // retry every N seconds
            } catch (InterruptedException e) {
                throw new IllegalStateException("Received interrupted exception", e);
            }
		}
	}
	
	@Override
	public List<TriggerFiredResult> triggersFired(final List<OperableTrigger> triggers)
			throws JobPersistenceException {
		
		if(LOGGER_RUNNING_CHANGE.isDebugEnabled()){
			StringBuffer logmessage = new StringBuffer();
			logmessage.append("Following Triggers fired are now fired by the Scheduler on this node:");
			
			for(OperableTrigger trigger:triggers){
				logmessage.append("\n-"+trigger.getKey().getName()+trigger.getKey().getGroup());
			}
			LOGGER_RUNNING_CHANGE.debug(logmessage);
		}
		
		final List<TriggerFiredResult> triggersFiredResult = new LinkedList<TriggerFiredResult>();

		connector.doInTransaction(new TransactionScoped() {
			@Override
			public void doInTransaction() throws JobPersistenceException {
				
				TreeSet<String> jobKeys = new TreeSet<String>();
				for(OperableTrigger trigger:triggers){
					//Lock jobs to prevent Deadlocks in triggers that fire the same Job 
					//at the same time while concurrent execution is disallowed
					jobKeys.add(ClusterCacheJobStore.generateKey(trigger.getJobKey()));
				}
				connector.lockJob(jobKeys);
				connector.lockTrigger(generateKeySet(triggers));
				
				for(OperableTrigger trigger:triggers){
					
					String triggerKey = ClusterCacheJobStore.generateKey(trigger.getKey());
					TriggerWrapper tw = connector.getTriggerWrapper(triggerKey);
					
					if(	tw != null && //Trigger was deleted since acquireing
							tw.state == TriggerWrapper.STATE_ACQUIRED //Trigger must be acquired
							){ 
						
						Calendar triggersCalendar = null;
						String calendarName = trigger.getCalendarName();
						if(calendarName != null){
							triggersCalendar = connector.getCalendar(calendarName);
							if(triggersCalendar == null){
								//calendar got deleted since the trigger got acquired
								continue;
							}
						}
						//Get the previous fire Time for return list before trigger triggers
						Date previousFireTime = trigger.getPreviousFireTime();
						//apply triggered for argument trigger and for cached trigger
						
						tw.trigger.triggered(triggersCalendar);
						
						JobDetail job  = retrieveJob(trigger.getJobKey());
						
						//prepare the retun list item
						TriggerFiredBundle triggerFiredInfo = new TriggerFiredBundle(
								(JobDetail) job.clone(),  								//job
								tw.trigger, 											//trigger 
								triggersCalendar, 										//calendar 
								tw.recovering, 											//jobIsRecovering -> job was started but got interrupted
								new Date(systemTime.currentTimeInMilliSeconds()), 	//fireTime
								trigger.getPreviousFireTime(), 							//scheduledFireTime 
								previousFireTime, 										//prevFireTime
								trigger.getNextFireTime());								//nextFireTime
						
						//reset trigger to normal state and persist
						tw.state = TriggerWrapper.STATE_NORMAL;
						tw.jobExecuting = true;
						tw.fireTime = triggerFiredInfo.getFireTime();
						tw.scheduleTime = triggerFiredInfo.getScheduledFireTime();
						tw.recovering = false;
						connector.putTriggerWrapper(triggerKey, tw);
						
						if(job != null && job.isConcurrentExecutionDisallowed()){
							List<TriggerWrapper> allTriggersThatFireTheJob = getTriggerWrappersForJob(job.getKey());
							
							connector.lockTrigger(generateKeySetFromWrapper(allTriggersThatFireTheJob));
							
							connector.lockMetaData(METAKEY_BLOCKED_JOBS_SET);
							
							for(TriggerWrapper triggerToBlock:allTriggersThatFireTheJob){
								//No trigger should start this Job again befor it has finished
								if(triggerToBlock.state == TriggerWrapper.STATE_PAUSED){
									triggerToBlock.state = TriggerWrapper.STATE_PAUSED_AND_BLOCKED;
								} else {
									triggerToBlock.state = TriggerWrapper.STATE_BLOCKED;
								}
								String triggerToBlockKey = ClusterCacheJobStore.generateKey(triggerToBlock.trigger.getKey());
								connector.putTriggerWrapper(triggerToBlockKey, triggerToBlock);
							}
							//remember the job as blocked
							Set<String> blockedJobs;
							
							if(connector.containsMetaData(METAKEY_BLOCKED_JOBS_SET)){
								blockedJobs = connector.getMetaData(METAKEY_BLOCKED_JOBS_SET);
							} else {
								blockedJobs = new HashSet<String>();
							}
							blockedJobs.add(ClusterCacheJobStore.generateKey(job.getKey()));
							connector.putMetaData(METAKEY_BLOCKED_JOBS_SET, blockedJobs);
							
						}
						triggersFiredResult.add(new TriggerFiredResult(triggerFiredInfo));
					}
				}
			}
		});

		return triggersFiredResult;
	}
	
	@Override
	public void triggeredJobComplete(final OperableTrigger completeTrigger,
			final JobDetail completeJobDetail, final CompletedExecutionInstruction triggerInstCode) {
		
		LOGGER_RUNNING_CHANGE.debug("The following Trigger has fired a job that is now complete: "
					 +completeTrigger.getKey().getName()+completeTrigger.getKey().getGroup()+
					 " Job: "+completeJobDetail.getKey().getName()+completeJobDetail.getKey().getGroup());
		
		this.retryInfinite(new Retry() {
			@Override
			public void tryOut() throws JobPersistenceException {
				connector.doInTransaction(new TransactionScoped() {
					@Override
					public void doInTransaction() throws JobPersistenceException {
						triggeredJobCompleteJobCompletion(completeJobDetail);
						triggeredJobCompleteTriggerCompletion(completeTrigger, triggerInstCode);
					}
				});
			}
		}, "triggered job complete");
		
	}
	
	private void triggeredJobCompleteJobCompletion(JobDetail completeJobDetail) throws JobPersistenceException{
		String jobKey = ClusterCacheJobStore.generateKey(completeJobDetail.getKey());
		connector.lockJob(jobKey);
		JobDetail storedJob = connector.getJob(jobKey);
		
		if(storedJob != null){
			
			if(storedJob.isPersistJobDataAfterExecution()){
				JobDataMap updatedData = completeJobDetail.getJobDataMap();
				
				if(updatedData != null){
					updatedData.clearDirtyFlag();
				}
				storedJob = storedJob.getJobBuilder().setJobData(updatedData).build();
				
				connector.putJob(jobKey, storedJob);
			}
			this.unblockTriggersForJob(storedJob, jobKey);
		} else {
			//clean up meta Data
			this.removeBlockedJobMetaData(jobKey);
		}
	}
	
	private void unblockTriggersForJob(JobDetail storedJob, String jobKey) throws JobPersistenceException{
		if(storedJob.isConcurrentExecutionDisallowed()){
			List<TriggerWrapper> triggersToUnblock = this.getTriggerWrappersForJob(storedJob.getKey());
			for(final TriggerWrapper triggerToUnblock: triggersToUnblock){
				
				connector.doInTransaction(new TransactionScoped() {
					@Override
					public void doInTransaction() throws JobPersistenceException {
						String triggerToUnblockKey = ClusterCacheJobStore.generateKey(triggerToUnblock.trigger.getKey());
						connector.lockTrigger(triggerToUnblockKey);
						if(triggerToUnblock.state == TriggerWrapper.STATE_PAUSED_AND_BLOCKED){
							triggerToUnblock.state = TriggerWrapper.STATE_PAUSED;
						} else {
							triggerToUnblock.state = TriggerWrapper.STATE_NORMAL;
						}
						connector.putTriggerWrapper(triggerToUnblockKey, triggerToUnblock);
					}
				});
			}
			this.removeBlockedJobMetaData(jobKey);
			
			this.schedulerSignaler.signalSchedulingChange(0L);
		}
	}
	
	private void triggeredJobCompleteTriggerCompletion(final OperableTrigger completeTrigger, 
			final CompletedExecutionInstruction triggerInstCode)
			throws JobPersistenceException{
		final String triggerKey = ClusterCacheJobStore.generateKey(completeTrigger.getKey());
		connector.doInTransaction(new TransactionScoped() {
			@Override
			public void doInTransaction() throws JobPersistenceException {
				connector.lockTrigger(triggerKey);
				
				TriggerWrapper storedTriggerWrapper = connector.getTriggerWrapper(triggerKey);
				
				if(storedTriggerWrapper != null){
					storedTriggerWrapper.state = TriggerWrapper.STATE_NORMAL;
					storedTriggerWrapper.fireingInstance = null;
					storedTriggerWrapper.jobExecuting = false;
					
					if(triggerInstCode == CompletedExecutionInstruction.DELETE_TRIGGER){
						if(completeTrigger.getNextFireTime() == null){
							//No next fire time scheduler does not need to be informed
							//we can't delete when trigger got rescheduled during jobexecution
							if(storedTriggerWrapper.trigger.getNextFireTime() == null){
								//the stored Time is also null -> no rescheduling has occured
								try { 
									removeTrigger(completeTrigger.getKey()); 
								} catch (JobPersistenceException jpe) {LOGGER_JOBSTORE_CHANGE.error("CompletedExecutionInstruction Delete Trigger failed: " + jpe.getMessage());}
							}
						} else {
							//next time is not null -> inform scheduler
							try { removeTrigger(completeTrigger.getKey());
							} catch (JobPersistenceException jpe) {LOGGER_JOBSTORE_CHANGE.error("CompletedExecutionInstruction Delete Trigger failed: " + jpe.getMessage());} 
							schedulerSignaler.signalSchedulingChange(0L);
						}
					} else if(triggerInstCode == CompletedExecutionInstruction.SET_TRIGGER_COMPLETE){
						storedTriggerWrapper.state = TriggerWrapper.STATE_COMPLETE;
						connector.putTriggerWrapper(triggerKey, storedTriggerWrapper);
						schedulerSignaler.signalSchedulingChange(0L);
					} else if(triggerInstCode == CompletedExecutionInstruction.SET_TRIGGER_ERROR){
						LOGGER_JOBSTORE_CHANGE.info("Trigger " + triggerKey + " set to ERROR state.");
						storedTriggerWrapper.state = TriggerWrapper.STATE_ERROR;
						connector.putTriggerWrapper(triggerKey, storedTriggerWrapper);
						schedulerSignaler.signalSchedulingChange(0L);
					} else if(triggerInstCode == CompletedExecutionInstruction.SET_ALL_JOB_TRIGGERS_ERROR){
						 LOGGER_JOBSTORE_CHANGE.info("All triggers of Job " + completeTrigger.getJobKey() + " set to ERROR state.");
						setAllTriggersOfJobToState(storedTriggerWrapper.trigger.getJobKey(), TriggerWrapper.STATE_ERROR);
						schedulerSignaler.signalSchedulingChange(0L);
					} else if(triggerInstCode == CompletedExecutionInstruction.SET_ALL_JOB_TRIGGERS_COMPLETE){
						setAllTriggersOfJobToState(storedTriggerWrapper.trigger.getJobKey(), TriggerWrapper.STATE_COMPLETE);
						schedulerSignaler.signalSchedulingChange(0L);
					} else {
						//persist no fireing instance
						connector.putTriggerWrapper(triggerKey, storedTriggerWrapper);
					}
				}
			}
		});
	}
	
	protected void setAllTriggersOfJobToState(final JobKey jobKey, final int state) throws JobPersistenceException{
		connector.doInTransaction(new TransactionScoped() {
			@Override
			public void doInTransaction() throws JobPersistenceException {
				List<TriggerWrapper> triggersForJob = getTriggerWrappersForJob(jobKey);
				for(TriggerWrapper triggerToSetState: triggersForJob){
					String triggerToSetStateKey = ClusterCacheJobStore.generateKey(triggerToSetState.trigger.getKey());
					connector.lockTrigger(triggerToSetStateKey);
					triggerToSetState.state = state;
					connector.putTriggerWrapper(triggerToSetStateKey, triggerToSetState);
				}
			}
		});
	}

	@Override
	public void setThreadPoolSize(int poolSize) {
		// nothing to do here
	}
	
	@Override
	public void schedulerStarted() throws SchedulerException {
		
		//create a clusterManagementListener that notifies the jobStore when a node of the Cluster got lost
		this.connector.createClusterRecoverListener(this);
		
		//Add node to metadata to keep track of the cluster
		Set<String> nodes = connector.getMetaData(METAKEY_NODE_ID_SET);
		if(nodes == null){
			nodes = new HashSet<String>();
			LOGGER_JOBSTORE_CHANGE.debug("New Nodeset created!");
		}
		nodes.add(this.nodeId);
		connector.putMetaData(METAKEY_NODE_ID_SET, nodes);
		LOGGER_JOBSTORE_CHANGE.debug("Node: "+this.nodeId+" added to MetaData");
		
		if(LOGGER_RECOVERY.isDebugEnabled()){
			StringBuffer logmessage = new StringBuffer();
			logmessage.append("Node name set got a new Member: "+nodeId+" Now the following nodes are known:");
			for(String s:nodes){
				logmessage.append("\n-"+s);
			}
			LOGGER_RECOVERY.debug(logmessage);
		}
	}
	
	@Override
	public void schedulerPaused() {
		LOGGER_JOBSTORE_CHANGE.info("Scheduler got paused");
	}
	
	@Override
	public void schedulerResumed() {
		LOGGER_JOBSTORE_CHANGE.info("Scheduler got resumed");
	}
	
	@Override
	public void shutdown() {
		this.shutdown = true;
	}

	@Override
	public long getEstimatedTimeToReleaseAndAcquireTrigger() {
		return 30;
	}

	@Override
	public boolean supportsPersistence() {
		//all JobStore data will be lost when all nodes are shutdown
		return false;
	}

	@Override
	public boolean isClustered() {
		return true;
	}
	
	@Override
	public int getNumberOfJobs() throws JobPersistenceException {
		return connector.getNumberOfJobs();
	}

	@Override
	public int getNumberOfTriggers() throws JobPersistenceException {
		return connector.getNumberOfTriggers();
	}

	@Override
	public int getNumberOfCalendars() throws JobPersistenceException {
		return connector.getNumberOfCalendars();
	}

	@Override
	public void storeJobAndTrigger(JobDetail newJob, OperableTrigger newTrigger)
			throws ObjectAlreadyExistsException, JobPersistenceException {
		
		this.storeJob(newJob, false);
		this.storeTrigger(newTrigger, false);

	}
	
	@Override
	public void setInstanceId(String schedInstId) {
		// Not needed
	}

	@Override
	public void setInstanceName(String schedName) {
		// Not needed
	}
	
	/**
	 * When a node leaves the cluster this method should be called by at least one of the remaining nodes.
	 * It will free the acquired Triggers of the node who left the cluster.
	 * It also will try to restart Jobs who require recovery.
	 * @param node the node who left the cluster
	 */
	public void recover(final String node){
		
		this.retryInfinite(new Retry() {
			@Override
			public void tryOut() throws JobPersistenceException {
				
				Set<String> nodeNameSet = connector.getMetaData(METAKEY_NODE_ID_SET);
				if(nodeNameSet.contains(node)){
					
					LOGGER_RECOVERY.debug("The missing Node: "+node+" has not yet been recovered, starting recovering now!");
					
					for(TriggerWrapper tw:connector.getAllTriggers()){ 
						
						LOGGER_RECOVERY.debug("Review the trigger: "+"\n"+ tw.getTriggerInfo());
												
						if(tw.fireingInstance != null && tw.fireingInstance.equals(node)){
							
							LOGGER_RECOVERY.debug("-Found a Trigger to recover: "+ClusterCacheJobStore.generateKey(tw.trigger.getKey()));
							
							//trigger was reserved by the missing node
							String jobKey = ClusterCacheJobStore.generateKey(tw.trigger.getJobKey());
							JobDetail job = connector.getJob(jobKey);
							
							if(tw.jobExecuting && job.requestsRecovery()){
								//job was interrupted and needs to be rescheduled
								final String triggerKey = ClusterCacheJobStore.generateKey(tw.trigger.getKey());

								//trigger schould only be recovert by one Node
								
								class Bool{public boolean value;}
								final Bool doContinue = new Bool();
								doContinue.value = false;
								class TriggerWrapperHolder {public TriggerWrapper trig;}
								final TriggerWrapperHolder tww = new TriggerWrapperHolder();
								tww.trig = tw;
								connector.doInTransaction(new TransactionScoped() {
									@Override
									public void doInTransaction() throws JobPersistenceException {
										connector.lockTrigger(triggerKey);
										tww.trig = connector.getTriggerWrapper(triggerKey);
										if(tww.trig.fireingInstance != null){
											tww.trig.fireingInstance = null;
											if(tww.trig.state == TriggerWrapper.STATE_ACQUIRED){
												//Trigger got re acquired while the job 
												//of the last firing was still running.
												tww.trig.state = TriggerWrapper.STATE_NORMAL;
											}
											connector.putTriggerWrapper(triggerKey, tww.trig);
										} else {
											//trigger got recovered by other node
											doContinue.value = true;
										}
									}
								});
								if(doContinue.value){
									continue;
								}
								tw = tww.trig;
								
                                JobDataMap jd = tw.trigger.getJobDataMap();
                                jd.put(Scheduler.FAILED_JOB_ORIGINAL_TRIGGER_NAME, tw.trigger.getKey().getName());
                                jd.put(Scheduler.FAILED_JOB_ORIGINAL_TRIGGER_GROUP, tw.trigger.getKey().getGroup());
                                jd.put(Scheduler.FAILED_JOB_ORIGINAL_TRIGGER_FIRETIME_IN_MILLISECONDS, String.valueOf(tw.fireTime));
                                jd.put(Scheduler.FAILED_JOB_ORIGINAL_TRIGGER_SCHEDULED_FIRETIME_IN_MILLISECONDS, String.valueOf(tw.scheduleTime));
                                
                                SimpleTrigger recoveryTrigger = newTrigger() 
		                        			.withIdentity("recover_" //Name
		                        						  +node+"_"
		                        						  +String.valueOf(new Date().getTime()),
		                        						  Scheduler.DEFAULT_RECOVERY_GROUP)
		                        			.forJob(job.getKey().getName(),
		                        					job.getKey().getGroup())
		                        			.withPriority(tw.trigger.getPriority())
		                        			.usingJobData(jd)
		                        			.startNow()
		                        			.withSchedule(simpleSchedule()
		                        					.withMisfireHandlingInstructionIgnoreMisfires()
		                        			)    
		                        			.build();
                                
                                OperableTrigger recOpTrigger = (OperableTrigger) recoveryTrigger; 
                                recOpTrigger.computeFirstFireTime(null);
                                
                                LOGGER_RECOVERY.debug("-Next Fire Time of the recovering Trigger: "+ recoveryTrigger.getNextFireTime());
                                try {
                                	storeTrigger(recOpTrigger, false, true, true);
                                } catch(JobPersistenceException jpe){
                                	LOGGER_RECOVERY.error("Trigger requested Recovery but could not be recovered! : "+triggerKey);
                                }
                                
                                //Triggers do not need to be unblocked because recoveryTrigger will unblock them after execution
                                
							} else {
								
								unblockTriggersForJob(job, jobKey);
								releaseAcquiredTrigger(tw.trigger);
							}
							
						}
					}
					
					//Delete the old node Name
					connector.doInTransaction(new TransactionScoped() {
						@Override
						public void doInTransaction() throws JobPersistenceException {
							connector.lockMetaData(METAKEY_NODE_ID_SET);
							Set<String> nodeNameSet = connector.getMetaData(METAKEY_NODE_ID_SET);
							nodeNameSet.remove(node);
							connector.putMetaData(METAKEY_NODE_ID_SET, nodeNameSet);
						}
					});
				} //else node has already recovered
			}
		}, "recover Trigger of node: "+node);
	}
	
	
	/**
	 * Generates the Stringrepresentation of multiple Keys
	 * @param keys 
	 * @return a (sorted) Set of the keys string representation.
	 * The sorted set should help minimize Deadlocks
	 */
	private Set<String> generateKeySet(Collection<? extends Key<?>> keys){ 
		Set<String> keySet = new TreeSet<String>();
		for(Key<?> k:keys){
			keySet.add(ClusterCacheJobStore.generateKey(k));
		}
		return keySet;
	}
	
	/**
	 * Generates the Stringrepresentation of multiple Triggers
	 * @param triggers 
	 * @return a (sorted) Set of the triggerkeys string representation.
	 * The sorted set should help minimize Deadlocks
	 */
	private Set<String> generateKeySet(List<OperableTrigger> triggers){ 
		Set<String> keySet = new TreeSet<String>();
		for(OperableTrigger trigger:triggers){
			keySet.add(ClusterCacheJobStore.generateKey(trigger.getKey()));
		}
		return keySet;
	}

	private Set<String> generateKeySetFromWrapper(List<TriggerWrapper> triggerWrapers){
		List<OperableTrigger> triggers = new LinkedList<OperableTrigger>();
		for(TriggerWrapper tw:triggerWrapers){
			triggers.add(tw.trigger);
		}
		return this.generateKeySet(triggers);
	}
	
	/**
	 * Generates a Key form a JobKey Object
	 * @param itemKey
	 * @return a textual key
	 */
	public static String generateKey(Key<?> itemKey){
		return itemKey.getGroup()+ itemKey.getName();
	}
	
	public ClusterCacheJobStore(){}//Unimplemented Constructor use initialize() instead
	
	public long getRetryTime() {
		return retryTime;
	}

	public void setRetryTime(long retryTime) {
		this.retryTime = retryTime;
	}
	
	public long getMisfireThreshold() {
		return misfireThreshold;
	}

	public void setMisfireThreshold(long misfireThreshold) {
		if(misfireThreshold < 1){
			throw new IllegalArgumentException("Misfire threshold must be higher than 0");
		}
		this.misfireThreshold = misfireThreshold;
	}

	///////////////////
	//Testing Support
	///////////////////
	/* The following Methods are necessary for unit tests
	 */
	
	public void setConnector(CacheConnector connector) {
		this.connector = connector;
	}
	
	public void setSystemTime(TimeOfSystem systemTime) {
		this.systemTime = systemTime;
	}
	
	public void setSchedulerSignaler(SchedulerSignaler schedulerSignaler) {
		this.schedulerSignaler = schedulerSignaler;
	}

	// New
	@Override
	public long getAcquireRetryDelay(int failureCount) {
		return 20L;
	}

	@Override
	public void resetTriggerFromErrorState(TriggerKey triggerKey) throws JobPersistenceException {
		synchronized (this.lock) {
			TriggerWrapper tw = this.connector.getTriggerWrapper(generateKey(triggerKey));
			if (tw != null && tw.state == TriggerWrapper.STATE_ERROR) {
				this.storeTrigger(tw.trigger, true, false, false);
			}
		}
	}

	protected final Object lock = new Object();
}

interface Retry{
	void tryOut() throws JobPersistenceException;
}



