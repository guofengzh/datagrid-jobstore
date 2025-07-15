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

import java.util.Date;

import org.quartz.spi.OperableTrigger;

/**
 * @author Felix Finkbeiner
 */
public class TriggerWrapper implements java.io.Serializable {
	
	private static final long serialVersionUID = 9071374325494528178L;

	public final OperableTrigger trigger;
	
	public int state = TriggerWrapper.STATE_NORMAL;
	
	//States are needed because TriggerState does not 
	//contain all required states (e.g. acquired)
	public static final int STATE_NORMAL = 0;
	public static final int STATE_PAUSED = 1;
	//Trigger has no next fire Time -> complete
	public static final int STATE_COMPLETE = 2;
	public static final int STATE_ACQUIRED = 3;
	//Blocks when concurrent job execution is not allowed 
	//and a job instance is currently running
	public static final int STATE_BLOCKED = 4;
	public static final int STATE_PAUSED_AND_BLOCKED = 5;
	public static final int STATE_ERROR = 6;
	
	public TriggerWrapper (OperableTrigger trigger){
		this.trigger = trigger;
	}
	
	//following are needed for recovery
	//The node which acquired the trigger for execution
	public String fireingInstance = null;
	public boolean jobExecuting = false;
	public Date fireTime = null;
	public Date scheduleTime = null;
	//is the job recovering
	public boolean recovering = false;
	
	/**
	 * For debugging
	 * @return a human readable description of the TriggerWrapper
	 */
	public String getTriggerInfo(){
		return    "TriggerInfo: " + ClusterCacheJobStore.generateKey(trigger.getKey())
				+ "\nState: " + getTriggerStateName()
				+ "\nFireringInstance: " + (fireingInstance == null ? "null": fireingInstance)
				+ "\nJobExecuting: " + jobExecuting
				+ "\nFireTime: "+ (fireTime == null ? "null": fireTime.toString())
				+ "\nScheduleTime: "+ (scheduleTime == null ? "null": scheduleTime.toString())
				+ "\nRecovering: "+ recovering;
	}
	
	/**
	 * For debugging 
	 * @return the state of the TriggerWrapper in a human readable string
	 */
	public String getTriggerStateName(){
		switch(state){
		case STATE_ACQUIRED: return "Acquired";
		case STATE_BLOCKED:  return "Blocked";
		case STATE_COMPLETE: return "Complete";
		case STATE_ERROR: 	 return "Error";
		case STATE_NORMAL:   return "Normal";
		case STATE_PAUSED:   return "Paused";
		case STATE_PAUSED_AND_BLOCKED: return "Paused and Blocked";
		default: return "unknwonw Trigger-state";
		}
	}
}
