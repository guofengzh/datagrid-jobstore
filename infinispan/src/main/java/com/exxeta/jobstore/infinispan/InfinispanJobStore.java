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

import java.util.Set;

import org.jboss.logging.Logger;
import org.quartz.SchedulerConfigException;

import com.exxeta.jobstore.ClusterCacheJobStore;

/**
 * @author Felix Finkbeiner
 */
public class InfinispanJobStore extends ClusterCacheJobStore {

	public String infinispanJNDI = null;
	public String infinispanJobStoreCacheJobsByKey = null;
	public String infinispanJobStoreCacheTriggersByKey = null;
	public String infinispanJobStoreCacheCalendarsByName = null;
	public String infinispanJobStoreCacheMetaData = null;
	
	private static final Logger LOGGER_RECOVERY = Logger.getLogger("com.exxeta.jobstore.ClusterCacheJobStore.Recovery");
	
	@Override
	protected void createCacheConnector() throws SchedulerConfigException{
		
		this.connector = new InfinispanCacheConnectorWithLocking(	infinispanJNDI, 
																	infinispanJobStoreCacheJobsByKey, 
																	infinispanJobStoreCacheTriggersByKey, 
																	infinispanJobStoreCacheCalendarsByName, 
																	infinispanJobStoreCacheMetaData);
		
	}
	
	/**
	 * Searches the metadata for the Node Name of the given Address
	 * @param nodeAdress the String representation of the Address
	 * @return the Node name or Null if the node was not found
	 */
	String GetNodeNameFromAdressToRecover(String nodeAdress){ 
		if(nodeAdress == null || nodeAdress.equals("")){
			return null;
		}
		
		Set<String> nodeNameSet = connector.getMetaData(METAKEY_NODE_ID_SET);
		
		if(LOGGER_RECOVERY.isDebugEnabled()){
			if(nodeNameSet == null){
				LOGGER_RECOVERY.debug("GetNodeNameFromAdressToRecover NodeNameSet is Null!");
			} else {
				for(String s:nodeNameSet){
					LOGGER_RECOVERY.debug("--GetNodeNameFromAdressToRecover Nodes in Set: "+s);
				}
			}
		}
		
		
		for(String nodeName:nodeNameSet){
			if(nodeAdress.startsWith(nodeName)){
				return nodeName;
			}
		}
		
		return null;
	}
	
	public void setInfinispanJNDI(String infinispanJNDI) {
		this.infinispanJNDI = infinispanJNDI;
	}
	
	public void setInfinispanJobStoreCacheJobsByKey(
			String infinispanJobStoreCacheJobsByKey) {
		this.infinispanJobStoreCacheJobsByKey = infinispanJobStoreCacheJobsByKey;
	}

	public void setInfinispanJobStoreCacheTriggersByKey(
			String infinispanJobStoreCacheTriggersByKey) {
		this.infinispanJobStoreCacheTriggersByKey = infinispanJobStoreCacheTriggersByKey;
	}

	public void setInfinispanJobStoreCacheCalendarsByName(
			String infinispanJobStoreCacheCalendarsByName) {
		this.infinispanJobStoreCacheCalendarsByName = infinispanJobStoreCacheCalendarsByName;
	}

	public void setInfinispanJobStoreCacheMetaData(
			String infinispanJobStoreCacheMetaData) {
		this.infinispanJobStoreCacheMetaData = infinispanJobStoreCacheMetaData;
	}

}
