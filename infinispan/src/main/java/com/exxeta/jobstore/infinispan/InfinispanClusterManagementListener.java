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

import java.util.List;

import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachemanagerlistener.annotation.ViewChanged;
import org.infinispan.notifications.cachemanagerlistener.event.ViewChangedEvent;
import org.infinispan.remoting.transport.Address;
import org.jboss.logging.Logger;

/**
 * This listener reacts to the loss of a node in a Infinispan cluster
 * @author Felix Finkbeiner
 */
@Listener
public class InfinispanClusterManagementListener {
	
	private static final Logger LOGGER = Logger.getLogger("com.exxeta.jobstore.ClusterCacheJobStore.Recovery");

	
	InfinispanJobStore jobStore;
	
	public InfinispanClusterManagementListener(InfinispanJobStore jobStore) {
		this.jobStore = jobStore;
	}
	
	@ViewChanged
	public void checkForRecovery(ViewChangedEvent event){
		List<Address> oldMembersList = event.getOldMembers();
		List<Address> newMembersList = event.getNewMembers();
		
		if(newMembersList.size() < oldMembersList.size()){
			//We lost at least one node so we need recovery
			for(Address nodeAdress:oldMembersList){
				if(!newMembersList.contains(nodeAdress)){
					//The node we lost
					LOGGER.info("Recover Node Address: "+ nodeAdress.toString());
					String nodeName = jobStore.GetNodeNameFromAdressToRecover(nodeAdress.toString());
					if(nodeName != null){ 
						LOGGER.debug("Recover Node Name: "+ nodeName);
						jobStore.recover(nodeName);
					}//else node already got recovered
				}
			}
		}
	}
}
