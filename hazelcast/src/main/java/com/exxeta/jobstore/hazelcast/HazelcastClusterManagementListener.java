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

import org.jboss.logging.Logger;

import com.exxeta.jobstore.ClusterCacheJobStore;
import com.hazelcast.core.MemberAttributeEvent;
import com.hazelcast.core.MembershipEvent;
import com.hazelcast.core.MembershipListener;

/**
 * @author Felix Finkbeiner
 */
public class HazelcastClusterManagementListener implements MembershipListener {
	
	private static final Logger LOGGER = Logger.getLogger("com.exxeta.jobstore.ClusterCacheJobStore.Recovery");
	
	private ClusterCacheJobStore js = null;
	
	public HazelcastClusterManagementListener(ClusterCacheJobStore js){
		this.js = js;
	}

	@Override
	public void memberRemoved(MembershipEvent membershipEvent) {
		LOGGER.debug("Recovering Node Adress: "+membershipEvent.getMember().getSocketAddress());
		js.recover(membershipEvent.getMember().getSocketAddress().toString());
	}

	@Override
	public void memberAdded(MembershipEvent membershipEvent) {}

	@Override
	public void memberAttributeChanged(MemberAttributeEvent memberAttributeEvent) {}

}
