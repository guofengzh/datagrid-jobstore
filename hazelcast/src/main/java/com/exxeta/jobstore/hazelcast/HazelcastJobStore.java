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

import java.io.FileNotFoundException;

import com.exxeta.jobstore.ClusterCacheJobStore;
import com.hazelcast.config.Config;
import com.hazelcast.config.XmlConfigBuilder;
import org.quartz.SchedulerConfigException;

/**
 * @author Felix Finkbeiner
 */
public class HazelcastJobStore extends ClusterCacheJobStore {
	
	private String hazelcastConfigurationXML = null;
	
	@Override
	protected void createCacheConnector() throws SchedulerConfigException {
		Config cfg = null;
		if(hazelcastConfigurationXML == null){
			cfg = new Config();
		} else {
			try{
				cfg = new XmlConfigBuilder(hazelcastConfigurationXML).build();
			}catch(FileNotFoundException fnfe){
				throw new SchedulerConfigException("Hazelcast configuration file could not be found");
			}
		}
		
		this.connector = new HazelcastCacheConnector(cfg);
		
	}
	
	HazelcastCacheConnector getConnector(){
		return (HazelcastCacheConnector) connector;
	}

	public void setHazelcastConfigurationXML(String hazelcastConfigurationXML) {
		this.hazelcastConfigurationXML = hazelcastConfigurationXML;
	}

	

}
