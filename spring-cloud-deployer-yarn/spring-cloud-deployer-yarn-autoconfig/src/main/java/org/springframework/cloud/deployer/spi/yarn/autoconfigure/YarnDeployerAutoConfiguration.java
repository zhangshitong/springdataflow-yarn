/*
 * Copyright 2016 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.cloud.deployer.spi.yarn.autoconfigure;

import java.util.HashMap;
import java.util.Map;

import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.AutoConfigureOrder;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.cloud.deployer.resource.maven.MavenResourceLoader;
import org.springframework.cloud.deployer.resource.support.DelegatingResourceLoader;
import org.springframework.cloud.deployer.spi.app.AppDeployer;
import org.springframework.cloud.deployer.spi.task.TaskLauncher;
import org.springframework.cloud.deployer.spi.yarn.AppDeployerStateMachine;
import org.springframework.cloud.deployer.spi.yarn.DefaultYarnCloudAppService;
import org.springframework.cloud.deployer.spi.yarn.TaskLauncherStateMachine;
import org.springframework.cloud.deployer.spi.yarn.YarnAppDeployer;
import org.springframework.cloud.deployer.spi.yarn.YarnCloudAppService;
import org.springframework.cloud.deployer.spi.yarn.YarnDeployerProperties;
import org.springframework.cloud.deployer.spi.yarn.YarnTaskLauncher;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.Ordered;
import org.springframework.core.io.DefaultResourceLoader;
import org.springframework.core.io.ResourceLoader;
import org.springframework.core.task.TaskExecutor;
import org.springframework.data.hadoop.fs.HdfsResourceLoader;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

/**
 * {@link EnableAutoConfiguration Auto-configuration} for dataflow
 * YARN deployer.
 *
 * @author Janne Valkealahti
 *
 */
@Configuration
@AutoConfigureOrder(Ordered.HIGHEST_PRECEDENCE)
@ConditionalOnClass({ AppDeployer.class, TaskLauncher.class })
@EnableConfigurationProperties({ YarnDeployerProperties.class })
public class YarnDeployerAutoConfiguration {

	@Value("${spring.cloud.deployer.yarn.version:}")
	private String deployerVersion;

	@Autowired(required = false)
	private MavenResourceLoader mavenResourceLoader;

	@Bean
	public DelegatingResourceLoader delegatingResourceLoader(org.apache.hadoop.conf.Configuration configuration) {
		DefaultResourceLoader defaultLoader = new DefaultResourceLoader();
		Map<String, ResourceLoader> loaders = new HashMap<>();
		loaders.put("hdfs", new HdfsResourceLoader(configuration));
		if (mavenResourceLoader != null) {
			loaders.put("maven", mavenResourceLoader);
		}
		loaders.put("file", defaultLoader);
		loaders.put("http", defaultLoader);
		return new DelegatingResourceLoader(loaders);
	}

	@Bean
	public YarnCloudAppService yarnCloudAppService() {
		return new DefaultYarnCloudAppService(deployerVersion);
	}

	@Bean
	public TaskExecutor yarnModuleDeployerTaskExecutor() {
		ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
		executor.setCorePoolSize(1);
		return executor;
	}

	@Configuration
	@ConditionalOnMissingBean(name = "appDeployer")
	public static class ProcessModuleDeployerConfig {

		@Bean
		public AppDeployerStateMachine appDeployerStateMachine(YarnCloudAppService yarnCloudAppService,
				TaskExecutor yarnModuleDeployerTaskExecutor, BeanFactory beanFactory, ApplicationContext applicationContext) throws Exception {
			return new AppDeployerStateMachine(yarnCloudAppService, yarnModuleDeployerTaskExecutor, beanFactory, applicationContext);
		}

		@Bean
		public AppDeployer appDeployer(YarnCloudAppService yarnCloudAppService,
				AppDeployerStateMachine appDeployerStateMachine) throws Exception {
			return new YarnAppDeployer(yarnCloudAppService, appDeployerStateMachine.buildStateMachine());
		}
	}

	@Configuration
	@ConditionalOnMissingBean(name = "taskLauncher")
	public static class TaskModuleDeployerConfig {

		@Bean
		public TaskLauncherStateMachine taskLauncherStateMachine(YarnCloudAppService yarnCloudAppService,
				TaskExecutor yarnModuleDeployerTaskExecutor, BeanFactory beanFactory, ApplicationContext applicationContext) throws Exception {
			return new TaskLauncherStateMachine(yarnCloudAppService, yarnModuleDeployerTaskExecutor, beanFactory, applicationContext);
		}

		@Bean
		public TaskLauncher taskLauncher(YarnCloudAppService yarnCloudAppService,
				TaskLauncherStateMachine taskLauncherStateMachine) throws Exception {
			return new YarnTaskLauncher(yarnCloudAppService, taskLauncherStateMachine.buildStateMachine());
		}
	}
}
