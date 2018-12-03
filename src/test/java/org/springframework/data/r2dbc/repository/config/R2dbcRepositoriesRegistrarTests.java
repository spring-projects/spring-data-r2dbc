/*
 * Copyright 2018 the original author or authors.
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
package org.springframework.data.r2dbc.repository.config;

import static org.mockito.Mockito.*;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.r2dbc.function.DatabaseClient;
import org.springframework.data.r2dbc.function.ReactiveDataAccessStrategy;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;

/**
 * Integration tests for {@link R2dbcRepositoriesRegistrar}.
 *
 * @author Mark Paluch
 */
@RunWith(SpringRunner.class)
@ContextConfiguration
public class R2dbcRepositoriesRegistrarTests {

	@Configuration
	@EnableR2dbcRepositories(basePackages = "org.springframework.data.r2dbc.repository.config")
	static class Config {

		@Bean
		public DatabaseClient databaseClient() {
			return mock(DatabaseClient.class);
		}

		@Bean
		public ReactiveDataAccessStrategy reactiveDataAccessStrategy() {
			return mock(ReactiveDataAccessStrategy.class);
		}
	}

	@Autowired PersonRepository personRepository;
	@Autowired ApplicationContext context;

	@Test // gh-13
	public void testConfiguration() {}
}
