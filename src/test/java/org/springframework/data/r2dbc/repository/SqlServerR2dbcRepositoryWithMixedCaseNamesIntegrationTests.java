/*
 * Copyright 2019-2021 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.r2dbc.repository;

import io.r2dbc.spi.ConnectionFactory;

import java.util.Optional;

import javax.sql.DataSource;

import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan.Filter;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.FilterType;
import org.springframework.data.r2dbc.config.AbstractR2dbcConfiguration;
import org.springframework.data.r2dbc.convert.R2dbcCustomConversions;
import org.springframework.data.r2dbc.mapping.R2dbcMappingContext;
import org.springframework.data.r2dbc.repository.config.EnableR2dbcRepositories;
import org.springframework.data.r2dbc.repository.support.R2dbcRepositoryFactory;
import org.springframework.data.r2dbc.testing.ExternalDatabase;
import org.springframework.data.r2dbc.testing.SqlServerTestSupport;
import org.springframework.data.relational.core.mapping.NamingStrategy;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringExtension;

/**
 * Integration tests for {@link LegoSetRepository} with table and column names that contain
 *  * upper and lower case characters against SQL-Server.
 *
 * @author Jens Schauder
 */
@ExtendWith(SpringExtension.class)
@ContextConfiguration
public class SqlServerR2dbcRepositoryWithMixedCaseNamesIntegrationTests
		extends AbstractR2dbcRepositoryWithMixedCaseNamesIntegrationTests {

	@RegisterExtension public static final ExternalDatabase database = SqlServerTestSupport.database();

	@Configuration
	@EnableR2dbcRepositories(considerNestedRepositories = true,
			includeFilters = @Filter(classes = { LegoSetRepository.class }, type = FilterType.ASSIGNABLE_TYPE))
	static class IntegrationTestConfiguration extends AbstractR2dbcConfiguration {

		@Bean
		@Override
		public ConnectionFactory connectionFactory() {
			return SqlServerTestSupport.createConnectionFactory(database);
		}

		@Override
		public R2dbcMappingContext r2dbcMappingContext(Optional<NamingStrategy> namingStrategy,
				R2dbcCustomConversions r2dbcCustomConversions) {

			R2dbcMappingContext r2dbcMappingContext = super.r2dbcMappingContext(namingStrategy, r2dbcCustomConversions);
			r2dbcMappingContext.setForceQuote(true);

			return r2dbcMappingContext;
		}
	}

	@Override
	protected DataSource createDataSource() {
		return SqlServerTestSupport.createDataSource(database);
	}

	@Override
	protected ConnectionFactory createConnectionFactory() {
		return SqlServerTestSupport.createConnectionFactory(database);
	}

	@Override
	protected String getCreateTableStatement() {
		return SqlServerTestSupport.CREATE_TABLE_LEGOSET_WITH_MIXED_CASE_NAMES;
	}

	@Override
	protected String getDropTableStatement() {
		return SqlServerTestSupport.DROP_TABLE_LEGOSET_WITH_MIXED_CASE_NAMES;
	}
}
