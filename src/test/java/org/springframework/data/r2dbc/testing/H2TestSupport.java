/*
 * Copyright 2019-2020 the original author or authors.
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
package org.springframework.data.r2dbc.testing;

import io.r2dbc.h2.H2ConnectionConfiguration;
import io.r2dbc.h2.H2ConnectionFactory;
import io.r2dbc.spi.ConnectionFactory;

import javax.sql.DataSource;

import org.springframework.jdbc.datasource.DriverManagerDataSource;

/**
 * Utility class for testing against H2.
 *
 * @author Mark Paluch
 * @author Bogdan Ilchyshyn
 */
public class H2TestSupport {

	public static String CREATE_TABLE_LEGOSET = "CREATE TABLE legoset (\n" //
			+ "    id          integer CONSTRAINT id PRIMARY KEY,\n" //
			+ "    version     integer NULL,\n" //
			+ "    name        varchar(255) NOT NULL,\n" //
			+ "    manual      integer NULL\n," //
			+ "    cert        bytea NULL\n" //
			+ ");";

	public static String CREATE_TABLE_LEGOSET_WITH_ID_GENERATION = "CREATE TABLE legoset (\n" //
			+ "    id          serial CONSTRAINT id PRIMARY KEY,\n" //
			+ "    version     integer NULL,\n" //
			+ "    name        varchar(255) NOT NULL,\n" //
			+ "    manual      integer NULL\n" //
			+ ");";

	/**
	 * Creates a new {@link ConnectionFactory}.
	 */
	public static ConnectionFactory createConnectionFactory() {

		return new H2ConnectionFactory(H2ConnectionConfiguration.builder() //
				.inMemory("r2dbc") //
				.username("sa") //
				.password("") //
				.option("DB_CLOSE_DELAY=-1").build());
	}

	/**
	 * Creates a new {@link DataSource}.
	 */
	public static DataSource createDataSource() {

		DriverManagerDataSource dataSource = new DriverManagerDataSource();

		dataSource.setUsername("sa");
		dataSource.setPassword("");
		dataSource.setUrl("jdbc:h2:mem:r2dbc;DB_CLOSE_DELAY=-1");

		return dataSource;
	}

}
