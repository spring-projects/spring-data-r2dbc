/*
 * Copyright 2021 the original author or authors.
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

import static org.assertj.core.api.Assertions.*;

import io.r2dbc.spi.ConnectionFactory;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import reactor.test.StepVerifier;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import javax.sql.DataSource;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataAccessException;
import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.PersistenceConstructor;
import org.springframework.data.r2dbc.testing.R2dbcIntegrationTestSupport;
import org.springframework.data.relational.core.mapping.Column;
import org.springframework.data.relational.core.mapping.Table;
import org.springframework.data.repository.reactive.ReactiveCrudRepository;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.lang.Nullable;

/**
 * Abstract base class for integration tests for {@link LegoSetRepository} with table and column names that contain
 * upper and lower case characters.
 * 
 * @author Jens Schauder
 */
public abstract class AbstractR2dbcRepositoryWithMixedCaseNamesIntegrationTests extends R2dbcIntegrationTestSupport {

	@Autowired private LegoSetRepository repository;
	protected JdbcTemplate jdbc;

	@BeforeEach
	void before() {

		this.jdbc = createJdbcTemplate(createDataSource());

		try {
			this.jdbc.execute(getDropTableStatement());
		} catch (DataAccessException e) {}

		this.jdbc.execute(getCreateTableStatement());
	}

	/**
	 * Creates a {@link DataSource} to be used in this test.
	 *
	 * @return the {@link DataSource} to be used in this test.
	 */
	protected abstract DataSource createDataSource();

	/**
	 * Creates a {@link ConnectionFactory} to be used in this test.
	 *
	 * @return the {@link ConnectionFactory} to be used in this test.
	 */
	protected abstract ConnectionFactory createConnectionFactory();

	/**
	 * Returns the CREATE TABLE statement for table {@code legoset} with the following three columns:
	 * <ul>
	 * <li>id integer (primary key), not null, auto-increment</li>
	 * <li>name varchar(255), nullable</li>
	 * <li>manual integer, nullable</li>
	 * </ul>
	 *
	 * @return the CREATE TABLE statement for table {@code legoset} with three columns.
	 */
	protected abstract String getCreateTableStatement();

	/**
	 * Returns the the DROP TABLE statement for table {@code LegoSet}.
	 *
	 * @return the DROP TABLE statement for table {@code LegoSet}.
	 */
	protected abstract String getDropTableStatement();

	@Test
	void insertAndReadEntities() {

		LegoSet legoSet1 = new LegoSet(null, "SCHAUFELRADBAGGER", 12);
		LegoSet legoSet2 = new LegoSet(null, "FORSCHUNGSSCHIFF", 13);

		repository.saveAll(Arrays.asList(legoSet1, legoSet2)) //
				.as(StepVerifier::create) //
				.expectNextCount(2) //
				.verifyComplete();

		List<LegoSet> legoSets = repository //
				.findAll() //
				.collectList() //
				.block(Duration.ofMillis(500));

		assertThat(legoSets).containsExactlyInAnyOrder(legoSet1, legoSet2);
	}

	interface LegoSetRepository extends ReactiveCrudRepository<LegoSet, Integer> {}

	@Getter
	@Setter
	@Table("LegoSet")
	@NoArgsConstructor
	public static class LegoSet {

		@Nullable @Column("Id") @Id Integer id;

		@Column("Name") String name;

		@Column("Manual") Integer manual;

		@PersistenceConstructor
		LegoSet(@Nullable Integer id, String name, Integer manual) {
			this.id = id;
			this.name = name;
			this.manual = manual;
		}

		@Override
		public boolean equals(Object o) {
			if (this == o)
				return true;
			if (o == null || getClass() != o.getClass())
				return false;
			LegoSet legoSet = (LegoSet) o;
			return Objects.equals(id, legoSet.id) && Objects.equals(name, legoSet.name)
					&& Objects.equals(manual, legoSet.manual);
		}

		@Override
		public int hashCode() {
			return Objects.hash(id, name, manual);
		}
	}
}
