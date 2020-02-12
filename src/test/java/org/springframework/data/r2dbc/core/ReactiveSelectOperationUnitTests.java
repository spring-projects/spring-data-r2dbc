/*
 * Copyright 2020 the original author or authors.
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
package org.springframework.data.r2dbc.core;

import static org.assertj.core.api.Assertions.*;
import static org.springframework.data.r2dbc.query.Criteria.*;
import static org.springframework.data.r2dbc.query.Query.*;

import io.r2dbc.spi.test.MockColumnMetadata;
import io.r2dbc.spi.test.MockResult;
import io.r2dbc.spi.test.MockRow;
import io.r2dbc.spi.test.MockRowMetadata;
import reactor.test.StepVerifier;

import org.junit.Before;
import org.junit.Test;

import org.springframework.data.annotation.Id;
import org.springframework.data.r2dbc.dialect.PostgresDialect;
import org.springframework.data.r2dbc.testing.StatementRecorder;
import org.springframework.data.relational.core.mapping.Column;

/**
 * Unit test for {@link ReactiveSelectOperation}.
 *
 * @author Mark Paluch
 */
public class ReactiveSelectOperationUnitTests {

	DatabaseClient client;
	R2dbcEntityTemplate entityTemplate;
	StatementRecorder recorder;

	@Before
	public void before() {

		recorder = StatementRecorder.newInstance();
		client = DatabaseClient.builder().connectionFactory(recorder)
				.dataAccessStrategy(new DefaultReactiveDataAccessStrategy(PostgresDialect.INSTANCE)).build();
		entityTemplate = new R2dbcEntityTemplate(client);
	}

	@Test // gh-220
	public void shouldSelectAll() {

		MockRowMetadata metadata = MockRowMetadata.builder().columnMetadata(MockColumnMetadata.builder().name("id").build())
				.build();
		MockResult result = MockResult.builder().rowMetadata(metadata)
				.row(MockRow.builder().identified("id", Object.class, "Walter").build()).build();

		recorder.addStubbing(s -> s.startsWith("SELECT"), result);

		entityTemplate.select(Person.class) //
				.matching(query(where("name").is("Walter")).limit(10).offset(20)) //
				.all() //
				.as(StepVerifier::create) //
				.expectNextCount(1) //
				.verifyComplete();

		StatementRecorder.RecordedStatement statement = recorder.getCreatedStatement(s -> s.startsWith("SELECT"));

		assertThat(statement.getSql())
				.isEqualTo("SELECT person.* FROM person WHERE person.THE_NAME = $1 LIMIT 10 OFFSET 20");
	}

	@Test // gh-220
	public void shouldSelectAs() {

		MockRowMetadata metadata = MockRowMetadata.builder().columnMetadata(MockColumnMetadata.builder().name("id").build())
				.build();
		MockResult result = MockResult.builder().rowMetadata(metadata)
				.row(MockRow.builder().identified("id", Object.class, "Walter").build()).build();

		recorder.addStubbing(s -> s.startsWith("SELECT"), result);

		entityTemplate.select(Person.class) //
				.as(PersonProjection.class) //
				.matching(query(where("name").is("Walter"))) //
				.all() //
				.as(StepVerifier::create) //
				.expectNextCount(1) //
				.verifyComplete();

		StatementRecorder.RecordedStatement statement = recorder.getCreatedStatement(s -> s.startsWith("SELECT"));

		assertThat(statement.getSql()).isEqualTo("SELECT person.THE_NAME FROM person WHERE person.THE_NAME = $1");
	}

	@Test // gh-220
	public void shouldSelectFromTable() {

		MockRowMetadata metadata = MockRowMetadata.builder().columnMetadata(MockColumnMetadata.builder().name("id").build())
				.build();
		MockResult result = MockResult.builder().rowMetadata(metadata)
				.row(MockRow.builder().identified("id", Object.class, "Walter").build()).build();

		recorder.addStubbing(s -> s.startsWith("SELECT"), result);

		entityTemplate.select(Person.class) //
				.from("the_table") //
				.matching(query(where("name").is("Walter"))) //
				.all() //
				.as(StepVerifier::create) //
				.expectNextCount(1) //
				.verifyComplete();

		StatementRecorder.RecordedStatement statement = recorder.getCreatedStatement(s -> s.startsWith("SELECT"));

		assertThat(statement.getSql()).isEqualTo("SELECT the_table.* FROM the_table WHERE the_table.THE_NAME = $1");
	}

	@Test // gh-220
	public void shouldSelectFirst() {

		MockRowMetadata metadata = MockRowMetadata.builder().columnMetadata(MockColumnMetadata.builder().name("id").build())
				.build();
		MockResult result = MockResult.builder().rowMetadata(metadata)
				.row(MockRow.builder().identified("id", Object.class, "Walter").build()).build();

		recorder.addStubbing(s -> s.startsWith("SELECT"), result);

		entityTemplate.select(Person.class) //
				.matching(query(where("name").is("Walter"))) //
				.first() //
				.as(StepVerifier::create) //
				.expectNextCount(1) //
				.verifyComplete();

		StatementRecorder.RecordedStatement statement = recorder.getCreatedStatement(s -> s.startsWith("SELECT"));

		assertThat(statement.getSql()).isEqualTo("SELECT person.* FROM person WHERE person.THE_NAME = $1 LIMIT 1");
	}

	@Test // gh-220
	public void shouldSelectOne() {

		MockRowMetadata metadata = MockRowMetadata.builder().columnMetadata(MockColumnMetadata.builder().name("id").build())
				.build();
		MockResult result = MockResult.builder().rowMetadata(metadata)
				.row(MockRow.builder().identified("id", Object.class, "Walter").build()).build();

		recorder.addStubbing(s -> s.startsWith("SELECT"), result);

		entityTemplate.select(Person.class) //
				.matching(query(where("name").is("Walter"))) //
				.one() //
				.as(StepVerifier::create) //
				.expectNextCount(1) //
				.verifyComplete();

		StatementRecorder.RecordedStatement statement = recorder.getCreatedStatement(s -> s.startsWith("SELECT"));

		assertThat(statement.getSql()).isEqualTo("SELECT person.* FROM person WHERE person.THE_NAME = $1 LIMIT 2");
	}

	@Test // gh-220
	public void shouldSelectExists() {

		MockRowMetadata metadata = MockRowMetadata.builder().columnMetadata(MockColumnMetadata.builder().name("id").build())
				.build();
		MockResult result = MockResult.builder().rowMetadata(metadata)
				.row(MockRow.builder().identified("id", Object.class, "Walter").build()).build();

		recorder.addStubbing(s -> s.startsWith("SELECT"), result);

		entityTemplate.select(Person.class) //
				.matching(query(where("name").is("Walter"))) //
				.exists() //
				.as(StepVerifier::create) //
				.expectNext(true) //
				.verifyComplete();

		StatementRecorder.RecordedStatement statement = recorder.getCreatedStatement(s -> s.startsWith("SELECT"));

		assertThat(statement.getSql()).isEqualTo("SELECT person.id FROM person WHERE person.THE_NAME = $1");
	}

	@Test // gh-220
	public void shouldSelectCount() {

		MockRowMetadata metadata = MockRowMetadata.builder().columnMetadata(MockColumnMetadata.builder().name("id").build())
				.build();
		MockResult result = MockResult.builder().rowMetadata(metadata)
				.row(MockRow.builder().identified(0, Long.class, 1L).build()).build();

		recorder.addStubbing(s -> s.startsWith("SELECT"), result);

		entityTemplate.select(Person.class) //
				.matching(query(where("name").is("Walter"))) //
				.count() //
				.as(StepVerifier::create) //
				.expectNext(1L) //
				.verifyComplete();

		StatementRecorder.RecordedStatement statement = recorder.getCreatedStatement(s -> s.startsWith("SELECT"));

		assertThat(statement.getSql()).isEqualTo("SELECT COUNT(person.id) FROM person WHERE person.THE_NAME = $1");
	}

	static class Person {

		@Id String id;

		@Column("THE_NAME") String name;

		public String getName() {
			return name;
		}

		public void setName(String name) {
			this.name = name;
		}
	}

	interface PersonProjection {

		String getName();
	}
}
