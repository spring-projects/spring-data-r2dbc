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
package org.springframework.data.r2dbc.repository.query;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

import kotlin.Unit;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.lang.reflect.Method;
import java.util.function.Supplier;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.data.annotation.Id;
import org.springframework.data.projection.ProjectionFactory;
import org.springframework.data.projection.SpelAwareProxyProjectionFactory;
import org.springframework.data.r2dbc.convert.MappingR2dbcConverter;
import org.springframework.data.r2dbc.convert.R2dbcConverter;
import org.springframework.data.r2dbc.core.DatabaseClient;
import org.springframework.data.r2dbc.core.FetchSpec;
import org.springframework.data.r2dbc.mapping.R2dbcMappingContext;
import org.springframework.data.r2dbc.repository.Modifying;
import org.springframework.data.relational.core.mapping.Table;
import org.springframework.data.relational.repository.query.RelationalParameterAccessor;
import org.springframework.data.repository.Repository;
import org.springframework.data.repository.core.RepositoryMetadata;
import org.springframework.data.repository.core.support.AbstractRepositoryMetadata;
import org.springframework.util.ReflectionUtils;

/**
 * Unit tests for {@link AbstractR2dbcQuery}
 *
 * @author Stephen Cohen
 */
@RunWith(MockitoJUnitRunner.StrictStubs.class)
public class AbstractR2dbcQueryUnitTests {

	@Mock(stubOnly = true) private DatabaseClient mockDatabaseClient;
	@Mock(stubOnly = true) private DatabaseClient.GenericExecuteSpec mockGenericExecuteSpec;
	@Mock(stubOnly = true) private DatabaseClient.TypedExecuteSpec<Object> mockTypedExecuteSpec;
	@Mock(stubOnly = true) private FetchSpec<Object> mockFetchSpec;

	private final MappingR2dbcConverter converter = new MappingR2dbcConverter(new R2dbcMappingContext());
	private final RepositoryMetadata repositoryMetadata = AbstractRepositoryMetadata.getMetadata(TestRepository.class);
	private final ProjectionFactory projectionFactory = new SpelAwareProxyProjectionFactory();

	@Before
	public void setUpMocks() {

		when(mockDatabaseClient.execute(Mockito.<Supplier<String>>any())).thenReturn(mockGenericExecuteSpec);

		when(mockGenericExecuteSpec.as(any())).thenReturn(mockTypedExecuteSpec);

		when(mockTypedExecuteSpec.fetch()).thenReturn(mockFetchSpec);
	}

	private R2dbcQueryMethod getQueryMethod(final String name) {

		final Method method = ReflectionUtils.findMethod(TestRepository.class, name, new Class<?>[0]);
		return new R2dbcQueryMethod(method, repositoryMetadata, projectionFactory, converter.getMappingContext());
	}

	private DummyR2dbcQuery makeQuery(final String methodName) {
		return new DummyR2dbcQuery(getQueryMethod(methodName), mockDatabaseClient, converter);
	}

	@Test // gh-421
	public void shouldExecuteFindReturningOneEntity() {

		final DummyR2dbcQuery query = makeQuery("findOne");
		final TestEntity entity = new TestEntity(1, "test");

		when(mockFetchSpec.one()).thenReturn(Mono.just(entity));

		final Object result = query.execute(new Object[0]);

		assertThat(result).isInstanceOf(Mono.class);

		@SuppressWarnings("unchecked")
		final Mono<Object> resultMono = (Mono<Object>) result;

		resultMono.as(StepVerifier::create) //
				.expectNext(entity) //
				.verifyComplete();
	}

	@Test // gh-421
	public void shouldExecuteFindReturningManyEntities() {

		final DummyR2dbcQuery query = makeQuery("findMany");
		final TestEntity[] entities = new TestEntity[] {
				new TestEntity(1, "test1"),
				new TestEntity(2, "test2")
		};

		when(mockFetchSpec.all()).thenReturn(Flux.fromArray(entities));

		final Object result = query.execute(new Object[0]);

		assertThat(result).isInstanceOf(Flux.class);

		@SuppressWarnings("unchecked")
		final Flux<Object> resultFlux = (Flux<Object>) result;

		resultFlux.collectList() //
				.as(StepVerifier::create) //
				.assertNext(results -> assertThat(results).containsExactlyInAnyOrder(entities)) //
				.verifyComplete();
	}

	@Test // gh-421
	public void shouldExecuteModifyingQueryReturningSuccessBoolean() {

		final DummyR2dbcQuery query = makeQuery("modifyAndReturnSuccessBoolean");

		when(mockFetchSpec.rowsUpdated()).thenReturn(Mono.just(1));

		final Object result = query.execute(new Object[0]);

		assertThat(result).isInstanceOf(Mono.class);

		@SuppressWarnings("unchecked")
		final Mono<Object> resultMono = (Mono<Object>) result;

		resultMono.as(StepVerifier::create) //
				.expectNext(true) //
				.verifyComplete();
	}

	@Test // gh-421
	public void shouldExecuteModifyingQueryReturningFailureBoolean() {

		final DummyR2dbcQuery query = makeQuery("modifyAndReturnSuccessBoolean");

		when(mockFetchSpec.rowsUpdated()).thenReturn(Mono.just(0));

		final Object result = query.execute(new Object[0]);

		assertThat(result).isInstanceOf(Mono.class);

		@SuppressWarnings("unchecked")
		final Mono<Object> resultMono = (Mono<Object>) result;

		resultMono.as(StepVerifier::create) //
				.expectNext(false) //
				.verifyComplete();
	}

	@Test // gh-421
	public void shouldExecuteModifyingQueryReturningAffectedRowCount() {

		final DummyR2dbcQuery query = makeQuery("modifyAndReturnAffectedRowCount");

		when(mockFetchSpec.rowsUpdated()).thenReturn(Mono.just(42));

		final Object result = query.execute(new Object[0]);

		assertThat(result).isInstanceOf(Mono.class);

		@SuppressWarnings("unchecked")
		final Mono<Object> resultMono = (Mono<Object>) result;

		resultMono.as(StepVerifier::create) //
				.expectNext(42) //
				.verifyComplete();
	}

	@Test // gh-421
	public void shouldExecuteModifyingQueryReturningVoid() {

		final DummyR2dbcQuery query = makeQuery("modifyAndReturnVoid");

		when(mockFetchSpec.rowsUpdated()).thenReturn(Mono.just(42));

		final Object result = query.execute(new Object[0]);

		assertThat(result).isInstanceOf(Mono.class);

		@SuppressWarnings("unchecked")
		final Mono<Object> resultMono = (Mono<Object>) result;

		resultMono.as(StepVerifier::create).verifyComplete();
	}

	@Test // gh-421
	public void shouldExecuteModifyingQueryReturningKotlinUnit() {

		final DummyR2dbcQuery query = makeQuery("modifyAndReturnKotlinUnit");

		when(mockFetchSpec.rowsUpdated()).thenReturn(Mono.just(42));

		final Object result = query.execute(new Object[0]);

		assertThat(result).isInstanceOf(Mono.class);

		@SuppressWarnings("unchecked")
		final Mono<Object> resultMono = (Mono<Object>) result;

		resultMono.as(StepVerifier::create) //
				.expectNext(Unit.INSTANCE) //
				.verifyComplete();
	}

	private static class DummyR2dbcQuery extends AbstractR2dbcQuery {

		public DummyR2dbcQuery(final R2dbcQueryMethod method, final DatabaseClient databaseClient,
				final R2dbcConverter converter) {
			super(method, databaseClient, converter);
		}

		@Override
		protected boolean isModifyingQuery() {
			return getQueryMethod().isModifyingQuery();
		}

		@Override
		protected BindableQuery createQuery(final RelationalParameterAccessor accessor) {

			return new BindableQuery() {

				@Override
				public <T extends DatabaseClient.BindSpec<T>> T bind(final T bindSpec) {
					return bindSpec;
				}

				@Override
				public String get() {
					return "testQuery";
				}
			};
		}
	}

	@SuppressWarnings("unused")
	private interface TestRepository extends Repository<TestEntity, Integer> {

		Mono<TestEntity> findOne();

		Flux<TestEntity> findMany();

		@Modifying
		Mono<Boolean> modifyAndReturnSuccessBoolean();

		@Modifying
		Mono<Integer> modifyAndReturnAffectedRowCount();

		@Modifying
		Mono<Void> modifyAndReturnVoid();

		@Modifying
		Mono<Unit> modifyAndReturnKotlinUnit();
	}

	@Table
	private static class TestEntity {

		@Id private final Integer id;
		private final String data;

		public TestEntity(final Integer id, final String data) {

			this.id = id;
			this.data = data;
		}

		public Integer getId() {
			return id;
		}

		public String getData() {
			return data;
		}

		@Override
		public boolean equals(final Object other) {

			if (!(other instanceof TestEntity)) {
				return false;
			}

			final TestEntity otherTestEntity = (TestEntity) other;

			return this.id.equals(otherTestEntity.id) && this.data.equals(otherTestEntity.data);
		}
	}
}
