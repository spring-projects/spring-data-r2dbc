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

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import org.springframework.dao.DataAccessException;
import org.springframework.dao.TransientDataAccessResourceException;
import org.springframework.data.r2dbc.query.Query;
import org.springframework.data.r2dbc.query.Update;

/**
 * Interface specifying a basic set of reactive R2DBC operations using entities. Implemented by
 * {@link R2dbcEntityTemplate}. Not often used directly, but a useful option to enhance testability, as it can easily be
 * mocked or stubbed.
 *
 * @author Mark Paluch
 * @since 1.1
 * @see DatabaseClient
 */
public interface R2dbcEntityOperations extends FluentR2dbcOperations {

	/**
	 * Expose the underlying {@link DatabaseClient} to allow SQL operations.
	 *
	 * @return the underlying {@link DatabaseClient}.
	 * @see DatabaseClient
	 */
	DatabaseClient getDatabaseClient();

	// -------------------------------------------------------------------------
	// Methods dealing with org.springframework.data.r2dbc.query.Query
	// -------------------------------------------------------------------------

	/**
	 * Returns the number of rows for the given entity class applying {@link Query}. This overridden method allows users
	 * to further refine the selection Query using a {@link Query} predicate to determine how many entities of the given
	 * {@link Class type} match the Query.
	 *
	 * @param query user-defined count {@link Query} to execute; must not be {@literal null}.
	 * @param entityClass {@link Class type} of the entity; must not be {@literal null}.
	 * @return the number of existing entities.
	 * @throws DataAccessException if any problem occurs while executing the query.
	 */
	Mono<Long> count(Query query, Class<?> entityClass) throws DataAccessException;

	/**
	 * Determine whether the result for {@code entityClass} {@link Query} yields at least one row.
	 *
	 * @param query user-defined exists {@link Query} to execute; must not be {@literal null}.
	 * @param entityClass {@link Class type} of the entity; must not be {@literal null}.
	 * @return {@literal true} if the object exists.
	 * @throws DataAccessException if any problem occurs while executing the query.
	 * @since 2.1
	 */
	Mono<Boolean> exists(Query query, Class<?> entityClass) throws DataAccessException;

	/**
	 * Execute a {@code SELECT} query and convert the resulting items to a stream of entities.
	 *
	 * @param query must not be {@literal null}.
	 * @param entityClass The entity type must not be {@literal null}.
	 * @return the result objects returned by the action.
	 * @throws DataAccessException if there is any problem issuing the execution.
	 */
	<T> Flux<T> select(Query query, Class<T> entityClass) throws DataAccessException;

	/**
	 * Execute a {@code SELECT} query and convert the resulting item to an entity.
	 *
	 * @param query must not be {@literal null}.
	 * @param entityClass The entity type must not be {@literal null}.
	 * @return the result object returned by the action or {@link Mono#empty()}.
	 * @throws DataAccessException if there is any problem issuing the execution.
	 */
	<T> Mono<T> selectOne(Query query, Class<T> entityClass) throws DataAccessException;

	/**
	 * Update the queried entities and return {@literal true} if the update was applied.
	 *
	 * @param query must not be {@literal null}.
	 * @param update must not be {@literal null}.
	 * @param entityClass The entity type must not be {@literal null}.
	 * @return the number of affected rows.
	 * @throws DataAccessException if there is any problem executing the query.
	 */
	Mono<Integer> update(Query query, Update update, Class<?> entityClass) throws DataAccessException;

	/**
	 * Remove entities (rows)/columns from the table by {@link Query}.
	 *
	 * @param query must not be {@literal null}.
	 * @param entityClass The entity type must not be {@literal null}.
	 * @return the number of affected rows.
	 * @throws DataAccessException if there is any problem issuing the execution.
	 */
	Mono<Integer> delete(Query query, Class<?> entityClass) throws DataAccessException;

	// -------------------------------------------------------------------------
	// Methods dealing with entities
	// -------------------------------------------------------------------------

	/**
	 * Insert the given entity and emit the entity if the insert was applied.
	 *
	 * @param entity The entity to insert, must not be {@literal null}.
	 * @return the inserted entity.
	 * @throws DataAccessException if there is any problem issuing the execution.
	 */
	<T> Mono<T> insert(T entity) throws DataAccessException;

	/**
	 * Update the given entity and emit the entity if the update was applied.
	 *
	 * @param entity The entity to update, must not be {@literal null}.
	 * @return the updated entity.
	 * @throws DataAccessException if there is any problem issuing the execution.
	 * @throws TransientDataAccessResourceException if the update did not affect any rows.
	 */
	<T> Mono<T> update(T entity) throws DataAccessException;

	/**
	 * Delete the given entity and emit the entity if the delete was applied.
	 *
	 * @param entity must not be {@literal null}.
	 * @return the deleted entity.
	 * @throws DataAccessException if there is any problem issuing the execution.
	 */
	<T> Mono<T> delete(T entity) throws DataAccessException;
}
