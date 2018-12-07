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
package org.springframework.data.r2dbc.function;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * Contract for fetching tabular results.
 *
 * @param <T> row result type.
 * @author Mark Paluch
 */
public interface RowsFetchSpec<T> {

	/**
	 * Get exactly zero or one result.
	 *
	 * @return {@link Mono#empty()} if no match found. Never {@literal null}.
	 * @throws org.springframework.dao.IncorrectResultSizeDataAccessException if more than one match found.
	 */
	Mono<T> one();

	/**
	 * Get the first or no result.
	 *
	 * @return {@link Mono#empty()} if no match found. Never {@literal null}.
	 */
	Mono<T> first();

	/**
	 * Get all matching elements.
	 *
	 * @return never {@literal null}.
	 */
	Flux<T> all();
}
