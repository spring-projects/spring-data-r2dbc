/*
 * Copyright 2018-2020 the original author or authors.
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

import reactor.core.publisher.Mono;

import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

/**
 * Implementation of {@link ReactiveInsertOperation}.
 *
 * @author Mark Paluch
 * @since 1.1
 */
class ReactiveInsertOperationSupport implements ReactiveInsertOperation {

	private final R2dbcEntityTemplate template;

	ReactiveInsertOperationSupport(R2dbcEntityTemplate template) {
		this.template = template;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.r2dbc.core.ReactiveInsertOperation#insert(java.lang.Class)
	 */
	@Override
	public <T> ReactiveInsert<T> insert(Class<T> domainType) {

		Assert.notNull(domainType, "DomainType must not be null");

		return new ReactiveInsertSupport<>(this.template, domainType, null);
	}

	static class ReactiveInsertSupport<T> implements ReactiveInsert<T> {

		private final R2dbcEntityTemplate template;

		private final Class<T> domainType;

		private final @Nullable String tableName;

		ReactiveInsertSupport(R2dbcEntityTemplate template, Class<T> domainType, String tableName) {
			this.template = template;
			this.domainType = domainType;
			this.tableName = tableName;
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.r2dbc.core.ReactiveInsertOperation.InsertWithTable#into(java.lang.String)
		 */
		@Override
		public TerminatingInsert<T> into(String tableName) {

			Assert.notNull(tableName, "Table name must not be null");

			return new ReactiveInsertSupport<>(this.template, this.domainType, tableName);
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.r2dbc.core.ReactiveInsertOperation.TerminatingInsert#one(java.lang.Object)
		 */
		@Override
		public Mono<T> using(T object) {

			Assert.notNull(object, "Object to insert must not be null");

			return this.template.doInsert(object, getTableName());
		}

		private String getTableName() {
			return this.tableName != null ? this.tableName : this.template.getTableName(this.domainType);
		}
	}
}
