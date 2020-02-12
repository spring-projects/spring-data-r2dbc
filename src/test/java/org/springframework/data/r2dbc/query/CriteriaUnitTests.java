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
package org.springframework.data.r2dbc.query;

import static org.assertj.core.api.Assertions.*;
import static org.springframework.data.r2dbc.query.Criteria.*;

import java.util.Arrays;

import org.junit.Test;

import org.springframework.data.r2dbc.query.Criteria.*;
import org.springframework.data.relational.core.sql.SqlIdentifier;

/**
 * Unit tests for {@link Criteria}.
 *
 * @author Mark Paluch
 */
public class CriteriaUnitTests {

	@Test // gh-64
	public void andChainedCriteria() {

		Criteria criteria = where("foo").is("bar").and("baz").isNotNull();

		assertThat(criteria.getColumn()).isEqualTo(SqlIdentifier.unquoted("baz"));
		assertThat(criteria.getComparator()).isEqualTo(Comparator.IS_NOT_NULL);
		assertThat(criteria.getValue()).isNull();
		assertThat(criteria.getPrevious()).isNotNull();
		assertThat(criteria.getCombinator()).isEqualTo(Combinator.AND);

		criteria = criteria.getPrevious();

		assertThat(criteria.getColumn()).isEqualTo(SqlIdentifier.unquoted("foo"));
		assertThat(criteria.getComparator()).isEqualTo(Comparator.EQ);
		assertThat(criteria.getValue()).isEqualTo("bar");
	}

	@Test // gh-64
	public void orChainedCriteria() {

		Criteria criteria = where("foo").is("bar").or("baz").isNotNull();

		assertThat(criteria.getColumn()).isEqualTo(SqlIdentifier.unquoted("baz"));
		assertThat(criteria.getCombinator()).isEqualTo(Combinator.OR);

		criteria = criteria.getPrevious();

		assertThat(criteria.getPrevious()).isNull();
		assertThat(criteria.getValue()).isEqualTo("bar");
	}

	@Test // gh-64
	public void shouldBuildEqualsCriteria() {

		Criteria criteria = where("foo").is("bar");

		assertThat(criteria.getColumn()).isEqualTo(SqlIdentifier.unquoted("foo"));
		assertThat(criteria.getComparator()).isEqualTo(Comparator.EQ);
		assertThat(criteria.getValue()).isEqualTo("bar");
	}

	@Test // gh-64
	public void shouldBuildNotEqualsCriteria() {

		Criteria criteria = where("foo").not("bar");

		assertThat(criteria.getColumn()).isEqualTo(SqlIdentifier.unquoted("foo"));
		assertThat(criteria.getComparator()).isEqualTo(Comparator.NEQ);
		assertThat(criteria.getValue()).isEqualTo("bar");
	}

	@Test // gh-64
	public void shouldBuildInCriteria() {

		Criteria criteria = where("foo").in("bar", "baz");

		assertThat(criteria.getColumn()).isEqualTo(SqlIdentifier.unquoted("foo"));
		assertThat(criteria.getComparator()).isEqualTo(Comparator.IN);
		assertThat(criteria.getValue()).isEqualTo(Arrays.asList("bar", "baz"));
	}

	@Test // gh-64
	public void shouldBuildNotInCriteria() {

		Criteria criteria = where("foo").notIn("bar", "baz");

		assertThat(criteria.getColumn()).isEqualTo(SqlIdentifier.unquoted("foo"));
		assertThat(criteria.getComparator()).isEqualTo(Comparator.NOT_IN);
		assertThat(criteria.getValue()).isEqualTo(Arrays.asList("bar", "baz"));
	}

	@Test // gh-64
	public void shouldBuildGtCriteria() {

		Criteria criteria = where("foo").greaterThan(1);

		assertThat(criteria.getColumn()).isEqualTo(SqlIdentifier.unquoted("foo"));
		assertThat(criteria.getComparator()).isEqualTo(Comparator.GT);
		assertThat(criteria.getValue()).isEqualTo(1);
	}

	@Test // gh-64
	public void shouldBuildGteCriteria() {

		Criteria criteria = where("foo").greaterThanOrEquals(1);

		assertThat(criteria.getColumn()).isEqualTo(SqlIdentifier.unquoted("foo"));
		assertThat(criteria.getComparator()).isEqualTo(Comparator.GTE);
		assertThat(criteria.getValue()).isEqualTo(1);
	}

	@Test // gh-64
	public void shouldBuildLtCriteria() {

		Criteria criteria = where("foo").lessThan(1);

		assertThat(criteria.getColumn()).isEqualTo(SqlIdentifier.unquoted("foo"));
		assertThat(criteria.getComparator()).isEqualTo(Comparator.LT);
		assertThat(criteria.getValue()).isEqualTo(1);
	}

	@Test // gh-64
	public void shouldBuildLteCriteria() {

		Criteria criteria = where("foo").lessThanOrEquals(1);

		assertThat(criteria.getColumn()).isEqualTo(SqlIdentifier.unquoted("foo"));
		assertThat(criteria.getComparator()).isEqualTo(Comparator.LTE);
		assertThat(criteria.getValue()).isEqualTo(1);
	}

	@Test // gh-64
	public void shouldBuildLikeCriteria() {

		Criteria criteria = where("foo").like("hello%");

		assertThat(criteria.getColumn()).isEqualTo(SqlIdentifier.unquoted("foo"));
		assertThat(criteria.getComparator()).isEqualTo(Comparator.LIKE);
		assertThat(criteria.getValue()).isEqualTo("hello%");
	}

	@Test // gh-64
	public void shouldBuildIsNullCriteria() {

		Criteria criteria = where("foo").isNull();

		assertThat(criteria.getColumn()).isEqualTo(SqlIdentifier.unquoted("foo"));
		assertThat(criteria.getComparator()).isEqualTo(Comparator.IS_NULL);
	}

	@Test // gh-64
	public void shouldBuildIsNotNullCriteria() {

		Criteria criteria = where("foo").isNotNull();

		assertThat(criteria.getColumn()).isEqualTo(SqlIdentifier.unquoted("foo"));
		assertThat(criteria.getComparator()).isEqualTo(Comparator.IS_NOT_NULL);
	}
}
