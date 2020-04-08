/*
 * Copyright 2019 the original author or authors.
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
package org.springframework.data.r2dbc.core

import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import org.assertj.core.api.Assertions.assertThat
import org.junit.Test
import org.springframework.data.r2dbc.query.Criteria

/**
 * Unit tests for [Criteria.CriteriaStep] extensions.
 *
 * @author Jonas Bark
 * @author Mingyuan Wu
 */
class CriteriaStepExtensionsTests {

	@Test // gh-122
	fun eqIsCriteriaStep() {

		val spec = mockk<Criteria.CriteriaStep>()
		val criteria = mockk<Criteria>()

		every { spec.`is`("test") } returns criteria

		assertThat(spec isEquals "test").isEqualTo(criteria)

		verify {
			spec.`is`("test")
		}
	}

	@Test // gh-122
	fun inVarargCriteriaStep() {

		val spec = mockk<Criteria.CriteriaStep>()
		val criteria = mockk<Criteria>()

		every { spec.`in`(any() as Array<Any>) } returns criteria

		assertThat(spec.isIn("test")).isEqualTo(criteria)

		verify {
			spec.`in`(arrayOf("test"))
		}
	}

	@Test // gh-122
	fun inListCriteriaStep() {

		val spec = mockk<Criteria.CriteriaStep>()
		val criteria = mockk<Criteria>()

		every { spec.`in`(listOf("test")) } returns criteria

		assertThat(spec.isIn(listOf("test"))).isEqualTo(criteria)

		verify {
			spec.`in`(listOf("test"))
		}
	}

	@Test // gh-122
	fun eqIsCriteriaStepForSpringData2() {

		val spec = mockk<org.springframework.data.relational.core.query.Criteria.CriteriaStep>()
		val criteria = mockk<org.springframework.data.relational.core.query.Criteria>()

		every { spec.`is`("test") } returns criteria

		assertThat(spec isEquals "test").isEqualTo(criteria)

		verify {
			spec.`is`("test")
		}
	}

	@Test // gh-122
	fun inVarargCriteriaStepForSpringData2() {

		val spec = mockk<org.springframework.data.relational.core.query.Criteria.CriteriaStep>()
		val criteria = mockk<org.springframework.data.relational.core.query.Criteria>()

		every { spec.`in`(any() as Array<Any>) } returns criteria

		assertThat(spec.isIn("test")).isEqualTo(criteria)

		verify {
			spec.`in`(arrayOf("test"))
		}
	}

	@Test // gh-122
	fun inListCriteriaStepForSpringData2() {

		val spec = mockk<org.springframework.data.relational.core.query.Criteria.CriteriaStep>()
		val criteria = mockk<org.springframework.data.relational.core.query.Criteria>()

		every { spec.`in`(listOf("test")) } returns criteria

		assertThat(spec.isIn(listOf("test"))).isEqualTo(criteria)

		verify {
			spec.`in`(listOf("test"))
		}
	}
}
