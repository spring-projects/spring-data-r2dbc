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

import org.springframework.data.r2dbc.query.Criteria

/**
 * Extension for [Criteria.CriteriaStep.is] providing a
 * `eq(value)` variant.
 *
 * @author Jonas Bark
 */
infix fun Criteria.CriteriaStep.isEquals(value: Any): Criteria =
		`is`(value)

/**
 * Extension for [Criteria.CriteriaStep.in] providing a
 * `isIn(value)` variant.
 *
 * @author Jonas Bark
 */
fun Criteria.CriteriaStep.isIn(vararg value: Any): Criteria =
		`in`(value)

/**
 * Extension for [Criteria.CriteriaStep.in] providing a
 * `isIn(value)` variant.
 *
 * @author Jonas Bark
 */
fun Criteria.CriteriaStep.isIn(values: Collection<Any>): Criteria =
		`in`(values)
