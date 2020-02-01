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
package org.springframework.data.r2dbc.repository.query;

import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.ConnectionFactoryMetadata;
import lombok.Data;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.data.annotation.Id;
import org.springframework.data.projection.SpelAwareProxyProjectionFactory;
import org.springframework.data.r2dbc.convert.R2dbcConverter;
import org.springframework.data.r2dbc.core.DatabaseClient;
import org.springframework.data.r2dbc.core.DefaultReactiveDataAccessStrategy;
import org.springframework.data.r2dbc.core.ReactiveDataAccessStrategy;
import org.springframework.data.r2dbc.dialect.DialectResolver;
import org.springframework.data.r2dbc.dialect.R2dbcDialect;
import org.springframework.data.r2dbc.mapping.R2dbcMappingContext;
import org.springframework.data.relational.core.mapping.RelationalMappingContext;
import org.springframework.data.relational.core.mapping.Table;
import org.springframework.data.relational.repository.query.RelationalParametersParameterAccessor;
import org.springframework.data.repository.Repository;
import org.springframework.data.repository.core.support.DefaultRepositoryMetadata;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.lang.reflect.Method;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

/**
 * @author Roman Chigvintsev
 */
@RunWith(MockitoJUnitRunner.class)
public class PartTreeR2dbcQueryIntegrationTests {
    private static final String TABLE = "users";
    private static final String ALL_FIELDS = TABLE + ".id, "
            + TABLE + ".first_name, "
            + TABLE + ".last_name, "
            + TABLE + ".date_of_birth, "
            + TABLE + ".age, "
            + TABLE + ".active";

    @Mock
    private ConnectionFactory connectionFactory;
    @Mock
    private R2dbcConverter r2dbcConverter;

    private RelationalMappingContext mappingContext;
    private ReactiveDataAccessStrategy dataAccessStrategy;
    private DatabaseClient databaseClient;

    @Before
    public void setUp() {
        ConnectionFactoryMetadata metadataMock = mock(ConnectionFactoryMetadata.class);
        when(metadataMock.getName()).thenReturn("PostgreSQL");
        when(connectionFactory.getMetadata()).thenReturn(metadataMock);

        mappingContext = new R2dbcMappingContext();
        doReturn(mappingContext).when(r2dbcConverter).getMappingContext();

        R2dbcDialect dialect = DialectResolver.getDialect(connectionFactory);
        dataAccessStrategy = new DefaultReactiveDataAccessStrategy(dialect, r2dbcConverter);

        databaseClient = DatabaseClient.builder()
                .connectionFactory(connectionFactory)
                .dataAccessStrategy(dataAccessStrategy)
                .build();
    }

    @Test
    public void createsQueryToFindAllEntitiesByStringAttribute() throws Exception {
        R2dbcQueryMethod queryMethod = getQueryMethod("findAllByFirstName", String.class);
        PartTreeR2dbcQuery r2dbcQuery = new PartTreeR2dbcQuery(queryMethod, databaseClient, r2dbcConverter,
                dataAccessStrategy);
        BindableQuery bindableQuery = r2dbcQuery.createQuery(getAccessor(queryMethod, new Object[]{"John"}));
        String expectedSql = "SELECT " + ALL_FIELDS + " FROM " + TABLE + " WHERE " + TABLE + ".first_name = ?";
        assertThat(bindableQuery.get()).isEqualTo(expectedSql);
    }

    @Test
    public void createsQueryWithIsNullCondition() throws Exception {
        R2dbcQueryMethod queryMethod = getQueryMethod("findAllByFirstName", String.class);
        PartTreeR2dbcQuery r2dbcQuery = new PartTreeR2dbcQuery(queryMethod, databaseClient, r2dbcConverter,
                dataAccessStrategy);
        BindableQuery bindableQuery = r2dbcQuery.createQuery((getAccessor(queryMethod, new Object[]{null})));
        String expectedSql = "SELECT " + ALL_FIELDS + " FROM " + TABLE + " WHERE " + TABLE + ".first_name IS NULL";
        assertThat(bindableQuery.get()).isEqualTo(expectedSql);
    }

    @Test
    public void createsQueryWithLimitForExistsProjection() throws Exception {
        R2dbcQueryMethod queryMethod = getQueryMethod("existsByFirstName", String.class);
        PartTreeR2dbcQuery r2dbcQuery = new PartTreeR2dbcQuery(queryMethod, databaseClient, r2dbcConverter,
                dataAccessStrategy);
        BindableQuery query = r2dbcQuery.createQuery((getAccessor(queryMethod, new Object[]{"John"})));
        String expectedSql = "SELECT " + TABLE + ".id FROM " + TABLE + " WHERE " + TABLE + ".first_name = ? LIMIT 1";
        assertThat(query.get()).isEqualTo(expectedSql);
    }

    @Test
    public void createsQueryToFindAllEntitiesByTwoStringAttributes() throws Exception {
        R2dbcQueryMethod queryMethod = getQueryMethod("findAllByLastNameAndFirstName", String.class, String.class);
        PartTreeR2dbcQuery r2dbcQuery = new PartTreeR2dbcQuery(queryMethod, databaseClient, r2dbcConverter,
                dataAccessStrategy);
        BindableQuery bindableQuery = r2dbcQuery.createQuery(getAccessor(queryMethod, new Object[]{"Doe", "John"}));
        String expectedSql = "SELECT " + ALL_FIELDS + " FROM " + TABLE
                + " WHERE " + TABLE + ".last_name = ? AND " + TABLE + ".first_name = ?";
        assertThat(bindableQuery.get()).isEqualTo(expectedSql);
    }

    @Test
    public void createsQueryToFindAllEntitiesByOneOfTwoStringAttributes() throws Exception {
        R2dbcQueryMethod queryMethod = getQueryMethod("findAllByLastNameOrFirstName", String.class, String.class);
        PartTreeR2dbcQuery r2dbcQuery = new PartTreeR2dbcQuery(queryMethod, databaseClient, r2dbcConverter,
                dataAccessStrategy);
        BindableQuery bindableQuery = r2dbcQuery.createQuery(getAccessor(queryMethod, new Object[]{"Doe", "John"}));
        String expectedSql = "SELECT " + ALL_FIELDS + " FROM " + TABLE
                + " WHERE " + TABLE + ".last_name = ? OR " + TABLE + ".first_name = ?";
        assertThat(bindableQuery.get()).isEqualTo(expectedSql);
    }

    @Test
    public void createsQueryToFindAllEntitiesByDateAttributeBetween() throws Exception {
        R2dbcQueryMethod queryMethod = getQueryMethod("findAllByDateOfBirthBetween", Date.class, Date.class);
        PartTreeR2dbcQuery r2dbcQuery = new PartTreeR2dbcQuery(queryMethod, databaseClient, r2dbcConverter,
                dataAccessStrategy);
        RelationalParametersParameterAccessor accessor = getAccessor(queryMethod, new Object[]{new Date(), new Date()});
        BindableQuery bindableQuery = r2dbcQuery.createQuery(accessor);
        String expectedSql = "SELECT " + ALL_FIELDS + " FROM " + TABLE
                + " WHERE " + TABLE + ".date_of_birth >= ? AND " + TABLE + ".date_of_birth <= ?";
        assertThat(bindableQuery.get()).isEqualTo(expectedSql);
    }

    @Test
    public void createsQueryToFindAllEntitiesByIntegerAttributeLessThan() throws Exception {
        R2dbcQueryMethod queryMethod = getQueryMethod("findAllByAgeLessThan", Integer.class);
        PartTreeR2dbcQuery r2dbcQuery = new PartTreeR2dbcQuery(queryMethod, databaseClient, r2dbcConverter,
                dataAccessStrategy);
        RelationalParametersParameterAccessor accessor = getAccessor(queryMethod, new Object[]{30});
        BindableQuery bindableQuery = r2dbcQuery.createQuery(accessor);
        String expectedSql = "SELECT " + ALL_FIELDS + " FROM " + TABLE + " WHERE " + TABLE + ".age < ?";
        assertThat(bindableQuery.get()).isEqualTo(expectedSql);
    }

    @Test
    public void createsQueryToFindAllEntitiesByIntegerAttributeLessThanEqual() throws Exception {
        R2dbcQueryMethod queryMethod = getQueryMethod("findAllByAgeLessThanEqual", Integer.class);
        PartTreeR2dbcQuery r2dbcQuery = new PartTreeR2dbcQuery(queryMethod, databaseClient, r2dbcConverter,
                dataAccessStrategy);
        RelationalParametersParameterAccessor accessor = getAccessor(queryMethod, new Object[]{30});
        BindableQuery bindableQuery = r2dbcQuery.createQuery(accessor);
        String expectedSql = "SELECT " + ALL_FIELDS + " FROM " + TABLE + " WHERE " + TABLE + ".age <= ?";
        assertThat(bindableQuery.get()).isEqualTo(expectedSql);
    }

    @Test
    public void createsQueryToFindAllEntitiesByIntegerAttributeGreaterThan() throws Exception {
        R2dbcQueryMethod queryMethod = getQueryMethod("findAllByAgeGreaterThan", Integer.class);
        PartTreeR2dbcQuery r2dbcQuery = new PartTreeR2dbcQuery(queryMethod, databaseClient, r2dbcConverter,
                dataAccessStrategy);
        RelationalParametersParameterAccessor accessor = getAccessor(queryMethod, new Object[]{30});
        BindableQuery bindableQuery = r2dbcQuery.createQuery(accessor);
        String expectedSql = "SELECT " + ALL_FIELDS + " FROM " + TABLE + " WHERE " + TABLE + ".age > ?";
        assertThat(bindableQuery.get()).isEqualTo(expectedSql);
    }

    @Test
    public void createsQueryToFindAllEntitiesByIntegerAttributeGreaterThanEqual() throws Exception {
        R2dbcQueryMethod queryMethod = getQueryMethod("findAllByAgeGreaterThanEqual", Integer.class);
        PartTreeR2dbcQuery r2dbcQuery = new PartTreeR2dbcQuery(queryMethod, databaseClient, r2dbcConverter,
                dataAccessStrategy);
        RelationalParametersParameterAccessor accessor = getAccessor(queryMethod, new Object[]{30});
        BindableQuery bindableQuery = r2dbcQuery.createQuery(accessor);
        String expectedSql = "SELECT " + ALL_FIELDS + " FROM " + TABLE + " WHERE " + TABLE + ".age >= ?";
        assertThat(bindableQuery.get()).isEqualTo(expectedSql);
    }

    @Test
    public void createsQueryToFindAllEntitiesByDateAttributeAfter() throws Exception {
        R2dbcQueryMethod queryMethod = getQueryMethod("findAllByDateOfBirthAfter", Date.class);
        PartTreeR2dbcQuery r2dbcQuery = new PartTreeR2dbcQuery(queryMethod, databaseClient, r2dbcConverter,
                dataAccessStrategy);
        RelationalParametersParameterAccessor accessor = getAccessor(queryMethod, new Object[]{new Date()});
        BindableQuery bindableQuery = r2dbcQuery.createQuery(accessor);
        String expectedSql = "SELECT " + ALL_FIELDS + " FROM " + TABLE + " WHERE " + TABLE + ".date_of_birth > ?";
        assertThat(bindableQuery.get()).isEqualTo(expectedSql);
    }

    @Test
    public void createsQueryToFindAllEntitiesByDateAttributeBefore() throws Exception {
        R2dbcQueryMethod queryMethod = getQueryMethod("findAllByDateOfBirthBefore", Date.class);
        PartTreeR2dbcQuery r2dbcQuery = new PartTreeR2dbcQuery(queryMethod, databaseClient, r2dbcConverter,
                dataAccessStrategy);
        RelationalParametersParameterAccessor accessor = getAccessor(queryMethod, new Object[]{new Date()});
        BindableQuery bindableQuery = r2dbcQuery.createQuery(accessor);
        String expectedSql = "SELECT " + ALL_FIELDS + " FROM " + TABLE + " WHERE " + TABLE + ".date_of_birth < ?";
        assertThat(bindableQuery.get()).isEqualTo(expectedSql);
    }

    @Test
    public void createsQueryToFindAllEntitiesByIntegerAttributeIsNull() throws Exception {
        R2dbcQueryMethod queryMethod = getQueryMethod("findAllByAgeIsNull");
        PartTreeR2dbcQuery r2dbcQuery = new PartTreeR2dbcQuery(queryMethod, databaseClient, r2dbcConverter,
                dataAccessStrategy);
        RelationalParametersParameterAccessor accessor = getAccessor(queryMethod, new Object[0]);
        BindableQuery bindableQuery = r2dbcQuery.createQuery(accessor);
        String expectedSql = "SELECT " + ALL_FIELDS + " FROM " + TABLE + " WHERE " + TABLE + ".age IS NULL";
        assertThat(bindableQuery.get()).isEqualTo(expectedSql);
    }

    @Test
    public void createsQueryToFindAllEntitiesByIntegerAttributeIsNotNull() throws Exception {
        R2dbcQueryMethod queryMethod = getQueryMethod("findAllByAgeIsNotNull");
        PartTreeR2dbcQuery r2dbcQuery = new PartTreeR2dbcQuery(queryMethod, databaseClient, r2dbcConverter,
                dataAccessStrategy);
        RelationalParametersParameterAccessor accessor = getAccessor(queryMethod, new Object[0]);
        BindableQuery bindableQuery = r2dbcQuery.createQuery(accessor);
        String expectedSql = "SELECT " + ALL_FIELDS + " FROM " + TABLE + " WHERE " + TABLE + ".age IS NOT NULL";
        assertThat(bindableQuery.get()).isEqualTo(expectedSql);
    }

    @Test
    public void createsQueryToFindAllEntitiesByStringAttributeLike() throws Exception {
        R2dbcQueryMethod queryMethod = getQueryMethod("findAllByFirstNameLike", String.class);
        PartTreeR2dbcQuery r2dbcQuery = new PartTreeR2dbcQuery(queryMethod, databaseClient, r2dbcConverter,
                dataAccessStrategy);
        RelationalParametersParameterAccessor accessor = getAccessor(queryMethod, new Object[]{"%John%"});
        BindableQuery bindableQuery = r2dbcQuery.createQuery(accessor);
        String expectedSql = "SELECT " + ALL_FIELDS + " FROM " + TABLE + " WHERE " + TABLE + ".first_name LIKE ?";
        assertThat(bindableQuery.get()).isEqualTo(expectedSql);
    }

    @Test
    public void createsQueryToFindAllEntitiesByStringAttributeNotLike() throws Exception {
        R2dbcQueryMethod queryMethod = getQueryMethod("findAllByFirstNameNotLike", String.class);
        PartTreeR2dbcQuery r2dbcQuery = new PartTreeR2dbcQuery(queryMethod, databaseClient, r2dbcConverter,
                dataAccessStrategy);
        RelationalParametersParameterAccessor accessor = getAccessor(queryMethod, new Object[]{"%John%"});
        BindableQuery bindableQuery = r2dbcQuery.createQuery(accessor);
        String expectedSql = "SELECT " + ALL_FIELDS + " FROM " + TABLE + " WHERE " + TABLE + ".first_name NOT LIKE ?";
        assertThat(bindableQuery.get()).isEqualTo(expectedSql);
    }

    @Test
    public void createsQueryToFindAllEntitiesByStringAttributeStartingWith() throws Exception {
        R2dbcQueryMethod queryMethod = getQueryMethod("findAllByFirstNameStartingWith", String.class);
        PartTreeR2dbcQuery r2dbcQuery = new PartTreeR2dbcQuery(queryMethod, databaseClient, r2dbcConverter,
                dataAccessStrategy);
        RelationalParametersParameterAccessor accessor = getAccessor(queryMethod, new Object[]{"Jo"});
        BindableQuery bindableQuery = r2dbcQuery.createQuery(accessor);
        String expectedSql = "SELECT " + ALL_FIELDS + " FROM " + TABLE + " WHERE " + TABLE + ".first_name LIKE ?";
        assertThat(bindableQuery.get()).isEqualTo(expectedSql);
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    @Test
    public void appendsLikeOperatorParameterWithPercentSymbolForStartingWithQuery() throws Exception {
        R2dbcQueryMethod queryMethod = getQueryMethod("findAllByFirstNameStartingWith", String.class);
        PartTreeR2dbcQuery r2dbcQuery = new PartTreeR2dbcQuery(queryMethod, databaseClient, r2dbcConverter,
                dataAccessStrategy);
        RelationalParametersParameterAccessor accessor = getAccessor(queryMethod, new Object[]{"Jo"});
        BindableQuery bindableQuery = r2dbcQuery.createQuery(accessor);
        DatabaseClient.BindSpec bindSpecMock = mock(DatabaseClient.BindSpec.class);
        bindableQuery.bind(bindSpecMock);
        verify(bindSpecMock, times(1)).bind(0, "Jo%");
    }

    @Test
    public void createsQueryToFindAllEntitiesByStringAttributeEndingWith() throws Exception {
        R2dbcQueryMethod queryMethod = getQueryMethod("findAllByFirstNameEndingWith", String.class);
        PartTreeR2dbcQuery r2dbcQuery = new PartTreeR2dbcQuery(queryMethod, databaseClient, r2dbcConverter,
                dataAccessStrategy);
        RelationalParametersParameterAccessor accessor = getAccessor(queryMethod, new Object[]{"hn"});
        BindableQuery bindableQuery = r2dbcQuery.createQuery(accessor);
        String expectedSql = "SELECT " + ALL_FIELDS + " FROM " + TABLE + " WHERE " + TABLE + ".first_name LIKE ?";
        assertThat(bindableQuery.get()).isEqualTo(expectedSql);
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    @Test
    public void prependsLikeOperatorParameterWithPercentSymbolForStartingWithQuery() throws Exception {
        R2dbcQueryMethod queryMethod = getQueryMethod("findAllByFirstNameEndingWith", String.class);
        PartTreeR2dbcQuery r2dbcQuery = new PartTreeR2dbcQuery(queryMethod, databaseClient, r2dbcConverter,
                dataAccessStrategy);
        RelationalParametersParameterAccessor accessor = getAccessor(queryMethod, new Object[]{"hn"});
        BindableQuery bindableQuery = r2dbcQuery.createQuery(accessor);
        DatabaseClient.BindSpec bindSpecMock = mock(DatabaseClient.BindSpec.class);
        bindableQuery.bind(bindSpecMock);
        verify(bindSpecMock, times(1)).bind(0, "%hn");
    }

    @Test
    public void createsQueryToFindAllEntitiesByStringAttributeContaining() throws Exception {
        R2dbcQueryMethod queryMethod = getQueryMethod("findAllByFirstNameContaining", String.class);
        PartTreeR2dbcQuery r2dbcQuery = new PartTreeR2dbcQuery(queryMethod, databaseClient, r2dbcConverter,
                dataAccessStrategy);
        RelationalParametersParameterAccessor accessor = getAccessor(queryMethod, new Object[]{"oh"});
        BindableQuery bindableQuery = r2dbcQuery.createQuery(accessor);
        String expectedSql = "SELECT " + ALL_FIELDS + " FROM " + TABLE + " WHERE " + TABLE + ".first_name LIKE ?";
        assertThat(bindableQuery.get()).isEqualTo(expectedSql);
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    @Test
    public void wrapsLikeOperatorParameterWithPercentSymbolsForContainingQuery() throws Exception {
        R2dbcQueryMethod queryMethod = getQueryMethod("findAllByFirstNameContaining", String.class);
        PartTreeR2dbcQuery r2dbcQuery = new PartTreeR2dbcQuery(queryMethod, databaseClient, r2dbcConverter,
                dataAccessStrategy);
        RelationalParametersParameterAccessor accessor = getAccessor(queryMethod, new Object[]{"hn"});
        BindableQuery bindableQuery = r2dbcQuery.createQuery(accessor);
        DatabaseClient.BindSpec bindSpecMock = mock(DatabaseClient.BindSpec.class);
        bindableQuery.bind(bindSpecMock);
        verify(bindSpecMock, times(1)).bind(0, "%hn%");
    }

    @Test
    public void createsQueryToFindAllEntitiesByIntegerAttributeWithDescendingOrderingByStringAttribute()
            throws Exception {
        R2dbcQueryMethod queryMethod = getQueryMethod("findAllByAgeOrderByLastNameDesc", Integer.class);
        PartTreeR2dbcQuery r2dbcQuery = new PartTreeR2dbcQuery(queryMethod, databaseClient, r2dbcConverter,
                dataAccessStrategy);
        RelationalParametersParameterAccessor accessor = getAccessor(queryMethod, new Object[]{"oh"});
        BindableQuery bindableQuery = r2dbcQuery.createQuery(accessor);
        String expectedSql = "SELECT " + ALL_FIELDS + " FROM " + TABLE
                + " WHERE " + TABLE + ".age = ? ORDER BY last_name DESC";
        assertThat(bindableQuery.get()).isEqualTo(expectedSql);
    }

    @Test
    public void createsQueryToFindAllEntitiesByStringAttributeNot() throws Exception {
        R2dbcQueryMethod queryMethod = getQueryMethod("findAllByLastNameNot", String.class);
        PartTreeR2dbcQuery r2dbcQuery = new PartTreeR2dbcQuery(queryMethod, databaseClient, r2dbcConverter,
                dataAccessStrategy);
        RelationalParametersParameterAccessor accessor = getAccessor(queryMethod, new Object[]{"Doe"});
        BindableQuery bindableQuery = r2dbcQuery.createQuery(accessor);
        String expectedSql = "SELECT " + ALL_FIELDS + " FROM " + TABLE + " WHERE " + TABLE + ".last_name != ?";
        assertThat(bindableQuery.get()).isEqualTo(expectedSql);
    }

    @Test
    public void createsQueryToFindAllEntitiesByIntegerAttributeIn() throws Exception {
        R2dbcQueryMethod queryMethod = getQueryMethod("findAllByAgeIn", Collection.class);
        PartTreeR2dbcQuery r2dbcQuery = new PartTreeR2dbcQuery(queryMethod, databaseClient, r2dbcConverter,
                dataAccessStrategy);
        RelationalParametersParameterAccessor accessor = getAccessor(queryMethod,
                new Object[]{Collections.singleton(25)});
        BindableQuery bindableQuery = r2dbcQuery.createQuery(accessor);
        String expectedSql = "SELECT " + ALL_FIELDS + " FROM " + TABLE + " WHERE " + TABLE + ".age IN (?)";
        assertThat(bindableQuery.get()).isEqualTo(expectedSql);
    }

    @Test
    public void createsQueryToFindAllEntitiesByIntegerAttributeNotIn() throws Exception {
        R2dbcQueryMethod queryMethod = getQueryMethod("findAllByAgeNotIn", Collection.class);
        PartTreeR2dbcQuery r2dbcQuery = new PartTreeR2dbcQuery(queryMethod, databaseClient, r2dbcConverter,
                dataAccessStrategy);
        RelationalParametersParameterAccessor accessor = getAccessor(queryMethod,
                new Object[]{Collections.singleton(25)});
        BindableQuery bindableQuery = r2dbcQuery.createQuery(accessor);
        String expectedSql = "SELECT " + ALL_FIELDS + " FROM " + TABLE + " WHERE " + TABLE + ".age NOT IN (?)";
        assertThat(bindableQuery.get()).isEqualTo(expectedSql);
    }

    @Test
    public void createsQueryToFindAllEntitiesByBooleanAttributeTrue() throws Exception {
        R2dbcQueryMethod queryMethod = getQueryMethod("findAllByActiveTrue");
        PartTreeR2dbcQuery r2dbcQuery = new PartTreeR2dbcQuery(queryMethod, databaseClient, r2dbcConverter,
                dataAccessStrategy);
        RelationalParametersParameterAccessor accessor = getAccessor(queryMethod, new Object[0]);
        BindableQuery bindableQuery = r2dbcQuery.createQuery(accessor);
        String expectedSql = "SELECT " + ALL_FIELDS + " FROM " + TABLE + " WHERE " + TABLE + ".active = TRUE";
        assertThat(bindableQuery.get()).isEqualTo(expectedSql);
    }

    @Test
    public void createsQueryToFindAllEntitiesByBooleanAttributeFalse() throws Exception {
        R2dbcQueryMethod queryMethod = getQueryMethod("findAllByActiveFalse");
        PartTreeR2dbcQuery r2dbcQuery = new PartTreeR2dbcQuery(queryMethod, databaseClient, r2dbcConverter,
                dataAccessStrategy);
        RelationalParametersParameterAccessor accessor = getAccessor(queryMethod, new Object[0]);
        BindableQuery bindableQuery = r2dbcQuery.createQuery(accessor);
        String expectedSql = "SELECT " + ALL_FIELDS + " FROM " + TABLE + " WHERE " + TABLE + ".active = FALSE";
        assertThat(bindableQuery.get()).isEqualTo(expectedSql);
    }

    @Test
    public void createsQueryToFindAllEntitiesByStringAttributeIgnoringCase() throws Exception {
        R2dbcQueryMethod queryMethod = getQueryMethod("findAllByFirstNameIgnoreCase", String.class);
        PartTreeR2dbcQuery r2dbcQuery = new PartTreeR2dbcQuery(queryMethod, databaseClient, r2dbcConverter,
                dataAccessStrategy);
        RelationalParametersParameterAccessor accessor = getAccessor(queryMethod, new Object[]{"John"});
        BindableQuery bindableQuery = r2dbcQuery.createQuery(accessor);
        String expectedSql = "SELECT " + ALL_FIELDS + " FROM " + TABLE + " WHERE UPPER(" + TABLE + ".first_name) = UPPER(?)";
        assertThat(bindableQuery.get()).isEqualTo(expectedSql);
    }

    private R2dbcQueryMethod getQueryMethod(String methodName, Class<?>... parameterTypes) throws Exception {
        Method method = UserRepository.class.getMethod(methodName, parameterTypes);
        return new R2dbcQueryMethod(method, new DefaultRepositoryMetadata(UserRepository.class),
                new SpelAwareProxyProjectionFactory(), mappingContext);
    }

    private RelationalParametersParameterAccessor getAccessor(R2dbcQueryMethod queryMethod, Object[] values) {
        return new RelationalParametersParameterAccessor(queryMethod, values);
    }

    private interface UserRepository extends Repository<User, Long> {
        Flux<User> findAllByFirstName(String firstName);

        Flux<User> findAllByLastNameAndFirstName(String lastName, String firstName);

        Flux<User> findAllByLastNameOrFirstName(String lastName, String firstName);

        Mono<Boolean> existsByFirstName(String firstName);

        Flux<User> findAllByDateOfBirthBetween(Date from, Date to);

        Flux<User> findAllByAgeLessThan(Integer age);

        Flux<User> findAllByAgeLessThanEqual(Integer age);

        Flux<User> findAllByAgeGreaterThan(Integer age);

        Flux<User> findAllByAgeGreaterThanEqual(Integer age);

        Flux<User> findAllByDateOfBirthAfter(Date date);

        Flux<User> findAllByDateOfBirthBefore(Date date);

        Flux<User> findAllByAgeIsNull();

        Flux<User> findAllByAgeIsNotNull();

        Flux<User> findAllByFirstNameLike(String like);

        Flux<User> findAllByFirstNameNotLike(String like);

        Flux<User> findAllByFirstNameStartingWith(String starting);

        Flux<User> findAllByFirstNameEndingWith(String ending);

        Flux<User> findAllByFirstNameContaining(String containing);

        Flux<User> findAllByAgeOrderByLastNameDesc(Integer age);

        Flux<User> findAllByLastNameNot(String lastName);

        Flux<User> findAllByAgeIn(Collection<Integer> ages);

        Flux<User> findAllByAgeNotIn(Collection<Integer> ages);

        Flux<User> findAllByActiveTrue();

        Flux<User> findAllByActiveFalse();

        Flux<User> findAllByFirstNameIgnoreCase(String firstName);
    }

    @Table("users")
    @Data
    private static class User {
        @Id
        private Long id;
        private String firstName;
        private String lastName;
        private Date dateOfBirth;
        private Integer age;
        private Boolean active;
    }
}
