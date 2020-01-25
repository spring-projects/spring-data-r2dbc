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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

/**
 * @author Roman Chigvintsev
 */
@RunWith(MockitoJUnitRunner.class)
public class PartTreeR2dbcQueryIntegrationTests {
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
        BindableQuery bindableQuery = r2dbcQuery.createQuery(getAccessor(queryMethod, new Object[]{"Matthews"}));
        assertThat(bindableQuery.get())
                .isEqualTo("SELECT users.id, users.first_name FROM users WHERE users.first_name = ?");
    }

    @Test
    public void createsQueryWithIsNullCondition() throws Exception {
        R2dbcQueryMethod queryMethod = getQueryMethod("findAllByFirstName", String.class);
        PartTreeR2dbcQuery r2dbcQuery = new PartTreeR2dbcQuery(queryMethod, databaseClient, r2dbcConverter,
                dataAccessStrategy);
        BindableQuery bindableQuery = r2dbcQuery.createQuery((getAccessor(queryMethod, new Object[]{null})));
        assertThat(bindableQuery.get())
                .isEqualTo("SELECT users.id, users.first_name FROM users WHERE users.first_name IS NULL");
    }

    @Test
    public void createsQueryWithLimitForExistsProjection() throws Exception {
        R2dbcQueryMethod queryMethod = getQueryMethod("existsByFirstName", String.class);
        PartTreeR2dbcQuery r2dbcQuery = new PartTreeR2dbcQuery(queryMethod, databaseClient, r2dbcConverter,
                dataAccessStrategy);
        BindableQuery query = r2dbcQuery.createQuery((getAccessor(queryMethod, new Object[]{"Matthews"})));
        assertThat(query.get())
                .isEqualTo("SELECT users.id FROM users WHERE users.first_name = ? LIMIT 1");
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

        Mono<Boolean> existsByFirstName(String firstName);
    }

    @Table("users")
    @Data
    private static class User {
        @Id
        private Long id;
        private String firstName;
    }
}
