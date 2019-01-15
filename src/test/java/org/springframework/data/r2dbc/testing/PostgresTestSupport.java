package org.springframework.data.r2dbc.testing;

import io.r2dbc.postgresql.PostgresqlConnectionConfiguration;
import io.r2dbc.postgresql.PostgresqlConnectionFactory;
import io.r2dbc.spi.ConnectionFactory;

import javax.sql.DataSource;

import org.postgresql.ds.PGSimpleDataSource;
import org.springframework.data.r2dbc.testing.ExternalDatabase.ProvidedDatabase;
import org.testcontainers.containers.PostgreSQLContainer;

/**
 * Utility class for testing against Postgres.
 *
 * @author Mark Paluch
 * @author Jens Schauder
 */
public class PostgresTestSupport {

	private static final PostgreSQLContainer POSTGRESQL_CONTAINER = new PostgreSQLContainer();

	public static String CREATE_TABLE_LEGOSET = "CREATE TABLE legoset (\n" //
			+ "    id          integer CONSTRAINT id PRIMARY KEY,\n" //
			+ "    name        varchar(255) NOT NULL,\n" //
			+ "    manual      integer NULL\n" //
			+ ");";

	public static String CREATE_TABLE_LEGOSET_WITH_ID_GENERATION = "CREATE TABLE legoset (\n" //
			+ "    id          serial CONSTRAINT id PRIMARY KEY,\n" //
			+ "    name        varchar(255) NOT NULL,\n" //
			+ "    manual      integer NULL\n" //
			+ ");";

	public static String INSERT_INTO_LEGOSET = "INSERT INTO legoset (id, name, manual) VALUES($1, $2, $3)";

	/**
	 * Returns a database either hosted locally at {@code postgres:@localhost:5432/postgres} or running inside Docker.
	 *
	 * @return information about the database. Guaranteed to be not {@literal null}.
	 */
	public static ExternalDatabase database() {

		ExternalDatabase local = local();
		if (local.checkValidity()) {
			return local;
		} else {
			return testContainer();
		}
	}

	/**
	 * Returns a locally provided database at {@code postgres:@localhost:5432/postgres}.
	 */
	private static ExternalDatabase local() {

		return ProvidedDatabase.builder() //
				.hostname("localhost") //
				.port(5432) //
				.database("postgres") //
				.username("postgres") //
				.password("").build();
	}

	/**
	 * Returns a database provided via Testcontainers.
	 */
	private static ExternalDatabase testContainer() {

		POSTGRESQL_CONTAINER.start();

		return ProvidedDatabase.builder() //
				.hostname("localhost") //
				.port(POSTGRESQL_CONTAINER.getFirstMappedPort()) //
				.database(POSTGRESQL_CONTAINER.getDatabaseName()) //
				.username(POSTGRESQL_CONTAINER.getUsername()) //
				.password(POSTGRESQL_CONTAINER.getPassword()).build();
	}

	/**
	 * Creates a new {@link ConnectionFactory} configured from the {@link ExternalDatabase}..
	 */
	public static ConnectionFactory createConnectionFactory(ExternalDatabase database) {

		return new PostgresqlConnectionFactory(PostgresqlConnectionConfiguration.builder() //
				.host(database.getHostname()) //
				.database(database.getDatabase()) //
				.port(database.getPort()) //
				.username(database.getUsername()) //
				.password(database.getPassword()) //
				.build());
	}

	/**
	 * Creates a new {@link DataSource} configured from the {@link ExternalDatabase}.
	 */
	public static DataSource createDataSource(ExternalDatabase database) {

		PGSimpleDataSource dataSource = new PGSimpleDataSource();

		dataSource.setUser(database.getUsername());
		dataSource.setPassword(database.getPassword());
		dataSource.setDatabaseName(database.getDatabase());
		dataSource.setServerName(database.getHostname());
		dataSource.setPortNumber(database.getPort());

		return dataSource;
	}
}
