/*
 * Hibernate, Relational Persistence for Idiomatic Java
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later
 * See the lgpl.txt file in the root directory or http://www.gnu.org/licenses/lgpl-2.1.html
 */
package org.hibernate.dialect;
import oracle.jdbc.jackson.json.OsonFactory;
import oracle.jdbc.jackson.json.OsonGenerator;
import oracle.jdbc.jackson.json.OsonParser;
import oracle.sql.json.OracleJsonDatum;
import org.hibernate.metamodel.mapping.EmbeddableMappingType;
import org.hibernate.type.SqlTypes;
import org.hibernate.type.descriptor.ValueBinder;
import org.hibernate.type.descriptor.ValueExtractor;
import org.hibernate.type.descriptor.WrapperOptions;
import org.hibernate.type.descriptor.java.JavaType;
import org.hibernate.type.descriptor.jdbc.*;
import java.io.*;
import java.sql.CallableStatement;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Specialized type mapping for {@code OSON} and the JSON SQL data type for Oracle.
 */
public class OracleOsonJacksonJdbcType extends OracleJsonJdbcType {
	/**
	 * Singleton access
	 */
	public static final OracleOsonJacksonJdbcType INSTANCE = new OracleOsonJacksonJdbcType( null );
	private OracleOsonJacksonJdbcType(EmbeddableMappingType embeddableMappingType) {
			super(embeddableMappingType);
	}

	@Override
	public int getJdbcTypeCode() {
		return SqlTypes.JSON;
	}

	@Override
	public String toString() {
		return "OracleOsonJacksonJdbcType";
	}

	private <X> InputStream toOson(X value, JavaType<X> javaType, WrapperOptions options) throws SQLException {

		/*
		Since we are using setBinaryStream in the doBind function, we are creating a PipedInputStream and a PipedOutputStream
		The generator writes to the PipedOutputStream and we return the PipedInputStream
		 */
		PipedInputStream in = new PipedInputStream();
		try {
			PipedOutputStream out = new PipedOutputStream(in);

			// new thread to avoid deadlocking the main thread
			new Thread(() -> {
				final OsonGenerator osonGenerator ;
				try  {
					osonGenerator = new OsonFactory().createGenerator(out);
					options.getSessionFactory().getFastSessionServices().getJsonFormatMapper().writeToTarget(value,javaType,osonGenerator,options);
					osonGenerator.close();
					out.close();
				}
				catch (IOException e) {
					throw new IllegalArgumentException(e.getMessage());

				}
			} ).start();
		}
		catch(IOException e) {
			throw new SQLException(e.getMessage());
		}
		return in;

	}

	@Override
	public <X> ValueBinder<X> getBinder(JavaType<X> javaType) {
		return new BasicBinder<>(javaType, this) {
			@Override
			protected void doBind(PreparedStatement st, X value, int index, WrapperOptions options) throws SQLException {
				InputStream osonObject = toOson(value,javaType,options);
				st.setBinaryStream(index, osonObject);
			}
			@Override
			protected void doBind(CallableStatement st, X value, String name, WrapperOptions options) throws SQLException {
				InputStream osonObject = toOson(value,javaType,options);
				st.setBinaryStream(name, osonObject);
			}
		};
	}

	@Override
	public <X> ValueExtractor<X> getExtractor(JavaType<X> javaType) {
		return new BasicExtractor<>( javaType, this ) {
			@Override
			protected X doExtract(ResultSet rs, int paramIndex, WrapperOptions options) throws SQLException {
				return fromOson((rs.getObject(paramIndex,OracleJsonDatum.class).shareBytes()) ,javaType,options ) ;
			}

			@Override
			protected X doExtract(CallableStatement statement, int index, WrapperOptions options) throws SQLException {
				return fromOson((statement.getObject(index,OracleJsonDatum.class).shareBytes()) ,javaType,options );
			}

			@Override
			protected X doExtract(CallableStatement statement, String name, WrapperOptions options) throws SQLException {
				return fromOson((statement.getObject(name,OracleJsonDatum.class).shareBytes()) ,javaType,options );
			}
			private X fromOson( byte[] osonBytes, JavaType<X> javaType, WrapperOptions options) {
				if ( osonBytes == null ) {
					return null;
				}
				OsonParser osonParser = new OsonFactory().createParser(osonBytes);
				return (options.getSessionFactory().getFastSessionServices().getJsonFormatMapper()).readFromSource(javaType,
						osonParser,
						options
				);
			}
		};
	}
}
