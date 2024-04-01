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
import org.hibernate.type.descriptor.WrapperOptions;
import org.hibernate.type.descriptor.java.JavaType;
import java.io.*;
import java.sql.CallableStatement;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Specialized type mapping for {@code OSON} and the JSON SQL data type for Oracle.
 */
public class OracleOsonJacksonHelper  {
	/**
	 * Singleton access
	 */

	public static <X> X doExtract(ResultSet rs, int paramIndex, JavaType<X> javaType, WrapperOptions options) throws SQLException {
		OracleJsonDatum obj = rs.getObject( paramIndex, OracleJsonDatum.class );
		if ( obj == null ) {
			return null;
		}
		return fromOson( obj.shareBytes(), javaType, options );
	}
	public static <X> X doExtract(CallableStatement statement, int paramIndex, JavaType<X> javaType, WrapperOptions options) throws SQLException {
		OracleJsonDatum obj = statement.getObject( paramIndex, OracleJsonDatum.class );
		if ( obj == null ) {
			return null;
		}
		return fromOson( obj.shareBytes(), javaType, options );
	}
	public static <X> X doExtract(CallableStatement statement, String name, JavaType<X> javaType, WrapperOptions options)
			throws SQLException {
		OracleJsonDatum obj = statement.getObject( name, OracleJsonDatum.class );
		if ( obj == null ) {
			return null;
		}
		return fromOson( obj.shareBytes(), javaType, options );
	}
	private static <X>  X fromOson( byte[] osonBytes, JavaType<X> javaType, WrapperOptions options) {
		if ( osonBytes == null ) {
			return null;
		}
		OsonParser osonParser = new OsonFactory().createParser( osonBytes);
		return (options.getSessionFactory().getFastSessionServices().getJsonFormatMapper()).readFromSource(javaType,
																										   osonParser,
																										   options
		);
	}
	protected static <X> void doBind(CallableStatement st, X value, String name,JavaType<X> javaType, WrapperOptions options)
			throws SQLException {
		InputStream osonObject = toOson( value, javaType, options );
		st.setBinaryStream( name, osonObject);
	}
	protected static <X> void doBind(PreparedStatement st, X value, int index,JavaType<X> javaType, WrapperOptions options)
			throws SQLException {
		InputStream osonObject = toOson( value, javaType, options );
		st.setBinaryStream( index, osonObject);
	}
	protected static <X> InputStream toOson(X value, JavaType<X> javaType, WrapperOptions options) throws SQLException {

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



}
