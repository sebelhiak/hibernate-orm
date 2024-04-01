/*
 * Hibernate, Relational Persistence for Idiomatic Java
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later
 * See the lgpl.txt file in the root directory or http://www.gnu.org/licenses/lgpl-2.1.html
 */
package org.hibernate.dialect;

import java.nio.charset.StandardCharsets;
import java.sql.CallableStatement;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.hibernate.metamodel.mapping.EmbeddableMappingType;
import org.hibernate.metamodel.spi.RuntimeModelCreationContext;
import org.hibernate.type.descriptor.ValueBinder;
import org.hibernate.type.descriptor.ValueExtractor;
import org.hibernate.type.descriptor.WrapperOptions;
import org.hibernate.type.descriptor.converter.spi.BasicValueConverter;
import org.hibernate.type.descriptor.java.JavaType;
import org.hibernate.type.descriptor.jdbc.AggregateJdbcType;
import org.hibernate.type.descriptor.jdbc.BasicBinder;
import org.hibernate.type.descriptor.jdbc.BasicExtractor;
import org.hibernate.type.descriptor.jdbc.OracleJsonBlobJdbcType;
import org.hibernate.type.format.FormatMapper;
import org.hibernate.type.format.jackson.JacksonJsonFormatMapper;

/**
 * Specialized type mapping for {@code JSON} and the JSON SQL data type for Oracle.
 *
 * @author Christian Beikov
 */
public class OracleJsonJdbcType extends OracleJsonBlobJdbcType {
	/**
	 * Singleton access
	 */
	public static final OracleJsonJdbcType INSTANCE = new OracleJsonJdbcType( null );

	private OracleJsonJdbcType(EmbeddableMappingType embeddableMappingType) {
		super( embeddableMappingType );
	}

	private FormatMapper getFormatMapper(WrapperOptions options){
		return options.getSessionFactory().getFastSessionServices().getJsonFormatMapper();
	}
	@Override
	public String toString() {
		return "OracleJsonJdbcType";
	}

	@Override
	public AggregateJdbcType resolveAggregateJdbcType(
			EmbeddableMappingType mappingType,
			String sqlType,
			RuntimeModelCreationContext creationContext) {
		return new OracleJsonJdbcType( mappingType );
	}

	@Override
	public String getCheckCondition(String columnName, JavaType<?> javaType, BasicValueConverter<?, ?> converter, Dialect dialect) {
		// No check constraint necessary, because the JSON DDL type is already OSON encoded
		return null;
	}
	@Override
	public <X> ValueBinder<X> getBinder(JavaType<X> javaType) {
		return new BasicBinder<>( javaType, this ) {
			@Override
			protected void doBind(PreparedStatement st, X value, int index, WrapperOptions options)
					throws SQLException {
				FormatMapper formatMapper = getFormatMapper( options );
				if(formatMapper instanceof JacksonJsonFormatMapper ){
					OracleOsonJacksonHelper.doBind( st, value, index,getJavaType(), options );
				}
				else {
					final String json = OracleJsonBlobJdbcType.INSTANCE.toString(
							value,
							getJavaType(),
							options
					);
					st.setBytes( index, json.getBytes( StandardCharsets.UTF_8 ) );
				}

			}

			@Override
			protected void doBind(CallableStatement st, X value, String name, WrapperOptions options)
					throws SQLException {
					FormatMapper formatMapper = getFormatMapper( options );
					if(formatMapper instanceof JacksonJsonFormatMapper ){
						OracleOsonJacksonHelper.doBind( st, value, name,getJavaType(), options );
					}
					else {
						final String json = OracleJsonBlobJdbcType.INSTANCE.toString(
								value,
								getJavaType(),
								options
						);
						st.setBytes( name, json.getBytes( StandardCharsets.UTF_8 ) );
					}

				}
		};
	}
	@Override
	public <X> ValueExtractor<X> getExtractor(JavaType<X> javaType) {
		return new BasicExtractor<>( javaType, this ) {
			@Override
			protected X doExtract(ResultSet rs, int paramIndex, WrapperOptions options) throws SQLException {
				FormatMapper formatMapper = getFormatMapper( options );
				if ( formatMapper instanceof JacksonJsonFormatMapper ) {
					return OracleOsonJacksonHelper.doExtract( rs, paramIndex,getJavaType(), options );
				}
				return fromString( rs.getBytes( paramIndex ), options );

			}

			@Override
			protected X doExtract(CallableStatement statement, int index, WrapperOptions options) throws SQLException {
				FormatMapper formatMapper = getFormatMapper( options );
				if ( formatMapper instanceof JacksonJsonFormatMapper ) {
					return OracleOsonJacksonHelper.doExtract( statement, index,getJavaType(), options );
				}
				return fromString( statement.getBytes( index ), options );
			}

			@Override
			protected X doExtract(CallableStatement statement, String name, WrapperOptions options) throws SQLException {
				FormatMapper formatMapper = getFormatMapper( options );
				if ( formatMapper instanceof JacksonJsonFormatMapper ) {
					return OracleOsonJacksonHelper.doExtract( statement, name,getJavaType(), options );
				}
				return fromString( statement.getBytes( name ), options );
			}

			private X fromString(byte[] json, WrapperOptions options) throws SQLException {
				if ( json == null ) {
					return null;
				}
				return OracleJsonBlobJdbcType.INSTANCE.fromString(
						new String( json, StandardCharsets.UTF_8 ),
						getJavaType(),
						options
				);
			}
		};
	}
}
