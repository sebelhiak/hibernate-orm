/*
 * Hibernate, Relational Persistence for Idiomatic Java
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later
 * See the lgpl.txt file in the root directory or http://www.gnu.org/licenses/lgpl-2.1.html
 */
package org.hibernate.dialect;

import java.lang.reflect.ParameterizedType;
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


import com.fasterxml.jackson.databind.type.TypeFactory;
import oracle.jdbc.OracleTypes;
import oracle.jdbc.jackson.oson.provider.JacksonOsonConverter;
import oracle.sql.json.OracleJsonDatum;

/**
 * Specialized type mapping for {@code JSON} and the JSON SQL data type for Oracle.
 *
 * @author Christian Beikov
 */
public class OracleOsonJdbcType extends OracleJsonBlobJdbcType {
  /**
   * Singleton access
   */
  public static final OracleOsonJdbcType INSTANCE = new OracleOsonJdbcType(
    null);
  private final EmbeddableMappingType embeddableMappingType;

  private OracleOsonJdbcType(EmbeddableMappingType embeddableMappingType) {
    super(embeddableMappingType);
    this.embeddableMappingType = embeddableMappingType;
  }

  @Override
  public String toString() {
    return "OracleOsonJdbcType";
  }

  @Override
  public AggregateJdbcType resolveAggregateJdbcType(
    EmbeddableMappingType mappingType, String sqlType,
    RuntimeModelCreationContext creationContext) {
    return new OracleOsonJdbcType(mappingType);
  }

  @Override
  public String getCheckCondition(
    String columnName, JavaType<?> javaType,
    BasicValueConverter<?, ?> converter, Dialect dialect) {
    // No check constraint necessary, because the JSON DDL type is already OSON encoded
    return null;
  }

  @Override
  public <X> ValueBinder<X> getBinder(JavaType<X> javaType) {
    return new BasicBinder<>(javaType, this) {
      @Override
      protected void doBind(
        PreparedStatement st, X value, int index, WrapperOptions options)
        throws SQLException {
        if(embeddableMappingType != null){
          final String json=  JsonHelper.toString( embeddableMappingType, value, options );
          st.setBytes( index, json.getBytes( StandardCharsets.UTF_8 ) );
        }
        else {
          st.setObject(index, value, OracleTypes.JSON);
        }
      }


      @Override
      protected void doBind(
        CallableStatement st, X value, String name, WrapperOptions options)
        throws SQLException {
        if(embeddableMappingType != null){
          final String json=  JsonHelper.toString( embeddableMappingType, value, options );
          st.setBytes( name, json.getBytes( StandardCharsets.UTF_8 ) );
        }
        else {
          st.setObject(name, value, OracleTypes.JSON);
        }

      }
    };
  }

  @Override
  public <X> ValueExtractor<X> getExtractor(JavaType<X> javaType) {
    return new BasicExtractor<>(javaType, this) {
      @Override
      protected X doExtract(
        ResultSet rs, int paramIndex, WrapperOptions options)
        throws SQLException {
        if (embeddableMappingType != null) {
          byte[] json = rs.getBytes(paramIndex);
          if (json == null)
            return null;
          return JsonHelper.fromString(
            embeddableMappingType,
            new String(json, StandardCharsets.UTF_8),
            javaType.getJavaTypeClass() != Object[].class,
            options
          );
        }
        if ((javaType.getJavaType() instanceof ParameterizedType)) {
          OracleJsonDatum oracleJasonDatum = rs.getObject(
            paramIndex, OracleJsonDatum.class);
          if (oracleJasonDatum == null) {
            return null;
          }
          byte[] osonBytes = oracleJasonDatum.shareBytes();

            return (X) JacksonOsonConverter.convertValue(osonBytes, TypeFactory.defaultInstance().constructType(javaType.getJavaType()));
        }
        return rs.getObject(paramIndex,getJavaType().getJavaTypeClass());
      }

      @Override
      protected X doExtract(
        CallableStatement statement, int index, WrapperOptions options)
        throws SQLException {

        if (embeddableMappingType != null) {
          byte[] json = statement.getBytes(index);
          if (json == null)
            return null;
          return JsonHelper.fromString(
            embeddableMappingType,
            new String(json, StandardCharsets.UTF_8),
            javaType.getJavaTypeClass() != Object[].class,
            options
          );
        }
        if ((javaType.getJavaType() instanceof ParameterizedType)) {
          OracleJsonDatum oracleJasonDatum = statement.getObject(index, OracleJsonDatum.class);
          if (oracleJasonDatum == null) {
            return null;
          }
          byte[] osonBytes = oracleJasonDatum.shareBytes();
          return (X) JacksonOsonConverter.convertValue(osonBytes, TypeFactory.defaultInstance().constructType(javaType.getJavaType()));
        }
        return statement.getObject(index,getJavaType().getJavaTypeClass());
      }

      @Override
      protected X doExtract(
        CallableStatement statement, String name, WrapperOptions options)
        throws SQLException {
        if (embeddableMappingType != null) {
          byte[] json = statement.getBytes(name);
          if (json == null)
            return null;
          return JsonHelper.fromString(
            embeddableMappingType,
            new String(json, StandardCharsets.UTF_8),
            javaType.getJavaTypeClass() != Object[].class,
            options
          );
        }
        if ((javaType.getJavaType() instanceof ParameterizedType)) {
          OracleJsonDatum oracleJasonDatum = statement.getObject(name, OracleJsonDatum.class);
          if (oracleJasonDatum == null) {
            return null;
          }
          byte[] osonBytes = oracleJasonDatum.shareBytes();
          return (X) JacksonOsonConverter.convertValue(osonBytes, TypeFactory.defaultInstance().constructType(javaType.getJavaType()));
        }
        return statement.getObject(name,getJavaType().getJavaTypeClass());

      }
    };
  }
}
