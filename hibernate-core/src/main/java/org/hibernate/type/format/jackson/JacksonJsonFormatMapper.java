/*
 * Hibernate, Relational Persistence for Idiomatic Java
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later.
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.hibernate.type.format.jackson;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import oracle.jdbc.jackson.json.OsonParser;
import oracle.jdbc.jackson.json.OsonGenerator;
import org.hibernate.type.format.FormatMapper;
import org.hibernate.type.descriptor.WrapperOptions;
import org.hibernate.type.descriptor.java.JavaType;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;

/**
 * @author Christian Beikov
 */
public  class JacksonJsonFormatMapper implements FormatMapper {

	public static final String SHORT_NAME = "jackson";

	private final ObjectMapper objectMapper;

	public JacksonJsonFormatMapper() {
		this(new ObjectMapper().findAndRegisterModules());
	}

	public JacksonJsonFormatMapper(ObjectMapper objectMapper) {
		this.objectMapper = objectMapper;
	}

	@Override
	public <T> T fromString(CharSequence charSequence, JavaType<T> javaType, WrapperOptions wrapperOptions) {
		if ( javaType.getJavaType() == String.class || javaType.getJavaType() == Object.class ) {
			return (T) charSequence.toString();
		}
		try {
			return objectMapper.readValue( charSequence.toString(), objectMapper.constructType( javaType.getJavaType() ) );
		}
		catch (JsonProcessingException e) {
			throw new IllegalArgumentException( "Could not deserialize string to java type: " + javaType, e );
		}
	}

	@Override
	public <T> String toString(T value, JavaType<T> javaType, WrapperOptions wrapperOptions) {
		if ( javaType.getJavaType() == String.class || javaType.getJavaType() == Object.class ) {
			return (String) value;
		}
		try {
			return objectMapper.writerFor( objectMapper.constructType( javaType.getJavaType() ) )
					.writeValueAsString( value );
		}
		catch (JsonProcessingException e) {
			throw new IllegalArgumentException( "Could not serialize object of java type: " + javaType, e );
		}
	}

	@Override
	public boolean supportsSourceType(Class<?> sourceType) {
		return sourceType == OsonParser.class || sourceType == JsonParser.class;
	}

	@Override
	public boolean supportsTargetType(Class<?> targetType) {
		return targetType == OsonGenerator.class || targetType == JsonGenerator.class;
	}

	@Override
	public <T> void writeToTarget(T value, JavaType<T> javaType, Object target, WrapperOptions options) {
		if( !supportsTargetType(target.getClass()) ) {
			throw new IllegalArgumentException( "Unsupported target type");
		}
		try {
			objectMapper.writeValue( (JsonGenerator) target, value );
		}
		catch (IOException e){
			throw new IllegalArgumentException( "Could not convert object of java type: " + javaType+"to Oson Bytes", e );
		}
	}

	@Override
	public <T> T readFromSource(JavaType<T> javaType, Object source, WrapperOptions options)  {
		if (!supportsSourceType(source.getClass())) {
			throw new IllegalArgumentException( "Unsupported target type");
		}
		try {
			return objectMapper.readValue((JsonParser) source, objectMapper.constructType(javaType.getJavaType()));
		}
		catch (IOException e){
			throw new IllegalArgumentException( "Could not parse object of java type: " + javaType, e );
		}
	}
}
