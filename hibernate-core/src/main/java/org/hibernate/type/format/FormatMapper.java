/*
 * Hibernate, Relational Persistence for Idiomatic Java
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later.
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.hibernate.type.format;

import org.hibernate.Incubating;
import org.hibernate.type.descriptor.WrapperOptions;
import org.hibernate.type.descriptor.java.JavaType;

/**
 * A mapper for mapping objects to and from a format.
 * <ul>
 * <li>A {@code FormatMapper} for JSON may be selected using the configuration
 *     property {@value org.hibernate.cfg.AvailableSettings#JSON_FORMAT_MAPPER}.
 * <li>A {@code FormatMapper} for XML may be selected using the configuration
 *     property {@value org.hibernate.cfg.AvailableSettings#XML_FORMAT_MAPPER}.
 * </ul>
 *
 *
 * @see org.hibernate.cfg.AvailableSettings#JSON_FORMAT_MAPPER
 * @see org.hibernate.cfg.AvailableSettings#XML_FORMAT_MAPPER
 *
 * @see org.hibernate.boot.spi.SessionFactoryOptions#getJsonFormatMapper()
 * @see org.hibernate.boot.spi.SessionFactoryOptions#getXmlFormatMapper()
 *
 * @see org.hibernate.type.descriptor.jdbc.JsonJdbcType
 * @see org.hibernate.type.descriptor.jdbc.XmlJdbcType
 *
 * @author Christian Beikov
 */
@Incubating
public interface FormatMapper {

	/**
	 * Deserializes an object from the character sequence.
	 */
	<T> T fromString(CharSequence charSequence, JavaType<T> javaType, WrapperOptions wrapperOptions);

	/**
	 * Serializes the object to a string.
	 */
	<T> String toString(T value, JavaType<T> javaType, WrapperOptions wrapperOptions);

	/**
	 * Checks if the given sourceType is supported for conversion.
	 */
	default boolean supportsSourceType(Class<?> sourceType){
		return false;
	}

	/**
	 * Checks if the given targetType is supported for conversion.
	 */
	default boolean supportsTargetType(Class<?> targetType){
		return false;
	}

	/**
	 * Writes the given value to the target using the specified JavaType and options.
	 */
	default <T> void writeToTarget(T value, JavaType<T> javaType, Object target, WrapperOptions options){}

	/**
	 * Reads a value from the source using the specified JavaType and options.
	 */
	default <T> T readFromSource(JavaType<T> javaType, Object source, WrapperOptions options){
		return null;
	}

}
