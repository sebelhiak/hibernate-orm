/*
 * Hibernate, Relational Persistence for Idiomatic Java
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later
 * See the lgpl.txt file in the root directory or http://www.gnu.org/licenses/lgpl-2.1.html
 */
package org.hibernate.orm.test.mapping.basic;

import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.cfg.AvailableSettings;
import org.hibernate.dialect.OracleDialect;
import org.hibernate.dialect.OracleJdbcHelper;
import org.hibernate.type.SqlTypes;

import org.hibernate.testing.orm.junit.DomainModel;
import org.hibernate.testing.orm.junit.RequiresDialect;
import org.hibernate.testing.orm.junit.ServiceRegistry;
import org.hibernate.testing.orm.junit.SessionFactory;
import org.hibernate.testing.orm.junit.SessionFactoryScope;
import org.hibernate.testing.orm.junit.Setting;
import org.hibernate.testing.orm.junit.SkipForDialect;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import org.mockito.ArgumentMatchers;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;

/**
 * @author Christian Beikov
 */
@RequiresDialect(OracleDialect.class)
@DomainModel(annotatedClasses = {
  OraclePolymorphicJsonTests.EntityWithJson.class,
  OraclePolymorphicJsonTests.EntityWithJsonA.class,
  OraclePolymorphicJsonTests.EntityWithJsonB.class
})
@SessionFactory
public abstract class OraclePolymorphicJsonTests {
  MockedStatic<OracleJdbcHelper> mockedStatic;
  @ServiceRegistry(settings = @Setting(name = AvailableSettings.JSON_FORMAT_MAPPER, value = "jsonb"))

  public static class JsonB extends OraclePolymorphicJsonTests {

    public JsonB() {
      mockedStatic = Mockito.mockStatic(OracleJdbcHelper.class);
      mockedStatic.when(() -> OracleJdbcHelper.loadJacksonExtension(
        (org.hibernate.service.ServiceRegistry) ArgumentMatchers.any(ServiceRegistry.class))).thenReturn(false);
    }
    @AfterAll
    public void teardown() {
      mockedStatic.close();
    }
  }

  @ServiceRegistry(settings = @Setting(name = AvailableSettings.JSON_FORMAT_MAPPER, value = "jackson"))
  public static class Jackson extends OraclePolymorphicJsonTests {

    public Jackson() {
    }
  }

  @BeforeEach
  public void setup(SessionFactoryScope scope) {
    scope.inTransaction(
      (session) -> {
        session.persist( new EntityWithJsonA( 1, new PropertyA( "e1" ) ) );
        session.persist( new EntityWithJsonB( 2, new PropertyB( 123 ) ) );
      }
    );
  }

  @AfterEach
  public void tearDown(SessionFactoryScope scope) {
    scope.inTransaction(
      (session) -> {
        session.remove( session.find( EntityWithJson.class, 1 ) );
        session.remove( session.find( EntityWithJson.class, 2 ) );
      }
    );
  }

  @Test
  public void verifyReadWorks(SessionFactoryScope scope) {
    scope.inTransaction(
      (session) -> {
        EntityWithJson entityWithJsonA = session.find( EntityWithJson.class, 1 );
        EntityWithJson entityWithJsonB = session.find( EntityWithJson.class, 2 );
        assertThat( entityWithJsonA, instanceOf( EntityWithJsonA.class ) );
        assertThat( entityWithJsonB, instanceOf( EntityWithJsonB.class ) );
        assertThat( ( (EntityWithJsonA) entityWithJsonA ).property.value, is( "e1" ) );
        assertThat( ( (EntityWithJsonB) entityWithJsonB ).property.value, is( 123 ) );
      }
    );
  }

  @Entity(name = "EntityWithJson")
  @Table(name = "EntityWithJson")
  public static abstract class EntityWithJson {
    @Id
    private Integer id;

    public EntityWithJson() {
    }

    public EntityWithJson(Integer id) {
      this.id = id;
    }
  }

  @Entity(name = "EntityWithJsonA")
  public static class EntityWithJsonA extends EntityWithJson {

    @JdbcTypeCode( SqlTypes.JSON )
    private PropertyA property;

    public EntityWithJsonA() {
    }

    public EntityWithJsonA(Integer id, PropertyA property) {
      super( id );
      this.property = property;
    }
  }

  @Entity(name = "EntityWithJsonB")
  public static class EntityWithJsonB extends EntityWithJson {

    @JdbcTypeCode( SqlTypes.JSON )
    private PropertyB property;

    public EntityWithJsonB() {
    }

    public EntityWithJsonB(Integer id, PropertyB property) {
      super( id );
      this.property = property;
    }
  }

  public static class PropertyA {
    private String value;

    public PropertyA() {
    }

    public PropertyA(String value) {
      this.value = value;
    }

    public String getValue() {
      return value;
    }

    public void setValue(String value) {
      this.value = value;
    }
  }

  public static class PropertyB {
    private int value;

    public PropertyB() {
    }

    public PropertyB(int value) {
      this.value = value;
    }

    public int getValue() {
      return value;
    }

    public void setValue(int value) {
      this.value = value;
    }
  }
}
