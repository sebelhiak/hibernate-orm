/*
 * Hibernate, Relational Persistence for Idiomatic Java
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later
 * See the lgpl.txt file in the root directory or http://www.gnu.org/licenses/lgpl-2.1.html
 */
package org.hibernate.orm.test.mapping.basic;

import java.util.List;
import java.util.Map;

import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.cfg.AvailableSettings;
import org.hibernate.dialect.OracleDialect;
import org.hibernate.dialect.OracleJdbcHelper;
import org.hibernate.metamodel.mapping.internal.BasicAttributeMapping;
import org.hibernate.metamodel.spi.MappingMetamodelImplementor;
import org.hibernate.persister.entity.EntityPersister;
import org.hibernate.type.SqlTypes;
import org.hibernate.type.descriptor.jdbc.JdbcType;
import org.hibernate.type.descriptor.jdbc.spi.JdbcTypeRegistry;

import org.hibernate.testing.orm.junit.DomainModel;
import org.hibernate.testing.orm.junit.JiraKey;
import org.hibernate.testing.orm.junit.RequiresDialect;
import org.hibernate.testing.orm.junit.ServiceRegistry;
import org.hibernate.testing.orm.junit.SessionFactory;
import org.hibernate.testing.orm.junit.SessionFactoryScope;
import org.hibernate.testing.orm.junit.Setting;
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
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.isA;
import static org.hamcrest.Matchers.notNullValue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;

/**
 * @author Christian Beikov
 */
@RequiresDialect(OracleDialect.class)
@DomainModel(annotatedClasses = JsonOracleMappingTests.EntityWithJson.class)
@SessionFactory
public abstract class JsonOracleMappingTests {
  MockedStatic<OracleJdbcHelper> mockedStatic;
  @ServiceRegistry(settings = @Setting(name = AvailableSettings.JSON_FORMAT_MAPPER, value = "jackson"))
  public static class Jackson extends JsonOracleMappingTests {

    public Jackson() {
      super( false );
    }
  }

  @ServiceRegistry(settings = @Setting(name = AvailableSettings.JSON_FORMAT_MAPPER, value = "jsonb"))
  public static class JsonB extends JsonOracleMappingTests{

    public JsonB() {
      super( true );
      mockedStatic = Mockito.mockStatic(OracleJdbcHelper.class);
      mockedStatic.when(() -> OracleJdbcHelper.loadJacksonExtension(
        (org.hibernate.service.ServiceRegistry) ArgumentMatchers.any(ServiceRegistry.class))).thenReturn(false);

    }
    @AfterAll
    public void teardown() {
      mockedStatic.close();
    }
  }


  private final Map<String, String> stringMap;
  private final Map<StringNode, StringNode> objectMap;
  private final List<StringNode> list;
  private final String json;

  protected JsonOracleMappingTests(boolean supportsObjectMapKey) {
    this.stringMap = Map.of( "name", "ABC" );
    this.objectMap = supportsObjectMapKey ? Map.of(
      new StringNode( "name" ),
      new StringNode( "ABC" )
    ) : null;
    this.list = List.of( new StringNode( "ABC" ) );
    this.json = "{\"name\":\"abc\"}";
  }

  @BeforeEach
  public void setup(SessionFactoryScope scope) {

    scope.inTransaction(
      (session) -> {
        session.persist( new EntityWithJson( 1, stringMap, objectMap, list, json ) );
      }
    );
  }

  @AfterEach
  public void tearDown(SessionFactoryScope scope) {

    scope.inTransaction(
      (session) -> {
        session.remove( session.find( EntityWithJson.class, 1 ) );
      }
    );
  }
  @Test
  public void verifyMappings(SessionFactoryScope scope) {

      final MappingMetamodelImplementor mappingMetamodel =
        scope.getSessionFactory().getRuntimeMetamodels().getMappingMetamodel();
      final EntityPersister entityDescriptor =
        mappingMetamodel.findEntityDescriptor(EntityWithJson.class);
      final JdbcTypeRegistry jdbcTypeRegistry =
        mappingMetamodel.getTypeConfiguration().getJdbcTypeRegistry();

      final BasicAttributeMapping stringMapAttribute =
        (BasicAttributeMapping) entityDescriptor.findAttributeMapping(
          "stringMap");
      final BasicAttributeMapping objectMapAttribute =
        (BasicAttributeMapping) entityDescriptor.findAttributeMapping(
          "objectMap");
      final BasicAttributeMapping listAttribute =
        (BasicAttributeMapping) entityDescriptor.findAttributeMapping("list");
      final BasicAttributeMapping jsonAttribute =
        (BasicAttributeMapping) entityDescriptor.findAttributeMapping(
          "jsonString");

      assertThat(
        stringMapAttribute.getJavaType().getJavaTypeClass(),
        equalTo(Map.class)
      );
      assertThat(
        objectMapAttribute.getJavaType().getJavaTypeClass(),
        equalTo(Map.class)
      );
      assertThat(
        listAttribute.getJavaType().getJavaTypeClass(), equalTo(List.class));
      assertThat(
        jsonAttribute.getJavaType().getJavaTypeClass(), equalTo(String.class));

      final JdbcType jsonType = jdbcTypeRegistry.getDescriptor(SqlTypes.JSON);
      assertThat(
        stringMapAttribute.getJdbcMapping().getJdbcType(),
        isA((Class<JdbcType>) jsonType.getClass())
      );
      assertThat(
        objectMapAttribute.getJdbcMapping().getJdbcType(),
        isA((Class<JdbcType>) jsonType.getClass())
      );
      assertThat(
        listAttribute.getJdbcMapping().getJdbcType(),
        isA((Class<JdbcType>) jsonType.getClass())
      );
      assertThat(
        jsonAttribute.getJdbcMapping().getJdbcType(),
        isA((Class<JdbcType>) jsonType.getClass())
      );

  }
  @Test
  public void verifyReadWorks(SessionFactoryScope scope) {

      scope.inTransaction((session) -> {
        EntityWithJson entityWithJson = session.find(EntityWithJson.class, 1);
        assertThat(entityWithJson.stringMap, is(stringMap));
        assertThat(entityWithJson.objectMap, is(objectMap));
        assertThat(entityWithJson.list, is(list));
      });

  }

  @Test
  @JiraKey( "HHH-16682" )
  public void verifyDirtyChecking(SessionFactoryScope scope) {
    scope.inTransaction(
      (session) -> {
        EntityWithJson entityWithJson = session.find( EntityWithJson.class, 1 );
        entityWithJson.stringMap.clear();
      }
    );
    scope.inTransaction(
      (session) -> {
        EntityWithJson entityWithJson = session.find( EntityWithJson.class, 1 );
        assertThat( entityWithJson.stringMap.isEmpty(), is( true ) );
      }
    );
  }


  @Entity(name = "EntityWithJson2")
  @Table(name = "EntityWithJson2")
  public static class EntityWithJson {
    @Id
    private Integer id;

    //tag::basic-json-example[]
    @JdbcTypeCode( SqlTypes.JSON )
    private Map<String, String> stringMap;
    //end::basic-json-example[]

    @JdbcTypeCode( SqlTypes.JSON )
    private Map<StringNode, StringNode> objectMap;

    @JdbcTypeCode( SqlTypes.JSON )
    private List<StringNode> list;

    @JdbcTypeCode( SqlTypes.JSON )
    private String jsonString;

    public EntityWithJson() {
    }

    public EntityWithJson(
      Integer id,
      Map<String, String> stringMap,
      Map<StringNode, StringNode> objectMap,
      List<StringNode> list,
      String jsonString) {
      this.id = id;
      this.stringMap = stringMap;
      this.objectMap = objectMap;
      this.list = list;
      this.jsonString = jsonString;
    }
  }

  public static class StringNode {
    private String string;

    public StringNode() {
    }

    public StringNode(String string) {
      this.string = string;
    }

    public String getString() {
      return string;
    }

    public void setString(String string) {
      this.string = string;
    }

    @Override
    public boolean equals(Object o) {
      if ( this == o ) {
        return true;
      }
      if ( o == null || getClass() != o.getClass() ) {
        return false;
      }

      StringNode that = (StringNode) o;

      return string != null ? string.equals( that.string ) : that.string == null;
    }

    @Override
    public int hashCode() {
      return string != null ? string.hashCode() : 0;
    }
  }
}
