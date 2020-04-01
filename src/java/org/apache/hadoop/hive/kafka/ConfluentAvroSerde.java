package org.apache.hadoop.hive.kafka;

import java.util.Properties;
import java.util.Map;
import java.util.HashMap;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.List;
import java.util.ListIterator;
import java.io.IOException;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.avro.AvroObjectInspectorGenerator;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.hadoop.conf.Configuration;
import io.confluent.kafka.streams.serdes.avro.GenericAvroSerde;
import org.apache.hadoop.hive.serde2.AbstractSerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.reflect.ReflectData;
import org.apache.hadoop.hive.serde2.avro.AvroGenericRecordWritable;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import org.apache.hadoop.hive.serde2.SerDeUtils;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.avro.AvroGenericRecordWritable;

public class ConfluentAvroSerde extends AbstractSerDe {

  private GenericAvroSerde inner;
  private ObjectInspector oi;
  private String topic;
  private Schema schema;
  private AvroDeserializer deser = null;
  private List<String> columnNames;
  private List<TypeInfo> columnTypes;

  private static final String LIST_COLUMN_COMMENTS = "columns.comments";
  private static final String TABLE_NAME = "name";
  private static final String TABLE_COMMENT = "comment";

  @Override
  public void initialize(Configuration configuration, Properties tableProperties,
    Properties partitionProperties) throws SerDeException {
    initialize(configuration, tableProperties);
  }

  @Override
  public void initialize(Configuration configuration, Properties properties) throws SerDeException {
    // Reset member variables so we don't get in a half-constructed state
    boolean isSerDeForRecordKeys = false;
    oi = null;
    topic = properties.getProperty("kafka.topic");

    Map<String, String> map = new HashMap<String, String>();
    for (final String name: properties.stringPropertyNames()) {
      map.put(name, properties.getProperty(name));
    }

    inner = new GenericAvroSerde();
    inner.configure(map, isSerDeForRecordKeys);

    final String columnNameProperty = properties.getProperty(serdeConstants.LIST_COLUMNS);
    final String columnTypeProperty = properties.getProperty(serdeConstants.LIST_COLUMN_TYPES);
    final String columnCommentProperty = properties.getProperty(LIST_COLUMN_COMMENTS, "");
    final String columnNameDelimiter = String.valueOf(SerDeUtils.COMMA);
    AvroObjectInspectorGenerator aoig = null;

    boolean gotColTypesFromColProps = true;
    if (columnNameProperty == null || columnNameProperty.isEmpty()
		    || columnTypeProperty == null || columnTypeProperty.isEmpty()) {
	    gotColTypesFromColProps = false;
    } else {
	    columnNames = internStringsInList(
			    Arrays.asList(columnNameProperty.split(columnNameDelimiter)));
	    columnTypes = TypeInfoUtils.getTypeInfosFromTypeString(columnTypeProperty);
    }

    deser = new AvroDeserializer(configuration);

    try {
      CachedSchemaRegistryClient schemaRegistry = new CachedSchemaRegistryClient(
  		    properties.getProperty("schema.registry.url"), 100, map);
      schema = schemaRegistry.getBySubjectAndId(properties.getProperty("schema.subject"), Integer.parseInt(properties.getProperty("schema.id")));

      aoig = new AvroObjectInspectorGenerator(schema);
      this.oi = aoig.getObjectInspector();

    } catch (RestClientException | IOException e) {
      throw new SerDeException(e.getMessage());
    }

    this.columnNames = internStringsInList(aoig.getColumnNames());
    this.columnTypes = aoig.getColumnTypes();

    if (!gotColTypesFromColProps) {
	    properties.setProperty(serdeConstants.LIST_COLUMNS, String.join(",", columnNames));
	    properties.setProperty(serdeConstants.LIST_COLUMN_TYPES, String.join(",", getTypeStringsFromTypeInfo(columnTypes)));
    }

  }

  private List<String> internStringsInList(List<String> list) {
	  if(list != null) {
		  try {
			  ListIterator<String> it = list.listIterator();
			  while(it.hasNext()) {
				  it.set(it.next().intern());
			  }
		  } catch(Exception e) {}
	  }
	  return list;
  }

  private List<String> getTypeStringsFromTypeInfo(List<TypeInfo> typeInfos) {
	  if(typeInfos == null) {
		  return null;
	  }

	  List<String> result = new ArrayList<>(typeInfos.size());
	  for(TypeInfo info : typeInfos) {
		  result.add(info.toString());
	  }
	  return result;
  }

  @Override
  public Class<? extends Writable> getSerializedClass() {
    return AvroGenericRecordWritable.class;
  }

  @Override
  public Writable serialize(Object o, ObjectInspector objectInspector) throws SerDeException {
       AvroGenericRecordWritable w = new 
	       KafkaSerDe.AvroBytesConverter(schema).getWritable(inner.serializer().serialize(topic, null, createGenericRecordFrom(o)));
       return w;
  }

  @Override
  public Object deserialize(Writable writable) throws SerDeException {
          return deser.deserialize(null, null, writable, schema);
  }

  @Override
  public ObjectInspector getObjectInspector() throws SerDeException {
    return oi;
  }

  @Override
  public SerDeStats getSerDeStats() {
    // No support for statistics. That seems to be a popular answer.
    return null;
  }

  private static GenericRecord createGenericRecordFrom(Object object) {
    final Schema schema = ReflectData.get().getSchema(object.getClass());
    final GenericData.Record record = new GenericData.Record(schema);
    Arrays.stream(object.getClass().getDeclaredFields()).forEach(field -> {
      try {
        record.put(field.getName(), field.get(object));
      } catch (IllegalAccessException e) {
        e.printStackTrace();
      }
    });
    return record;
  }
 
}
