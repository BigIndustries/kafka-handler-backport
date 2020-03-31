package org.apache.hadoop.hive.kafka;

import java.util.Properties;
import java.util.Map;
import java.util.HashMap;
import java.util.Arrays;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.BytesWritable;
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

public class ConfluentAvroSerde extends AbstractSerDe {

  private GenericAvroSerde inner;
  private ObjectInspector oi;
  private String topic;

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
  }

  @Override
  public Class<? extends Writable> getSerializedClass() {
    return AvroGenericRecordWritable.class;
  }

  @Override
  public Writable serialize(Object o, ObjectInspector objectInspector) throws SerDeException {
    return new BytesWritable(inner.serializer().serialize(topic, null, createGenericRecordFrom(o)));
  }

  @Override
  public Object deserialize(Writable writable) throws SerDeException {
    return inner.deserializer().deserialize(topic, null, ((BytesWritable)writable).getBytes());
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
