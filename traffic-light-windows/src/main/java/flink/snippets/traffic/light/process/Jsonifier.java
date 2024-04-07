package flink.snippets.traffic.light.process;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

public class Jsonifier<T> extends ProcessFunction<T, String> {
  public ObjectMapper mapper;

  @Override
  public void open(Configuration parameters) {
    mapper = new ObjectMapper();
  }

  @Override
  public void processElement(T event, ProcessFunction<T, String>.Context ctx, Collector<String> out) throws Exception {
    out.collect(mapper.writeValueAsString(event));
  }
}
