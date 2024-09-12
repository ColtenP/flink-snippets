package flink.snippets.join.sql;

import flink.snippets.join.sql.models.VehicleEvent;
import flink.snippets.join.sql.models.VehicleTelemetry;
import flink.snippets.join.sql.sources.VehicleEventGenerator;
import flink.snippets.join.sql.sources.VehicleTelemetryGenerator;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

public class App {
  public static void main(String[] args) throws URISyntaxException, IOException {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    StreamTableEnvironment tenv = StreamTableEnvironment.create(env);

    DataStream<VehicleEvent> vehicleEvents = VehicleEventGenerator.toSource(env);
    DataStream<VehicleTelemetry> vehicleTelemetry = VehicleTelemetryGenerator.toSource(env);

    tenv.createTemporaryView("vehicle_events", vehicleEvents);
    tenv.createTemporaryView("vehicle_telemetry", vehicleTelemetry);

    String sql = readResource("/stream.sql");
    System.out.println(sql);

    tenv.executeSql(sql);
  }

  public static String readResource(String path) throws URISyntaxException, IOException {
    URL resource = App.class.getResource(path);
    assert resource != null;
    Path resourcePath = Paths.get(resource.toURI());
    List<String> lines = Files.readAllLines(resourcePath);
    StringBuilder stringBuilder = new StringBuilder();
    lines.forEach(line -> stringBuilder.append(line).append("\n"));

    return  stringBuilder.toString();
  }
}