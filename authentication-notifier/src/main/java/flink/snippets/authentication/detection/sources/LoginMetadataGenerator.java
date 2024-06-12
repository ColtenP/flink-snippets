package flink.snippets.authentication.detection.sources;

import flink.snippets.authentication.detection.models.LoginMetadata;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.connector.datagen.source.GeneratorFunction;

import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class LoginMetadataGenerator implements GeneratorFunction<Long, LoginMetadata> {
  private Random random;
  private List<UUID> userIds;
  private List<UUID> locationIds;
  private List<UUID> deviceIds;

  @Override
  public void open(SourceReaderContext readerContext) {
    random = new Random();

    userIds = IntStream
        .range(0, 100_000)
        .mapToObj(index -> UUID.randomUUID())
        .collect(Collectors.toList());

    locationIds = IntStream
        .range(0, 1_000)
        .mapToObj(index -> UUID.randomUUID())
        .collect(Collectors.toList());

    deviceIds = IntStream
        .range(0, 1_000)
        .mapToObj(index -> UUID.randomUUID())
        .collect(Collectors.toList());
  }

  @Override
  public LoginMetadata map(Long sequence) {
    return new LoginMetadata(
        userIds.get(random.nextInt(userIds.size())),
        locationIds.get(random.nextInt(locationIds.size())),
        deviceIds.get(random.nextInt(deviceIds.size()))
    );
  }
}
