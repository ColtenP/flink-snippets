package flink.snippets.authentication.detection.sources;

import flink.snippets.authentication.detection.models.LoginMetadata;
import org.apache.flink.connector.datagen.source.GeneratorFunction;

import java.util.Random;
import java.util.UUID;

public class LoginMetadataGenerator implements GeneratorFunction<Long, LoginMetadata> {
  private final transient Random random = new Random();

  @Override
  public LoginMetadata map(Long sequence) throws Exception {
    random.setSeed(sequence % 100_000);
    UUID userId = new UUID(random.nextInt(), random.nextInt());
    random.setSeed(sequence % 1_000);
    UUID locationId = new UUID(random.nextInt(), random.nextInt());
    random.setSeed(sequence % 100);
    UUID deviceId = new UUID(random.nextInt(), random.nextInt());
    return new LoginMetadata(userId, locationId, deviceId);
  }
}
