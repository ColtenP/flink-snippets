package flink.snippets.traffic.light;

import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.junit.ClassRule;
import org.junit.Test;

public class IncorrectTrafficLightAppTest {
  @ClassRule
  public static MiniClusterWithClientResource flinkCluster =
      new MiniClusterWithClientResource(
          new MiniClusterResourceConfiguration.Builder()
              .setNumberSlotsPerTaskManager(1)
              .setNumberTaskManagers(1)
              .build()
      );

  @Test
  public void runIncorrectTrafficLightApp() throws Exception {
    IncorrectTrafficLightApp.runFlow();
  }
}
