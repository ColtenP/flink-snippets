package flink.snippets.authentication.detection.process;

import flink.snippets.authentication.detection.models.LoginMetadata;
import flink.snippets.authentication.detection.models.LoginNotification;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

public class LoginNotifier extends KeyedProcessFunction<UUID, LoginMetadata, LoginNotification> {
  private ValueState<Set<UUID>> locationHistoryState;
  private ValueState<Set<UUID>> deviceHistoryState;

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);

    this.locationHistoryState = getRuntimeContext()
        .getState(new ValueStateDescriptor<>("LocationHistory", TypeInformation.of(new TypeHint<>() {})));
    this.deviceHistoryState = getRuntimeContext()
        .getState(new ValueStateDescriptor<>("DeviceHistory", TypeInformation.of(new TypeHint<>() {})));
  }

  @Override
  public void processElement(
      LoginMetadata loginMetadata,
      KeyedProcessFunction<UUID, LoginMetadata, LoginNotification>.Context context,
      Collector<LoginNotification> collector
  ) throws Exception {
    Set<UUID> locationHistory = locationHistoryState.value();
    Set<UUID> deviceHistory = deviceHistoryState.value();

    if (locationHistory == null) {
      locationHistory = new HashSet<>();
      locationHistoryState.update(locationHistory);
    }

    if (deviceHistory == null) {
      deviceHistory = new HashSet<>();
      deviceHistoryState.update(deviceHistory);
    }

    boolean knownLocation = locationHistory.contains(loginMetadata.locationId);
    boolean knownDevice = deviceHistory.contains(loginMetadata.deviceId);

    if (!knownLocation) {
      locationHistory.add(loginMetadata.locationId);
      locationHistoryState.update(locationHistory);
    }

    if (!knownDevice) {
      deviceHistory.add(loginMetadata.deviceId);
      deviceHistoryState.update(deviceHistory);
    }

    if (!knownLocation || !knownDevice) {
      collector.collect(new LoginNotification(
          loginMetadata.transactionId,
          loginMetadata.userId,
          knownLocation,
          knownDevice
      ));
    }
  }
}
