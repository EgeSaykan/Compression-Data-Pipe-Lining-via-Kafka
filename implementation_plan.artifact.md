# Remove Redundant Time Tracking Fields

Remove `initialTime` and `endTime` (referred to as "beginning time" by the user) from the Bluetooth wire protocol and its corresponding handlers. These fields are redundant because each `SensorData` row already contains a `timestamp` field.

## User Review Required

> [!IMPORTANT]
> This change modifies the **Bluetooth wire protocol**. The header size will be reduced from **26 bytes to 10 bytes**. Both the Android application and the PC receiver must be updated simultaneously to maintain communication.

## Proposed Changes

### Bluetooth Communication Layer

#### [MODIFY] [BluetoothSender.kt](file:///D:/Safe/Durham/SOCAR/phone_pipe/app/src/main/java/com/example/phone_pipe/bluetooth/BluetoothSender.kt)
- Update `HEADER_SIZE` from 26 to 10.
- Remove `initialTime` and `endTime` parameters from `sendBatch`.
- Remove the code that writes these fields into the `ByteBuffer` header.
- Update log messages and documentation.

#### [MODIFY] [BluetoothPipelineController.kt](file:///D:/Safe/Durham/SOCAR/phone_pipe/app/src/main/java/com/example/phone_pipe/bluetooth/BluetoothPipelineController.kt)
- Remove the calculation of `initialTime` and `endTime` in the `flush` method.
- Update all calls to `sender.sendBatch` to remove the redundant time arguments.

### Native / PC Receiver Components

#### [MODIFY] [wire_protocol.h](file:///D:/Safe/Durham/SOCAR/phone_pipe/app/src/main/cpp/wire_protocol.h)
- Update `kHeaderSize` from 26 to 10.
- Remove `initialTimeMs` and `endTimeMs` from the `PacketHeader` struct.
- Update `decodeHeader` and `encodeHeader` functions to no longer process these fields.

#### [MODIFY] [recieve_usa_kafka.cpp](file:///D:/Safe/Durham/SOCAR/phone_pipe/app/src/main/cpp/recieve_usa_kafka.cpp)
- Remove the print statements that reference `hdr.initialTimeMs` and `hdr.endTimeMs`.

## Verification Plan

### Automated Tests
- Run Gradle build to ensure no compilation errors in Kotlin code.
- Verify header byte offsets in `BluetoothSender.kt`.

### Manual Verification
- Deploy to device and verify logs show the updated header size and removal of time fields.
- Re-build the PC receiver and verify it correctly parses the new 10-byte header.
