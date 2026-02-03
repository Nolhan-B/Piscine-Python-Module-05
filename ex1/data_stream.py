from typing import Any, List, Dict, Union, Optional
from abc import ABC, abstractmethod


class DataStream(ABC):

    def __init__(self, stream_id: str) -> None:
        self.stream_id: str = stream_id
        self.processed_count: int = 0
        self.total_items: int = 0

    @abstractmethod
    def process_batch(self, data_batch: List[Any]) -> str:
        pass

    def filter_data(
        self, data_batch: List[Any], criteria: Optional[str] = None
    ) -> List[Any]:
        if criteria is None:
            return data_batch
        return [item
                for item in data_batch
                if criteria.lower() in str(item).lower()]

    def get_stats(self) -> Dict[str, Union[str, int, float]]:
        return {
            "stream_id": self.stream_id,
            "processed_count": self.processed_count,
            "total_items": self.total_items,
        }


class SensorStream(DataStream):

    def __init__(self, stream_id: str) -> None:
        super().__init__(stream_id)
        self.stream_type: str = "Environmental Data"
        self.temperature_sum: float = 0.0
        self.temperature_count: int = 0

    def process_batch(self, data_batch: List[Any]) -> str:
        self.total_items += len(data_batch)

        for reading in data_batch:
            if isinstance(reading, str) and "temp:" in reading:
                try:
                    temp = float(reading.split("temp:")[1].split(",")[0])
                    self.temperature_sum += temp
                    self.temperature_count += 1
                except (ValueError, IndexError):
                    pass

        avg_temp = (
            self.temperature_sum / self.temperature_count
            if self.temperature_count > 0
            else 0.0
        )

        return (f"Sensor analysis: {len(data_batch)} "
                f"readings processed, avg temp: {avg_temp:.1f}°C")

    def filter_data(
        self, data_batch: List[Any], criteria: Optional[str] = None
    ) -> List[Any]:
        if criteria == "high-priority":
            critical = []
            for item in data_batch:
                if isinstance(item, str) and "temp:" in item:
                    try:
                        temp = float(item.split("temp:")[1].split(",")[0])
                        if temp > 25.0 or temp < 15.0:
                            critical.append(item)
                    except (ValueError, IndexError):
                        pass
            return critical
        return super().filter_data(data_batch, criteria)


class TransactionStream(DataStream):

    def __init__(self, stream_id: str) -> None:
        super().__init__(stream_id)
        self.stream_type: str = "Financial Data"
        self.net_flow: int = 0

    def process_batch(self, data_batch: List[Any]) -> str:
        self.total_items += len(data_batch)

        batch_flow = 0
        for transaction in data_batch:
            if isinstance(transaction, str):
                if "buy:" in transaction:
                    try:
                        amount = int(transaction.split("buy:")[1])
                        batch_flow -= amount
                    except (ValueError, IndexError):
                        pass
                elif "sell:" in transaction:
                    try:
                        amount = int(transaction.split("sell:")[1])
                        batch_flow += amount
                    except (ValueError, IndexError):
                        pass

        self.net_flow += batch_flow

        return (f"Transaction analysis: {len(data_batch)} operations, "
                f"net flow: {batch_flow:+d} units")

    def filter_data(
        self, data_batch: List[Any], criteria: Optional[str] = None
    ) -> List[Any]:
        if criteria == "high-priority":
            large = []
            for transaction in data_batch:
                if isinstance(transaction, str):
                    for prefix in ["buy:", "sell:"]:
                        if prefix in transaction:
                            try:
                                amount = int(transaction.split(prefix)[1])
                                if amount > 100:
                                    large.append(transaction)
                                    break
                            except (ValueError, IndexError):
                                pass
            return large
        return super().filter_data(data_batch, criteria)


class EventStream(DataStream):

    def __init__(self, stream_id: str) -> None:
        super().__init__(stream_id)
        self.stream_type: str = "System Events"
        self.error_count: int = 0

    def process_batch(self, data_batch: List[Any]) -> str:
        self.total_items += len(data_batch)

        batch_errors = sum(
            1
            for event in data_batch
            if isinstance(event, str) and "error" in event.lower()
        )
        self.error_count += batch_errors

        error_text = "error" if batch_errors == 1 else "errors"

        return (
            f"Event analysis: {len(data_batch)} events, "
            f"{batch_errors} {error_text} detected"
        )


class StreamProcessor:

    def __init__(self) -> None:
        self.streams: List[DataStream] = []

    def add_stream(self, stream: DataStream) -> None:
        self.streams.append(stream)

    def process_all(self, data_batches: List[List[Any]]) -> List[str]:
        results: List[str] = []
        for i, stream in enumerate(self.streams):
            if i < len(data_batches):
                result = stream.process_batch(data_batches[i])
                results.append(result)
        return results


def main() -> None:
    print("=== CODE NEXUS - POLYMORPHIC STREAM SYSTEM ===")

    print("\nInitializing Sensor Stream...")
    sensor = SensorStream("SENSOR_001")
    print(f"Stream ID: {sensor.stream_id}, Type: {sensor.stream_type}")
    sensor_data = ["temp:22.5", "humidity:65", "pressure:1013"]
    print(f"Processing sensor batch: {sensor_data}")
    print(sensor.process_batch(sensor_data))

    print("\nInitializing Transaction Stream...")
    trans = TransactionStream("TRANS_001")
    print(f"Stream ID: {trans.stream_id}, "
          f"Type: {trans.stream_type}")
    trans_data = ["buy:100", "sell:150", "buy:75"]
    print(f"Processing transaction batch: {trans_data}")
    print(trans.process_batch(trans_data))

    print("\nInitializing Event Stream...")
    event = EventStream("EVENT_001")
    print(f"Stream ID: {event.stream_id}, Type: {event.stream_type}")
    event_data = ["login", "error", "logout"]
    print(f"Processing event batch: {event_data}")
    print(event.process_batch(event_data))

    print("\n=== Polymorphic Stream Processing ===")
    print("Processing mixed stream types through unified interface...")

    processor = StreamProcessor()
    processor.add_stream(SensorStream("SENSOR_002"))
    processor.add_stream(TransactionStream("TRANS_002"))
    processor.add_stream(EventStream("EVENT_002"))

    batch_data = [
        ["temp:21.0", "humidity:60"],
        ["buy:50", "sell:75", "buy:25", "sell:100"],
        ["login", "logout", "error"],
    ]

    print("\nBatch 1 Results:")
    processor.process_all(batch_data)

    labels = ["Sensor", "Transaction", "Event"]
    counts = [2, 4, 3]
    units = {
        "Sensor": "readings",
        "Transaction": "operations",
    }

    for label, count in zip(labels, counts):
        unit = units.get(label, "events")
        print(f"- {label} data: {count} {unit} processed")

    print("\nStream filtering active: High-priority data only")

    sensor_filter_data = ["temp:30.0", "temp:10.0", "humidity:50"]
    sensor_filtered = sensor.filter_data(sensor_filter_data, "high-priority")

    trans_filter_data = ["buy:150", "sell:200", "buy:50"]
    trans_filtered = trans.filter_data(trans_filter_data, "high-priority")

    trans_text = "transaction" if len(trans_filtered) == 1 else "transactions"

    print(
        f"Filtered results: {len(sensor_filtered)} critical sensor alerts,"
        f" {len(trans_filtered)} large {trans_text}"
    )

    print()
    print("All streams processed successfully. Nexus throughput optimal.")


if __name__ == "__main__":
    main()
