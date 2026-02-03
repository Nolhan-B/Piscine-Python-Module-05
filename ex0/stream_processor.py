from typing import Any, List, Tuple
from abc import ABC, abstractmethod


class DataProcessor(ABC):
    @abstractmethod
    def process(self, data: Any) -> str:
        pass

    @abstractmethod
    def validate(self, data: Any) -> bool:
        pass

    def format_output(self, result: str) -> str:
        return f"Output: {result}"


class NumericProcessor(DataProcessor):
    def process(self, data: Any) -> str:
        return (f"Processed {len(data)} numeric values, "
                f"sum={sum(data)}, avg={sum(data) / len(data):.1f}")

    def validate(self, data: Any) -> bool:
        if not isinstance(data, list):
            return False
        return all(isinstance(e, (int, float)) for e in data)


class TextProcessor(DataProcessor):
    def process(self, data: str) -> str:
        return (
            f"Processed text: {len(data)} characters, "
            f"{len(data.split())} words"
        )

    def validate(self, data: Any) -> bool:
        return isinstance(data, str)


class LogProcessor(DataProcessor):
    def process(self, data: str) -> str:
        if "ERROR" in data:
            lvl, message = data.split(":", 1)
            return f"[ALERT] {lvl} level detected:{message}"
        if "INFO" in data:
            lvl, message = data.split(":", 1)
            return f"[{lvl}] {lvl} level detected:{message}"
        return ""

    def validate(self, data: Any) -> bool:
        if not isinstance(data, str):
            return False
        if "ERROR" in data or "INFO" in data:
            return True
        return False


def main() -> None:
    print("=== CODE NEXUS - DATA PROCESSOR FOUNDATION ===\n")

    print("Initializing Numeric Processor...")
    numeric_data: List[int] = [1, 2, 3, 4, 5]
    np: NumericProcessor = NumericProcessor()
    print(f"Processing data: {numeric_data}")
    if np.validate(numeric_data):
        processed_data: str = np.process(numeric_data)
        print("Validation: Numeric data verified")
        print(np.format_output(processed_data))

    print("\nInitializing Text Processor...")
    text_data: str = "Hello Nexus World"
    tp: TextProcessor = TextProcessor()
    print(f'Processing data: "{text_data}"')
    if tp.validate(text_data):
        processed_data = tp.process(text_data)
        print("Validation: Text data verified")
        print(tp.format_output(processed_data))

    print("\nInitializing Log Processor...")
    log_data: str = "ERROR: Connection timeout"
    lp: LogProcessor = LogProcessor()
    print(f'Processing data: "{log_data}"')
    if lp.validate(log_data):
        processed_data = lp.process(log_data)
        print("Validation: Log entry verified")
        print(np.format_output(processed_data))

    print("\n=== Polymorphic Processing Demo ===\n")
    print("Processing multiple data types through same interface...")

    data_set: List[Tuple[DataProcessor, Any]] = [
        (np, [1, 2, 3]),
        (tp, "Hello world!"),
        (lp, "INFO: System ready"),
    ]

    idx: int = 1
    for processor, data in data_set:
        if processor.validate(data):
            print(f"Result {idx}: {processor.process(data)}")
            idx += 1

    print("\nFoundation systems online. Nexus ready for advanced streams.")


if __name__ == "__main__":
    main()
