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
        return (
            f"Processed {len(data)} numeric values, "
            f"sum={sum(data)}, avg={sum(data) / len(data):.1f}"
        )

    def validate(self, data: Any) -> bool:
        for e in data:
            if not isinstance(e, int):
                return False
        return True


class TextProcessor(DataProcessor):
    def process(self, data: str) -> str:
        return (
            f"Processed text: {len(data)} characters, "
            f"{len(data.split())} words"
        )

    def validate(self, data: Any) -> bool:
        for e in data:
            if not isinstance(e, str):
                return False
        return True


class LogProcessor(DataProcessor):
    def process(self, data: str) -> str:
        if "ERROR" in data:
            lvl, message = data.split(":", 1)
            return f"[ALERT] ERROR level detected:{message}"
        if "INFO" in data:
            lvl, message = data.split(":", 1)
            return f"[INFO] Info level detected:{message}"
        return ""

    def validate(self, data: Any) -> bool:
        if not isinstance(data, str):
            print("Error: Non-numeric data found: invalid data")
            return False
        if "ERROR" in data or "INFO" in data:
            return True
        return False


def main() -> None:
    print("== CODE NEXUS - DATA PROCESSOR FOUNDATION ===\n")

    print("Initializing Numeric Processor...")
    data: List[int] = [1, 2, 3, 4, 5]
    np: NumericProcessor = NumericProcessor()

    print(f"Processing data: {data}")
    processed_data: str = np.process(data)
    if np.validate(data):
        print("Validation: Numeric data verified")
        print(np.format_output(processed_data))

    print("\nInitializing Text Processor...")
    data = "Hello Nexus World"
    tp: TextProcessor = TextProcessor()

    print(f"Processing data: {data}")
    processed_data = tp.process(data)
    if tp.validate(data):
        print("Validation: Text data verified")
        print(tp.format_output(processed_data))

    print("\nInitializing Log Processor...")
    data = "ERROR: Connection timeout"
    lp: LogProcessor = LogProcessor()

    print(f"Processing data: {data}")
    processed_data = lp.process(data)
    if lp.validate(data):
        print("Validation: Log entry verified")
        print(lp.format_output(processed_data))

    print("\n=== Polymorphic Processing Demo ===\n")
    print("Processing multiple data types through same interface...")

    data_set: List[Tuple[DataProcessor, Any]] = [
        (np, [1, 2, 3]),
        (tp, "Hello world!"),
        (lp, "INFO: System ready"),
    ]

    iter: int = 1
    for p, data in data_set:
        if p.validate(data):
            print(f"Result {iter}: {p.process(data)}")
        iter += 1

    print("\nFoundation systems online. Nexus ready for advanced streams.")


if __name__ == "__main__":
    main()
