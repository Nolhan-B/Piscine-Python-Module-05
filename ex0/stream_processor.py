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
    def validate(self, data: Any) -> bool:
        if not isinstance(data, list) or len(data) == 0:
            return False
        for e in data:
            if not isinstance(e, int):
                return False
        return True

    def process(self, data: Any) -> str:
        try:
            total = sum(data)
            avg = total / len(data)
            return (
                f"Processed {len(data)} numeric values, "
                f"sum={total}, avg={avg:.1f}"
            )
        except Exception:
            return "Numeric processing failed"


class TextProcessor(DataProcessor):
    def validate(self, data: Any) -> bool:
        return isinstance(data, str)

    def process(self, data: Any) -> str:
        try:
            return (
                f"Processed text: {len(data)} characters, "
                f"{len(data.split())} words"
            )
        except Exception:
            return "Text processing failed"


class LogProcessor(DataProcessor):
    def validate(self, data: Any) -> bool:
        if not isinstance(data, str):
            return False
        return "ERROR:" in data or "INFO:" in data

    def process(self, data: Any) -> str:
        try:
            level, message = data.split(":", 1)
            message = message.strip()

            if level == "ERROR":
                return f"[ALERT] ERROR level detected: {message}"
            if level == "INFO":
                return f"[INFO] INFO level detected: {message}"

            return "Unknown log level"
        except Exception:
            return "Log processing failed"


def main() -> None:
    print("=== CODE NEXUS - DATA PROCESSOR FOUNDATION ===\n")

    print("Initializing Numeric Processor...")
    data: List[int] = [1, 2, 3, 4, 5]
    np = NumericProcessor()

    print(f"Processing data: {data}")
    if np.validate(data):
        result = np.process(data)
        print("Validation: Numeric data verified")
        print(np.format_output(result))

    print("\nInitializing Text Processor...")
    data = "Hello Nexus World"
    tp = TextProcessor()

    print(f"Processing data: {data}")
    if tp.validate(data):
        result = tp.process(data)
        print("Validation: Text data verified")
        print(tp.format_output(result))

    print("\nInitializing Log Processor...")
    data = "ERROR: Connection timeout"
    lp = LogProcessor()

    print(f"Processing data: {data}")
    if lp.validate(data):
        result = lp.process(data)
        print("Validation: Log entry verified")
        print(tp.format_output(result))

    print("\n=== Polymorphic Processing Demo ===\n")
    print("Processing multiple data types through same interface...")

    data_set: List[Tuple[DataProcessor, Any]] = [
        (np, [1, 2, 3]),
        (tp, "Hello world!"),
        (lp, "INFO: System ready"),
    ]

    i = 1
    for processor, data in data_set:
        if processor.validate(data):
            print(f"Result {i}: {processor.process(data)}")
        i += 1

    print("\nFoundation systems online. Nexus ready for advanced streams.")


if __name__ == "__main__":
    main()
