import time
from config.settings import settings
from core.pipeline import SummaryPipeline


def main():
    print("Запуск процесса суммаризации...")
    start_time = time.time()
    pipeline = SummaryPipeline()
    result = pipeline.run()
    execution_time = time.time() - start_time
    print(f"Процесс завершен за {execution_time:.2f} секунд.")
    print(f"Финальный результат сохранен в: {settings.OUTPUT_FILE}")


if __name__ == "__main__":
    main()