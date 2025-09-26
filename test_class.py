from core.pipeline import SummaryPipeline

# Список файлов (пути в MinIO, например, "folder/doc1.txt", "folder/doc2.md")
file_list = [""]

# Создаем пайплайн
pipeline = SummaryPipeline()

# Запускаем с указанным списком файлов
result = pipeline.run(file_list=file_list) 

# Выводим результат
print(result)

