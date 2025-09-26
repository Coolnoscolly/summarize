import os
import random
from typing import List, Tuple, Optional, Iterable
from minio import Minio
from minio.error import S3Error
from config.settings import settings
from utils.singleton import Singleton
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

class MinioLoader(Singleton):
    """Загружает документы из MinIO/S3 bucket с возможностью:
    - фильтра по расширениям
    - фильтра по 'папке' (prefix)
    - выборки только части документов (всегда четное количество)
    """

    def __init__(self):
        # cert_check=False отключает проверку сертификата и вызывает InsecureRequestWarning при HTTPS.
        # Это намеренно оставлено без изменений для обратной совместимости.
        self.client = Minio(
            settings.MINIO_ENDPOINT,
            access_key=settings.MINIO_ACCESS_KEY,
            secret_key=settings.MINIO_SECRET_KEY,
            secure=settings.MINIO_SECURE,
            region=settings.MINIO_REGION,
            cert_check=False,
        )
        self.bucket_name = settings.MINIO_BUCKET_NAME
        self.allowed_extensions = [
            ext.strip().lower() for ext in settings.ALLOWED_EXTENSIONS
        ]
        self.sample_fraction: float = settings.MINIO_SAMPLE_FRACTION
        self.randomize_sampling: bool = settings.MINIO_SAMPLE_RANDOM
        self.sampling_seed: Optional[int] = settings.MINIO_SAMPLE_SEED
        self.default_folder_prefix: Optional[str] = settings.MINIO_FOLDER_PREFIX
        self._rnd = (
            random.Random(self.sampling_seed)
            if self.sampling_seed is not None
            else random
        )

    def check_connection(self) -> bool:
        """Проверяет подключение к MinIO"""
        try:
            self.client.list_buckets()
            return True
        except Exception as e:
            print(f"Ошибка подключения к MinIO: {e}")
            return False

    def list_files(self, prefix: Optional[str] = None) -> List[str]:
        """Возвращает список всех файлов с разрешенными расширениями"""
        try:
            objects = self.client.list_objects(
                self.bucket_name, prefix=prefix, recursive=True
            )
            files: List[str] = []
            for obj in objects:
                if any(
                    obj.object_name.lower().endswith(ext)
                    for ext in self.allowed_extensions
                ):
                    files.append(obj.object_name)
            return sorted(files)
        except S3Error as e:
            print(f"Ошибка при получении списка файлов: {e}")
            return []

    def _sample_files(self, files: List[str]) -> List[str]:
        """Возвращает четное количество файлов"""
        if not files:
            return files
        fraction = self.sample_fraction
        if fraction <= 0 or fraction >= 1:
            count = len(files)
        else:
            count = max(2, int((len(files) * fraction) + 0.5))
        if count % 2 != 0:
            count += 1
        if count >= len(files):
            count = len(files) - (len(files) % 2)
        if count == 0:
            return []
        if self.randomize_sampling:
            return self._rnd.sample(files, count)
        else:
            return files[:count]

    def _normalize_and_filter_file_list(self, file_list_input) -> List[str]:
        """
        Нормализует входной список файлов:
        - поддерживает строку с перечнем через запятую
        - поддерживает список строк, где элементы также могут содержать запятые
        - удаляет пробелы, пустые элементы
        - фильтрует по допустимым расширениям
        """
        candidates: List[str] = []
        if isinstance(file_list_input, str):
            candidates = file_list_input.split(",")
        elif isinstance(file_list_input, Iterable):
            for item in file_list_input:
                if isinstance(item, str):
                    if "," in item:
                        candidates.extend(item.split(","))
                    else:
                        candidates.append(item)
                else:
                    # игнорируем не-строки
                    continue
        else:
            raise TypeError("file_list должен быть строкой или списком строк")

        # очистка и фильтрация по расширению
        normalized: List[str] = []
        for c in candidates:
            f = c.strip()
            if not f:
                continue
            if any(f.lower().endswith(ext) for ext in self.allowed_extensions):
                normalized.append(f)
        return normalized

    def _parse_file_path(self, file_path: str) -> Tuple[str, str]:
        """Извлекает имя бакета и путь к объекту из полного пути"""
        parts = file_path.split("/", 1)
        if len(parts) == 2:
            return parts[0], parts[1]  # bucket_name, object_path
        else:
            # Если бакет не указан, используем бакет по умолчанию
            return self.bucket_name, file_path

    def read_file(self, file_path: str) -> str:
        """Читает содержимое файла из MinIO"""
        try:
            bucket_name, object_name = self._parse_file_path(file_path)
            response = self.client.get_object(bucket_name, object_name)
            content = response.read().decode("utf-8", errors="ignore")
            response.close()
            response.release_conn()
            return content
        except S3Error as e:
            print(f"Ошибка при чтении файла {file_path}: {e}")
            raise
        except UnicodeDecodeError:
            try:
                bucket_name, object_name = self._parse_file_path(file_path)
                response = self.client.get_object(bucket_name, object_name)
                content = response.read().decode("cp1251", errors="ignore")
                response.close()
                response.release_conn()
                return content
            except Exception:
                raise ValueError(f"Не удалось декодировать файл: {file_path}")

    def load_documents(
        self, folder_prefix: Optional[str] = None, file_list: Optional[List[str]] = None
    ) -> List[Tuple[str, str]]:
        """Загружает документы из MinIO"""

        if file_list:
            files = self._normalize_and_filter_file_list(file_list)
            
            if len(files) == 1:
                print("Пропускаем семплирование для одного файла")
            else:
                files = self._sample_files(files)
                
            print(f"После семплирования: {files}")
            
            if not files:
                print("⚠️  Файлы после фильтрации/семплирования пусты")
                return []
        else:
            # Если список файлов не указан, работаем по старой логике с проверкой подключения
            if not self.check_connection():
                raise ConnectionError("Не удалось подключиться к MinIO")

            effective_prefix = (
                folder_prefix
                if folder_prefix is not None
                else self.default_folder_prefix
            )
            files = self.list_files(prefix=effective_prefix)
            files = self._sample_files(files)

        documents: List[Tuple[str, str]] = []
        for file_path in files:
            try:
                content = self.read_file(file_path).strip()
                if content:
                    documents.append((file_path, content))
            except Exception as e:
                print(f"Ошибка при обработке файла {file_path}: {e}")
        return documents


class HybridLoader:
    """Гибридный загрузчик: поддерживает MinIO и локальные файлы"""

    def __init__(self):
        self.minio_loader = MinioLoader()

    def load_documents(
        self, folder_prefix: Optional[str] = None, file_list: Optional[List[str]] = None
    ) -> List[Tuple[str, str]]:
        # Всегда пытаемся использовать MinioLoader, но с разной логикой в зависимости от file_list
        try:
            minio_docs = self.minio_loader.load_documents(
                folder_prefix=folder_prefix, file_list=file_list
            )
            if minio_docs:
                if file_list:
                    print(
                        f"Загружено {len(minio_docs)} документов из MinIO (явный список)"
                    )
                else:
                    print(
                        f"Загружено {len(minio_docs)} документов из MinIO (по префиксу)"
                    )
                return minio_docs
        except Exception as e:
            if file_list:
                print(f"Не удалось загрузить из MinIO (явный список): {e}")
            else:
                print(f"Не удалось загрузить из MinIO (по префиксу): {e}")

        return []
