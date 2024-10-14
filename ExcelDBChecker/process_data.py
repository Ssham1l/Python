import pandas as pd
import logging
import sys

def process_data_with_dump(input_file, dump_file, output_file):
    logger = logging.getLogger(__name__)
    logger.info("Начинается обработка данных.")

    # Шаг 1: Чтение входного файла
    logger.info(f"Чтение входного файла: {input_file}")
    try:
        input_df = pd.read_excel(input_file)
        logger.info(f"Входной файл успешно прочитан. Количество записей: {len(input_df)}")
    except Exception as e:
        logger.error(f"Ошибка при чтении входного файла: {e}", exc_info=True)
        return

    # Шаг 2: Чтение дамп-файла
    logger.info(f"Чтение дамп-файла: {dump_file}")
    try:
        dump_df = pd.read_csv(dump_file, usecols=['lm_code', 'supplier_id'])
        logger.info(f"Дамп-файл успешно прочитан. Количество записей: {len(dump_df)}")
    except Exception as e:
        logger.error(f"Ошибка при чтении дамп-файла: {e}", exc_info=True)
        return

    # Шаг 3: Создание множества пар для быстрого поиска
    logger.info("Создание множества пар из дамп-файла.")
    dump_set = set(zip(dump_df['lm_code'], dump_df['supplier_id']))
    logger.info(f"Множество пар создано. Общее количество уникальных пар: {len(dump_set)}")

    # Освобождаем память
    del dump_df

    # Шаг 4: Сравнение пар из входного файла с данными дамп-файла
    logger.info("Начинается сравнение данных.")
    try:
        input_df['есть в базе'] = input_df.apply(
            lambda row: (row['lm_code'], row['supplier_id']) in dump_set, axis=1
        )
        logger.info("Сравнение завершено.")
    except Exception as e:
        logger.error(f"Ошибка при сравнении данных: {e}", exc_info=True)
        return

    # Шаг 5: Сохранение результатов в выходной файл
    logger.info(f"Сохранение результатов в файл: {output_file}")
    try:
        input_df.to_excel(output_file, index=False)
        logger.info("Результаты успешно сохранены.")
    except Exception as e:
        logger.error(f"Ошибка при сохранении выходного файла: {e}", exc_info=True)
        return

    logger.info("Обработка данных завершена.")

if __name__ == "__main__":
    # Создаем логгер
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)

    # Создаем форматтер
    formatter = logging.Formatter(
        '%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    # Создаем обработчик для вывода в консоль
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(formatter)

    # Создаем обработчик для записи в файл
    file_handler = logging.FileHandler('process_data.log', mode='w', encoding='utf-8')
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(formatter)

    # Добавляем обработчики к логгеру
    logger.addHandler(console_handler)
    logger.addHandler(file_handler)

    # Указываем путь к файлам
    input_file = "/Users/shamil/Desktop/migration_11.10/input_data.xlsx"
    dump_file = "/Users/shamil/Desktop/migration_11.10/dump_data.csv"
    output_file = "/Users/shamil/Desktop/migration_11.10/output_data.xlsx"

    # Запуск функции обработки данных
    process_data_with_dump(input_file, dump_file, output_file)
