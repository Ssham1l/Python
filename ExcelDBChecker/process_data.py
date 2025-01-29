import pandas as pd
import logging
import sys

# Инициализация логгера
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

formatter = logging.Formatter(
    '%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

console_handler = logging.StreamHandler(sys.stdout)
console_handler.setLevel(logging.INFO)
console_handler.setFormatter(formatter)

file_handler = logging.FileHandler('add_dimensions_to_excel.log', mode='w', encoding='utf-8')
file_handler.setLevel(logging.INFO)
file_handler.setFormatter(formatter)

logger.addHandler(console_handler)
logger.addHandler(file_handler)

def add_dimensions_to_excel(input_file, dump_file, output_file):
    try:
        logger.info("Начало обработки данных.")

        # Загрузка исходного Excel файла
        logger.info(f"Загрузка Excel файла: {input_file}")
        df_excel = pd.read_excel(input_file)

        # Проверка наличия нужных колонок в df_excel
        required_columns = {'lm_code', 'supplier_id'}
        if not required_columns.issubset(df_excel.columns):
            raise ValueError(f"Отсутствуют необходимые столбцы в input_file: {required_columns - set(df_excel.columns)}")

        # Загрузка данных из дампа (CSV)
        logger.info(f"Загрузка CSV файла дампа: {dump_file}")
        df_db = pd.read_csv(dump_file)

        # Проверка наличия необходимых колонок в df_db
        if not required_columns.issubset(df_db.columns):
            raise ValueError(f"Отсутствуют необходимые столбцы в dump_file: {required_columns - set(df_db.columns)}")

        # Выполняем объединение по двум ключам: lm_code и supplier_id
        logger.info("Сопоставление данных по полям 'lm_code' и 'supplier_id'")
        merged_df = pd.merge(df_excel, df_db, on=["lm_code", "supplier_id"], how="left")

        # Список атрибутов, которые нужно подтянуть из дамп-файла
        attributes_to_extract = [
            'unit_depth', 'unit_height', 'unit_width',
            'unit_net_weight', 'unit_gross_weight',
            'unit_pack_type', 'unit_pack_material', 'unit_this_side_up',
            'unit_fragile', 'unit_no_stack', 'unit_stor_temp',
            'unit_stack_height_inc', 'unit_pack_place_c', 'unit_liquid',
            'unit_layer_process', 'unit_blister_hole_d', 'unit_blister_length_lc',
            'unit_blister_length_rc', 'unit_blister_length_uc', 'unit_blister_hole_c',
            'unit_adr_class', 'unit_un_code', 'unit_tunnel_code',
            'unit_env_hazardous', 'unit_packaging_code',
            'inner_depth', 'inner_width', 'inner_height', 'inner_gross_weight',
            'inner_nest', 'inner_pack_type', 'inner_pack_material',
            'outer_depth', 'outer_width', 'outer_height', 'outer_gross_weight',
            'outer_nest', 'outer_pack_type', 'outer_pack_material',
            'pallet_layers_c', 'pallet_boxes_in_layer'
        ]

        # Список атрибутов, которые нужно делить на 1000
        attributes_to_divide_by_1000 = [
            'unit_net_weight',
            'unit_gross_weight',
            'inner_gross_weight',
            'outer_gross_weight'
        ]

        # Обрабатываем атрибуты
        for attr in attributes_to_extract:
            if attr in merged_df.columns:
                if attr in attributes_to_divide_by_1000:
                    merged_df[attr] = merged_df[attr] / 1000
            else:
                logger.warning(f"Столбец {attr} отсутствует в дампе. Он не будет добавлен в итоговый файл.")

        # Экспорт обновленного Excel файла
        logger.info(f"Сохранение обновленного файла: {output_file}")
        merged_df.to_excel(output_file, index=False)

        logger.info("Обработка данных завершена успешно.")
    except Exception as e:
        logger.error(f"Произошла ошибка: {e}", exc_info=True)

if __name__ == "__main__":
    # Указываем абсолютные пути к файлам
    input_file = '//Users/shamil/Documents/python_scripts/add_dimensions_to_excel/29.01.2025/input_data.xlsx'
    dump_file = '/Users/shamil/Documents/python_scripts/add_dimensions_to_excel/29.01.2025/dump_data.csv'
    output_file = '/Users/shamil/Documents/python_scripts/add_dimensions_to_excel/29.01.2025/output_data.xlsx'
    
    # Запуск функции обработки данных
    add_dimensions_to_excel(input_file, dump_file, output_file)
