from processing import process_data



try:
    process_data.save_tick_data_csv()
except Exception as e:
    print('Não foi possível adicionar os novos ticks ao arquivo csv: ', e)

