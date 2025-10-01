from processing import process_data



try:
    process_data.save_tick_data_csv()
except Exception as e:
    print('Não foi possível adicionar os novos ticks ao arquivo csv: ', e)

try:
    process_data.save_tick_data_db()
except Exception as e:
    print('Não foi possível adicionar os novos ticks ao arquivo banco de dados: ', e)

try:
    process_data.save_renko_csv()
except Exception as e:
    print('Não foi possível adicionar os novos tijolos renko ao csv: ', e)

try:
    process_data.save_renko_db()
except Exception as e:
    print('Não foi possível adicionar os novos tijolos renko ao banco de dados: ', e)