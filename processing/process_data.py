import pandas as pd
import os
from datetime import date, time, datetime
from . import renko
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
from config import symbol, engine, R_values, tick, INPUT_FOLDER_PATH, OUTPUT_FOLDER_PATH

def get_file_path(folder_path = INPUT_FOLDER_PATH, source = "profit"):
    """
    Returns the path of the first CSV file found in a folder.

    Args:
        folder_path (str, optional): Path to the folder containing the CSV files.
            Default is `INPUT_FOLDER_PATH `.
        source (str, optional): Data source subfolder name. Should be 'profit', 'metatrader' or None.
            Defaults to 'profit'.
        
    Returns:
        str or None: Full path of the first CSV file found, or None if no CSV file exists in the folder.
    """

    allowed_sources = ['profit', 'metatrader', None]

    if source not in allowed_sources:
        raise ValueError(f"Invalid source '{source}'. Must be one of {allowed_sources}.")
    
    if source is not None:
        folder_path = os.path.join(folder_path, source)
   
    files = os.listdir(folder_path)
    

    df_path = None
    for f in files:
        if f.endswith(".csv"):  
            df_path = os.path.join(folder_path, f)
            print("Arquivo CSV detectado: ", df_path)
            break

    return df_path

def rename_columns(df):
    df = df.rename(columns={
        'Preço': 'preco',
        'Quantidade': 'quantidade',
        'Data': 'data',
        'Hora': 'hora',
        'Comprador': 'comprador',
        'Vendedor': 'vendedor',
        'Tipo': 'tipo',
        'Ativo': 'ativo'
        })
    return df


def normalize_df(input_df_path):

    """
    Load and normalize a tick data CSV.

    Reads a semicolon-separated CSV (encoded in Latin-1), renames its columns 
    to standardized names, and converts numeric fields from Brazilian/European 
    formatting (e.g., "1.234,56") into float values.

    Args:
        input_df_path (str): Path to the tick data CSV file.

    Returns:
        pandas.DataFrame: Normalized DataFrame with standardized column names 
        and numeric types for 'preco' and 'quantidade'.
    """

    input_file = input_df_path

    df = pd.read_csv(input_file, sep=';', encoding='latin1')

    try:
        df = rename_columns(df)

    except Exception as e:
        print("Não foi possível renomear as colunas durante a normalização: ", e)
    print(df.head())
    print("Length: ", len(df))


    #since the price is in 140.000 format for 140k...
    df["preco"]*= 1000
    #df.to_csv(output_file, sep=',', index=False, encoding='utf-8')
    return df


def save_tick_data_csv():

    """
    Reads the latest tick data, normalizes it to standart format, and appends it to the historical dataset csv file (if any).
    If there's no previous data, a new file is created.

    Args:
        None

    Returns:
        None
            The function writes the updated dataset to disk but does not return a value..

    Raises:
        FileNotFoundError: If the input file path or `all_time_ticks` path cannot be found.
        pd.errors.ParserError: If a CSV file is malformed.
        Any exception raised by `normalize_df` or `df_olders_first`.

    """
    input_df_path = get_file_path()

    try: 
        new_data = pd.read_csv(input_df_path)
        new_data = new_data.rename(columns={
        'Preço': 'preco',
        'Quantidade': 'quantidade',
        'Data': 'data',
        'Hora': 'hora',
        'Comprador': 'comprador',
        'Vendedor': 'vendedor',
        'Tipo': 'tipo',
        'Ativo': 'ativo'
        })
    except Exception as e:
        try: 
            new_data = normalize_df(input_df_path)
            print("Tabela já normalizada")
        except Exception as e:
            print("Não foi possível normalizar a nova tabela: ", e)

    new_data = df_olders_first(new_data)

    #Access older data and merges them
    older_data_path = os.path.join(OUTPUT_FOLDER_PATH, "all_time_ticks", "all_time_ticks.csv")
    if os.path.exists(older_data_path):
        older_data = pd.read_csv(older_data_path)
    else:
        # If there's no previous data, it creates an empty DataFrame with the expected columns
        older_data = pd.DataFrame(columns=["ativo", "data", "hora","comprador",
            "preco", "quantidade", "vendedor", "tipo"
        ])
        print("Building dataframe 'all_time_ticks' as a csv file")

    if is_new_data_updated(new_data, older_data, use_df = True) == False:
        return print("Dados não inseridos no csv: a data já está presente no conjunto de dados")
    
    combined_df = pd.concat([older_data, new_data], ignore_index=True)
    combined_df.to_csv(older_data_path , sep=',', index=False, encoding='utf-8')     


def save_tick_data_db():

    """
    Reads the tick data csv file in the folder_path param, normalizes it to standart format, and inserts it into the database.

    Args:
        None
    Returns:
        None
            The function does not return a value, but modifies a database table.
    """
    input_df_path = get_file_path()

    try: 
        new_data = pd.read_csv(input_df_path)
        new_data = new_data.rename(columns={
        'Preço': 'preco',
        'Quantidade': 'quantidade',
        'Data': 'data',
        'Hora': 'hora',
        'Comprador': 'comprador',
        'Vendedor': 'vendedor',
        'Tipo': 'tipo',
        'Ativo': 'ativo'
        })
    except Exception as e:
        try: 
            new_data = normalize_df(input_df_path)
            print("Tabela normalizada")
        except Exception as e:
            print("Não foi possível normalizar a nova tabela: ", e)

    if is_new_data_updated(new_data, "all_time_ticks") == False:
        return print("Dados não inseridos no banco de dados: a data já está presente no banco de dados")
    
    else:
    
        new_data = df_olders_first(new_data)
        new_data["data"] = pd.to_datetime(new_data["data"], dayfirst=True, errors='coerce').dt.date
        new_data["hora"] = pd.to_datetime(new_data["hora"], format="%H:%M:%S", errors='coerce').dt.time

    
        try: 
            with engine.begin() as conn:  # transaction-safe
                    # Inserts the new data
                    try:
                        new_data.to_sql('all_time_ticks', conn, if_exists='append', index=False, method='multi', chunksize=500)
                        print("Novos dados dados de ticks salvos com sucesso no banco de dados")
                    except SQLAlchemyError as e:
                        print("Erro na inserção de dados pd to sql: ", e)
        except Exception as e:
            print("Erro na comunicação com o banco de dados")




def df_olders_first(df):
  
    """
    Sorts a temporal Dataframe by date and hour (data, hora), placing older values first.

    Args:
        df:
            The dataframe to be sorted.

    Returns:
        df_sorted:
            The dataframe now sorted.
    """

    df_sorted = df.sort_values(
    by=["data", "hora"],
    ascending=[True, True], 
    ignore_index=True,         
    )

    return df_sorted
 

def save_renko_csv(R_values = R_values):

    """
    Process tick data and update Renko chart CSVs.

    This function reads the most recent tick data file, converts it into a Renko
    DataFrame for each given box size (R), and appends or creates the corresponding
    Renko CSV file in the `renko` directory inside output_data. If a previous Renko file exists, 
    it concatenates the new data while avoiding duplication. Otherwise, a new CSV 
    file is created.

    Args
    ----------
    R_values : list of int, optional
        List of Renko box sizes to be processed (e.g., ``[8, 13, 40]``).
        Defaults to the global variable `R_values`.

    Returns
    -------
    None
        The function does not return a value, but modifies or creates CSV files 
        under the `renko` folder.
    """
        
    input_df_path =  get_file_path()

    for R in R_values:
        box_size = (R-1)*tick

        try:
            try:
                output_dir = (os.path.join(OUTPUT_FOLDER_PATH, 'renko', f'{R}R'))
                previous_df_path = get_file_path(folder_path = output_dir, source = None)

                if previous_df_path is not None:        
                    previous_df = pd.read_csv(previous_df_path)
                else:
                    # If there's no previous data, it creates an empty DataFrame with the expected columns
                    previous_df =  None
                    print(f"No previous csv found for this frequency. Building dataframe '{symbol}{R}R' as a csv file")
                          
            except Exception as e:
                print(f"Erro ao procurar os dados anteriores na frequência {R}R: ", e)

            #Adicionar aqui uma verificação de data para não ter duplicação

            try: 
                    tick_df = pd.read_csv(input_df_path)
                    tick_df = tick_df.rename(columns={
                    'Preço': 'preco',
                    'Quantidade': 'quantidade',
                    'Data': 'data',
                    'Hora': 'hora',
                    'Comprador': 'comprador',
                    'Vendedor': 'vendedor',
                    'Tipo': 'tipo',
                    'Ativo': 'ativo'
                    })
            except Exception as e:
                try: 
                    tick_df = normalize_df(input_df_path)
                    print("Tabela normalizada")
                except Exception as e:
                    print("Não foi possível ou necessário normalizar a nova tabela: ", e)

            
            if is_new_data_updated(tick_df, previous_df, use_df = True) == False:
                return print("Dados não inseridos na tabela renko: já existem dados para essa data")
    

            trade_df = df_olders_first(tick_df)
           

            renko_df = renko.build_renko_from_tick_df(trade_df, previous_df = previous_df, box_size=box_size, tick = tick)
           
            if (previous_df is not None) and (not previous_df.empty):
                previous_df = previous_df.iloc[:-1]
                combined_df = pd.concat([previous_df , renko_df], ignore_index=True)
                #Aqui a modificação deverá ser feita na última linha do DB e deverá ser feita a inserção de novas linhas
                combined_df.to_csv(os.path.join(OUTPUT_FOLDER_PATH, 'renko', f'{R}R',f'{symbol}_{R}R.csv' ), sep=',', index=False, encoding='utf-8')     
            else:
                try:
                    renko_df.to_csv(os.path.join(OUTPUT_FOLDER_PATH, 'renko', f'{R}R', f'{symbol}_{R}R.csv' ), sep=',', index=False, encoding='utf-8')
                except Exception as e:
                    print("Erro: ", e)
            print("Novos dados renko salvos com sucesso nos arquivos .csv")
        except Exception as e:
            print(f"Os dados do dia anterior não puderam ser processados ({R}R): ", e)


def save_renko_db(R_values = R_values):
    
    """
    Process tick data and update Renko tables in the database.

    This function reads the most recent tick data file, transforms it into a 
    Renko DataFrame for each given box size (R), and updates the corresponding 
    Renko table in the database. If previous Renko data exists, the last row 
    is deleted (to avoid duplication/overlap), and the new rows are appended.

    Args:
    ----------
    R_values : list of int, optional
        List of Renko box sizes to be processed (e.g., ``[8, 13, 40]``).
        Defaults to the global variable `R_values`.

    Returns
    -------
    None
        The function does not return a value. Instead, it modifies database tables.
    """

    input_df_path =  get_file_path()

    try: 

        for R in R_values:
            box_size = (R-1)*tick

            try:
                #previous_df = get_file_path(os.path.join('renko_data', f'{R}R'))
                #aqui será necessário acessar DB. Fazer as modificações adequadas 
                query = f"SELECT * FROM {symbol}_{R}r ORDER BY id DESC LIMIT 2"
                last_row = pd.read_sql(query, engine)

                if last_row is None or last_row.empty:
                    print(f"Não há dados anteriores na frequência {R}R")
                    #Assigns None to last_row in order to avoid crashing int the function build_renko_from_df()
                    last_row = None

            except Exception as e:
                print(f"Erro ao consultar os dados anteriores para a frequência {R}R no banco de dados: ", e)

            #Adicionar aqui uma verificação de data para não ter duplicação

            
            try: 
                tick_df = pd.read_csv(input_df_path)
                tick_df = tick_df.rename(columns={
                'Preço': 'preco',
                'Quantidade': 'quantidade',
                'Data': 'data',
                'Hora': 'hora',
                'Comprador': 'comprador',
                'Vendedor': 'vendedor',
                'Tipo': 'tipo',
                'Ativo': 'ativo'
                })
            except Exception as e:
                try: 
                    tick_df = normalize_df(input_df_path)
                    print("Tabela normalizada")
                except Exception as e:
                    print("Não foi possível ou necessário normalizar a nova tabela: ", e)
                    
            
            if is_new_data_updated(tick_df, f'{symbol}_{R}') == False:
                return print("Dados não inseridos na tabela renko no banco de dados: já existem dados para essa data")
           
            trade_df = df_olders_first(tick_df)
    

            renko_df = renko.build_renko_from_tick_df(trade_df, previous_df = last_row, box_size=box_size, tick = tick)
           
            with engine.begin() as conn: 
            #Remover a ultima linha antes de inserir a nova, pois ela será atualizada
                try:
                    result = conn.execute(text(f"""
                        DELETE FROM {symbol}_{R}r
                        WHERE id = (SELECT id FROM {symbol}_{R}r ORDER BY id DESC LIMIT 1)
                    """))
                    print(f"Linhas removidas: {result.rowcount}")
                except SQLAlchemyError as e:
                    print(f"Não há dados anteriores na tabela {symbol}_{R}r")

                try:
                    renko_df.to_sql(f'{symbol}_{R}r', conn, if_exists='append', index=False, method='multi', chunksize=50)
                except SQLAlchemyError as e:
                    print("Erro na inserção de dados pd to sql: ", e)
            
        print("Tabelas renko inseridas com sucesso no banco de dados")
    except Exception as e:
        print(f"Os dados do dia anterior para {symbol.upper()} não puderam ser processados ou inseridos no banco de dados ({R}R): ", e)



def is_new_data_updated(new_df, table_name, use_df= False):

    """
    Checks if the new DataFrame contains more recent records compared to the stored data.

    Parameters
    ----------
    new_df : pandas.DataFrame
        DataFrame with the new data, containing the columns 'data' (date) and 'hora' (time).
    table_name : str or pandas.DataFrame
        If `use_df=False`, this should be the name of the table in the database.
        If `use_df=True`, this should be a reference DataFrame.
    use_df : bool, optional, default=False
        Defines whether `table_name` is a database table (`False`) 
        or a DataFrame (`True`).

    Returns
    -------
    bool
        True if `new_df` has a newer date/time than the last stored record,
        False otherwise.
    """
       
    if use_df == False:
        query = f"SELECT data, hora FROM {table_name} ORDER BY data, hora DESC LIMIT 1"
        last_update = pd.read_sql(query, engine)
        if last_update.empty:
            return True
        last_date = pd.to_datetime(last_update['data'].iloc[0],  format="mixed", dayfirst=True).date()
        last_time = pd.to_datetime(last_update['hora'].iloc[0], format="%H:%M:%S").time()

    else:
        last_update = table_name
        if last_update is None or last_update.empty:
            return True
        last_date = pd.to_datetime(last_update['data'].iloc[-1],  format="mixed", dayfirst=True).date()
        last_time = pd.to_datetime(last_update['hora'].iloc[-1], format="%H:%M:%S").time()


    new_df = rename_columns(new_df)
    new_df = new_df.sort_values(by=['data', 'hora'], ascending = False, inplace=False).head(1)
    new_date = pd.to_datetime(new_df.iloc[0]['data'], format="mixed", dayfirst=True).date()
    new_time = pd.to_datetime(new_df.iloc[0]['hora'], format="%H:%M:%S").time()

    new_datetime = datetime.combine(new_date, new_time)
    last_datetime = datetime.combine(last_date, last_time)

    if (new_datetime > last_datetime):
        return True
    else:
        return False




   

