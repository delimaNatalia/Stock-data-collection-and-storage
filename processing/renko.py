import pandas as pd

def open_new_renko_dict(row, open_price, box_size, direction="up"):
    base_price =  open_price

    if direction == "up":
        up_box_min = open_price + box_size
        down_box_min = open_price - 2 * box_size
    else:  # "down"
        up_box_min = open_price+ 2 * box_size
        down_box_min = open_price - box_size
    new_row = {
        "ativo": row.ativo,
        "data": row.data,
        "hora": row.hora,
        "abertura": open_price,
        "fechamento": base_price,
        "maxima": base_price,
        "minima": base_price,
        "volume": row.quantidade,
        "negocios": 1,
    }

    return new_row, up_box_min, down_box_min

def build_renko_from_tick_df(trade_df, previous_df=None, box_size=60, tick =5):
    try:

        #box_size = box_size/1000
        rows = []  # renko_rows
      
        if (previous_df is not None) and (not previous_df.empty):
            
            last_row = previous_df.iloc[-1]
           
            if previous_df.iloc[-2]['abertura'] < previous_df.iloc[-2]['fechamento']:
                last_direction = 'up'
            else:
                last_direction = 'down'

            new_row = {
                "ativo": last_row.ativo,
                "data": last_row.data,
                "hora": last_row.hora,
                "abertura": last_row.abertura,
                "fechamento": last_row.fechamento,
                "maxima": last_row.maxima,
                "minima": last_row.minima,
                "volume": last_row.volume,
                "negocios": last_row.negocios,
            }
            rows.append(new_row)
            



        
        if (previous_df is None) or (previous_df.empty):
            # primeira linha
            first_row = next(trade_df.itertuples(index=False))
            
            starting_price = (first_row.preco//box_size) * box_size

            new_row, up_box_min, down_box_min = open_new_renko_dict(first_row, starting_price, box_size, direction= "up")
            rows.append(new_row)

            up_box_min = starting_price + box_size
            down_box_min = starting_price - box_size
        
        else:
            if last_direction == 'up':
                up_box_min = last_row['abertura'] + box_size
                down_box_min = last_row['abertura'] - (2* box_size)
            if last_direction == 'down':
                up_box_min = last_row['abertura'] + (2*box_size)
                down_box_min = last_row['abertura'] -  box_size  
        


        for row in trade_df.itertuples(index=False):
            price = row.preco
            qty = row.quantidade
            last = rows[-1]
            last["fechamento"] = price
            current_price = price

            # novos tijolos de alta
            while price > (up_box_min):                                   #open_price
                rows[-1]["fechamento"] = up_box_min
                rows[-1]["abertura"] = up_box_min - box_size
                new_row, up_box_min, down_box_min = open_new_renko_dict(row, up_box_min, box_size, direction= "up")
                rows.append(new_row)
                current_price += box_size


            # novos tijolos de baixa
            while price < (down_box_min):                                   #open_price
                rows[-1]["fechamento"] = down_box_min
                rows[-1]["abertura"] = down_box_min + box_size
                new_row, up_box_min, down_box_min = open_new_renko_dict(row, down_box_min, box_size, direction= "down")
                #rows[-1]["Fechamento"] = rows[-1]["Abertura"] - box_size
                rows.append(new_row)

            # atualizar último tijolo
            last["minima"] = min(price, last["minima"])
            last["maxima"] = max(price, last["maxima"])
            last["volume"] += qty
            last["negocios"] += 1


        renko_df = pd.DataFrame(rows, columns=[
            "ativo","data","hora","abertura","fechamento","maxima","minima","volume","negocios"
        ])

        renko_df["volume"] = renko_df["volume"].astype(int)
        renko_df["negocios"] = renko_df["negocios"].astype(int)

        return renko_df
    except Exception as e:
        print("Erro ao construir os tijolos renko: ", e)



def build_renko_from_tick(trade_df, box_size=60, tick_size = 5):

    #box_size = box_size/1000
    rows = []  # bricks

    # primeira linha
    first_row = next(trade_df.itertuples(index=False))
    starting_price = (first_row.preco//box_size) * box_size

    new_row, up_box_min, down_box_min = open_new_renko_dict(first_row, starting_price, box_size, direction= "up")
    rows.append(new_row)

    up_box_min = starting_price + box_size
    down_box_min = starting_price - box_size

    for row in trade_df.itertuples(index=False):
        price = row.preco
        qty = row.quantidade
        last = rows[-1]
        last["Fechamento"] = price
        current_price = price

        # novos tijolos de alta
        while price > (up_box_min):                                   #open_price
            rows[-1]["Fechamento"] = up_box_min
            rows[-1]["Abertura"] = up_box_min - box_size
            new_row, up_box_min, down_box_min = open_new_renko_dict(row, up_box_min, box_size, direction= "up")
            rows.append(new_row)
            current_price += box_size


        # novos tijolos de baixa
        while price < (down_box_min):                                   #open_price
            rows[-1]["Fechamento"] = down_box_min
            rows[-1]["Abertura"] = down_box_min + box_size
            new_row, up_box_min, down_box_min = open_new_renko_dict(row, down_box_min, box_size, direction= "down")
            #rows[-1]["Fechamento"] = rows[-1]["Abertura"] - box_size
            rows.append(new_row)

        # atualizar último tijolo
        last["Minima"] = min(price, last["Minima"])
        last["Maxima"] = max(price, last["Maxima"])
        last["Volume"] += qty
        last["Negocios"] += 1


    return pd.DataFrame(rows, columns=[
        "Ativo","Data","Hora","Abertura","Fechamento","Maxima","Minima","Volume","Negocios"
    ])



