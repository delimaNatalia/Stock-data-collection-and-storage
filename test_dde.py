import xlwings as xw
import time

# Attach to an open Excel workbook (the one running RTD)
wb = xw.Book("ProfitRTD.xlsm")



# Select the sheet where your RTD is (replace with the actual sheet name if needed)
sheet = wb.sheets[0]

# Example: read a few RTD cells
while True:
    asset = sheet["A2"].value   # WINFUT
    ultimo = sheet["D2"].value  # Último preço
    abertura = sheet["E2"].value
    maximo = sheet["F2"].value
    minimo = sheet["G2"].value
    volume = sheet["I2"].value

    print("Ativo:", asset)
    print("Último:", ultimo)
    print("Abertura:", abertura)
    print("Máximo:", maximo)
    print("Mínimo:", minimo)
    print("Volume:", volume)

    time.sleep(2)

