import pandas as pd

#Accepts dict and saves to a .xlsx with the items name.

def to_xlsx(file):
    #name = "{file} output_with_spacing.xlsx"
    #print(f"printing file {file} of type {type(file)} ")
    df = pd.DataFrame(file)
    df.to_excel('output.xlsx', index=False)
    #print(f"{file} passed to .xlsx at {name}")

