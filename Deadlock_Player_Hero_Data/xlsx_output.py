import pandas as pd

#Accepts dict and saves to a .xlsx with the items name.

def to_xlsx(file):
    df = pd.DataFrame(file)
    #name = "{file} output_with_spacing.xlsx"
    #print(f"printing file {file} of type {type(file)} ")
    df.to_excel('{file}.xlsx', index=False)
    print(f"{df} passed to .xlsx")


def main():
    return

if __name__ == main():
    main()