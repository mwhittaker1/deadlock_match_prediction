import pandas as pd

#Accepts dict and saves to a .xlsx with the items name.

def to_xlsx(file, fname):
    df = pd.DataFrame(file)
    df.to_excel(f'{fname}.xlsx', index=False)
    print(f"{df} passed to .xlsx")


def main():
    data = {
    'name': ['John', 'Alice', 'Bob'],
    'age': [30, 25, 35],
    'city': ['New York', 'Los Angeles', 'Chicago']
    }
    df = pd.DataFrame(data)
    #to_xlsx(df)

#if __name__ == main():
#    main()