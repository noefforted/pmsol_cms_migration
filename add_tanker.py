import pandas as pd

def tanker_migrate():
    df_employee = pd.read_excel("data_source/employee_tanker.xlsx")
    # print(f"Column names: {df_employee.columns}")
    

if __name__ == "__main__":
    tanker_migrate()