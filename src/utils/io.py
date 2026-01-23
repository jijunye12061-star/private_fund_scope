import pandas as pd


def export_to_excel(df_dict, filename):
    """
    Export multiple DataFrames to different sheets in an Excel file

    Parameters:
    df_dict: Dict with sheet names as keys and DataFrames as values
    filename: str, path to save the Excel file
    """
    with pd.ExcelWriter(filename, engine='openpyxl') as writer:
        for sheet_name, df in df_dict.items():
            if isinstance(df.index[0], pd.Timestamp):
                df.index = df.index.strftime('%Y-%m-%d')
            df.to_excel(writer, sheet_name=sheet_name)