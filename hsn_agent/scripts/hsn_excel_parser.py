import pandas as pd



def hsn_excel_parser(input_path: str, output_path: str) -> None:
    df = pd.read_excel(input_path, dtype=str)

    # Normalize the column names and trim every string cell of the newlines, leading and trailing spaces
    df.columns = df.columns.str.strip()
    print(df.columns)
    for c in df.select_dtypes(include="object"):
        df[c] = df[c].str.strip()

    def get_depth(r):
        lvl = r.get("Level", "") or ""
        code = r.get("HS Code", "") or ""
        s = lvl if "-" in lvl else code

    print("done with hsn_excel_parser")

if __name__ == "__main__":
    hsn_excel_parser("/home/nithin/work/llm/langchain/react/HSN-GPT/hsn-data/test.xlsx","/home/nithin/work/llm/langchain/react/HSN-GPT/hsn-data/animals_clean.xlsx" )
