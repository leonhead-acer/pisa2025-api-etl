import pandas as pd

def extract_student_participants(filepath: str) -> pd.DataFrame:
    """
       Simple Extract Function in Python with Error Handling
       :param filepath: str, file path to CSV data
       :output: pandas dataframe, extracted from CSV data
    """
    try:
        # Read the CSV file and store it in a dataframe
        df = pd.read_excel(filepath,sheet_name='students')
        df = df.rename(columns = {'username':'login'})

    # Handle exception if any of the files are missing
    except FileNotFoundError as e:
        print(f"Error: {e}")

    # Handle any other exceptions
    except Exception as e:
        print(f"Error: {e}")

    finally:
        print("Student participant data exported")
        return df


def extract_country_codes(filepath: str) -> pd.DataFrame:
    """
       Simple Extract Function in Python with Error Handling
       :param filepath: str, file path to CSV data
       :output: pandas dataframe, extracted from CSV data
    """
    try:
        # Read the CSV file and store it in a dataframe
        df = pd.read_excel(filepath).astype({'isocntcd': str})
        df['isocntcd'] = df['isocntcd'].apply(lambda x: x.zfill(3))
        df = df.loc[:,['isocntcd','isoalpha3','isoname']].drop_duplicates(subset = ['isocntcd'],keep = 'first')

    # Handle exception if any of the files are missing
    except FileNotFoundError as e:
        print(f"Error: {e}")

    # Handle any other exceptions
    except Exception as e:
        print(f"Error: {e}")

    finally:
        print("Country code data exported")
        return df