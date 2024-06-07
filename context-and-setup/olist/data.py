import os
import pandas as pd

class Olist:
    def get_data(self):
        """
        This function returns a Python dict.
        Its keys should be 'sellers', 'orders', 'order_items' etc...
        Its values should be pandas.DataFrames loaded from csv files
        """
        # Hints 1: Build csv_path as "absolute path" in order to call this method from anywhere.
            # Do not hardcode your path as it only works on your machine ('Users/username/code...')
            # Use __file__ instead as an absolute path anchor independant of your usename
            # Make extensive use of `breakpoint()` to investigate what `__file__` variable is really
        # Hint 2: Use os.path library to construct path independent of Mac vs. Unix vs. Windows specificities

        # 1. get path to csv files
        project_dir = os.path.dirname(os.path.dirname(__file__))
        csv_path = os.path.join(project_dir, 'data/csv')


        # 2. create a dict with a dataframe for each csv file

        file_names = [name for name in os.listdir(csv_path) if name.endswith('.csv')]
        # file_names.remove('.keep')

        key_names = [file.removeprefix("olist_").removesuffix("_dataset.csv").removesuffix(".csv") for file in file_names if file.endswith('.csv')]

        data = {}

        for key, file in zip(key_names, file_names):
            data[key] = pd.read_csv(os.path.join(csv_path, file))

        print(data.keys())

        return data




    def ping(self):
        """
        You call ping I print pong.
        """
        print("pong")
