
from sqlalchemy import create_engine
import pandas as pd

class Customers:
    def __init__(self, db_path='S30_db.db'):
        # Initialize the database connection
        self.engine = create_engine(f'sqlite:///{db_path}')
        print("Database connection initialized.")

    def create_df(self):
        try:
            # Load a table directly as a DataFrame
            print("created dataframes...")
            sales_df = pd.read_sql_table('sales', self.engine)
            orders_df = pd.read_sql_table('orders', self.engine)
            items_df = pd.read_sql_table('items', self.engine)
            customers_df = pd.read_sql_table('customers', self.engine)
            return sales_df, orders_df, items_df, customers_df
        except Exception as e:
            return f"Creating dataframe failed due to the error: {str(e)}"

    def agg_data(self, result_df):
        try:
            filter_df = (
            result_df.groupby(['customer_id', 'age', 'item_name'])['quantity']
            .sum()
            .reset_index()
            )
            # Filtering and converting to integer
            filter_df = filter_df[filter_df['quantity'] > 0]
            filter_df['quantity'] = filter_df['quantity'].astype(int)
            return filter_df
        except Exception as e:
            return f"Aggrgating data failed due to the error: {str(e)}" 
    

    def merge_df(self, sales_df, orders_df, items_df, customers_df ):
        try:
            customer_sales = pd.merge(sales_df, customers_df, on='customer_id')
            filter_customer = customer_sales[(customer_sales['age'] >= 18) & (customer_sales['age'] <= 35)]
            customer_order = pd.merge(filter_customer, orders_df, on='sales_id')
            result_df = pd.merge(customer_order, items_df, on='item_id')
            print("merged dataframes..")
            return result_df
        except Exception as e:
            return f"Merging dataframes failed due to the error: {str(e)}"

    def transform_data(self, result_df):
        try:
            filter_df = self.agg_data(result_df)
            filter_df.rename(columns={'customer_id': 'Customer', 'age': 'Age', 'item_name': 'Item', 'quantity': 'Quantity'}, inplace=True)
            print("filtered data...")
            return filter_df
        except Exception as e:
            return f"Transformation failed due to the error: {str(e)}"

    def write_csv(self, filter_df):
        try:
            print(len(filter_df)) #70
            filter_df.to_csv('output\\filtered_data_pandas.csv', index=False, sep=';')
            print('successfully written to csv..')
        except Exception as e:
            return f"Failed to write to csv: {str(e)}"

    def close(self):
        """
        Close the database connection.
        """
        try:
            self.engine.dispose()
            print("Database connection closed.")
        except Exception as e:
            return f"Failed to cloase the connection: {str(e)}"

    def main(self):
        try:
            print("main started..")
            sales_df, orders_df, items_df, customers_df  = self.create_df()
            result_df = self.merge_df(sales_df, orders_df, items_df, customers_df )
            filter_df = self.transform_data(result_df)
            self.write_csv(filter_df)
            self.close()
            print("main completed..")
        except Exception as e:
            return f"Executing main failed: {str(e)}"


# Creating an instance and calling the main method
c = Customers()
c.main()


