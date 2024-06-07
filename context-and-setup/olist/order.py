import pandas as pd
import numpy as np
from olist.utils import haversine_distance
from olist.data import Olist


class Order:
    '''
    DataFrames containing all orders as index,
    and various properties of these orders as columns
    '''
    def __init__(self):
        # Assign an attribute ".data" to all new instances of Order
        self.data = Olist().get_data()

    def get_wait_time(self, is_delivered=True):
        """
        Returns a DataFrame with:
        [order_id, wait_time, expected_wait_time, delay_vs_expected, order_status]
        and filters out non-delivered orders unless specified
        """
        df = self.data['orders'].copy()

        if is_delivered:
            df = df[df.order_status == 'delivered']

        date_columns = [col for col in df.columns if 'date' in col or 'timestamp' in col or '_at' in col]

        for col in date_columns:
            df[col] = pd.to_datetime(df[col])

        wait_time = df['order_delivered_customer_date'] - df['order_purchase_timestamp']
        df['wait_time'] = wait_time/np.timedelta64(1, 'D')

        expected_wait_time = df['order_estimated_delivery_date'] - df['order_purchase_timestamp']
        df['expected_wait_time'] = expected_wait_time/np.timedelta64(1, 'D')

        delay_vs_expected = df['order_delivered_customer_date'] - df['order_estimated_delivery_date']
        df['delay_vs_expected'] = delay_vs_expected/np.timedelta64(1, 'D')


        mask_ontime = df['delay_vs_expected'] < 0
        df.loc[mask_ontime, 'delay_vs_expected'] = 0

        return df.iloc[:, [0, 2, -3, -2, -1]]


        # Hint: Within this instance method, you have access to the instance of the class Order in the variable self, as well as all its attributes


    def get_review_score(self):
        """
        Returns a DataFrame with:
        order_id, dim_is_five_star, dim_is_one_star, review_score
        """
        df = self.data['order_reviews'].copy()

        dim_is_one_star = df['review_score'].map(lambda score: int(score == 1))
        dim_is_five_star = df['review_score'].map(lambda score: int(score == 5))
        df['dim_is_five_star'] = dim_is_five_star
        df['dim_is_one_star'] = dim_is_one_star

        return df.iloc[:, [1,2,-2,-1]]

    def get_number_products(self):
        """
        Returns a DataFrame with:
        order_id, number_of_products
        """
        merged = pd.merge(self.data['orders'], self.data['order_items'], on='order_id', how='inner')

        return merged.groupby(['order_id'])['product_id'].nunique().reset_index().rename(columns={'product_id': 'number_of_products'})

    def get_number_sellers(self):
        """
        Returns a DataFrame with:
        order_id, number_of_sellers
        """
        merged = pd.merge(self.data['orders'], self.data['order_items'], on='order_id', how='inner')

        return merged.groupby(['order_id'])['seller_id'].nunique().reset_index().rename(columns={'seller_id': 'number_of_sellers'})



    def get_price_and_freight(self):
        """
        Returns a DataFrame with:
        order_id, price, freight_value
        """
        order_items = self.data['order_items'].copy()
        return order_items.groupby(['order_id'])[['price', 'freight_value']].agg('sum').reset_index()

    # Optional
    def get_distance_seller_customer(self):
        """
        Returns a DataFrame with:
        order_id, distance_seller_customer
        """
        # $CHALLENGIFY_BEGIN

        # import data
        data = self.data
        orders = data['orders']
        order_items = data['order_items']
        sellers = data['sellers']
        customers = data['customers']

        # Since one zip code can map to multiple (lat, lng), take the first one
        geo = data['geolocation']
        geo = geo.groupby('geolocation_zip_code_prefix', as_index=False).first()

        # Merge geo_location for sellers
        sellers_mask_columns = [
            'seller_id', 'seller_zip_code_prefix', 'geolocation_lat', 'geolocation_lng'
        ]

        sellers_geo = sellers.merge(
            geo,
            how='left',
            left_on='seller_zip_code_prefix',
            right_on='geolocation_zip_code_prefix')[sellers_mask_columns]

        # Merge geo_location for customers
        customers_mask_columns = ['customer_id', 'customer_zip_code_prefix', 'geolocation_lat', 'geolocation_lng']

        customers_geo = customers.merge(
            geo,
            how='left',
            left_on='customer_zip_code_prefix',
            right_on='geolocation_zip_code_prefix')[customers_mask_columns]

        # Match customers with sellers in one table
        customers_sellers = customers.merge(orders, on='customer_id')\
            .merge(order_items, on='order_id')\
            .merge(sellers, on='seller_id')\
            [['order_id', 'customer_id','customer_zip_code_prefix', 'seller_id', 'seller_zip_code_prefix']]

        # Add the geoloc
        matching_geo = customers_sellers.merge(sellers_geo, on='seller_id').merge(customers_geo, on='customer_id', suffixes=('_seller', '_customer'))

        # Remove na()
        matching_geo = matching_geo.dropna()

        matching_geo.loc[:, 'distance_seller_customer'] =\
            matching_geo.apply(lambda row:
                               haversine_distance(row['geolocation_lng_seller'],
                                                  row['geolocation_lat_seller'],
                                                  row['geolocation_lng_customer'],
                                                  row['geolocation_lat_customer']),
                               axis=1)
        # Since an order can have multiple sellers,
        # return the average of the distance per order
        order_distance = matching_geo.groupby('order_id', as_index=False).agg({'distance_seller_customer':'mean'})

        return order_distance
        # $CHALLENGIFY_END

    def get_training_data(self, is_delivered=True, with_distance_seller_customer=False):
        """
        Returns a clean DataFrame (without NaN), with the all following columns:
        ['order_id', 'wait_time', 'expected_wait_time', 'delay_vs_expected',
        'order_status', 'dim_is_five_star', 'dim_is_one_star', 'review_score',
        'number_of_products', 'number_of_sellers', 'price', 'freight_value',
        'distance_seller_customer']
        """
        training = self.get_wait_time(is_delivered) \
            .merge(self.get_review_score(), on='order_id')\
            .merge(self.get_number_products(), on='order_id')\
            .merge(self.get_number_sellers(), on='order_id')\
            .merge(self.get_price_and_freight(), on='order_id')

        if with_distance_seller_customer:
            training = training.merge(
                self.get_distance_seller_customer(), on='order_id')

        return training.dropna()
        # Hint: make sure to re-use your instance methods defined above
