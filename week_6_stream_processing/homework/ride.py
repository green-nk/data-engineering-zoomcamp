from datetime import datetime


class Ride:
    def __init__(self, data):
        self.vendor_id = int(data[0])
        self.lpep_pickup_datetime = datetime.strptime(data[1], "%Y-%m-%d %H:%M:%S"),
        self.lpep_dropoff_datetime = datetime.strptime(data[2], "%Y-%m-%d %H:%M:%S"),
        self.store_and_fwd_flag = str(data[3])
        self.rate_code_id = int(data[4])
        self.pu_location_id = int(data[5])
        self.do_location_id = int(data[6])
        self.passenger_count = int(data[7])
        self.trip_distance = float(data[8])
        self.fare_amount = float(data[9])
        self.extra = float(data[10])
        self.mta_tax = float(data[11])
        self.tip_amount = float(data[12])
        self.tolls_amount = float(data[13])
        self.ehail_fee = data[14]
        self.improvement_surcharge = float(data[15])
        self.total_amount = float(data[16])
        self.payment_type = int(data[17])
        self.trip_type = int(data[18])
        self.congestion_surcharge = data[19]
