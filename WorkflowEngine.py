import pandas as pd
import geopandas as gpd
from geopy.distance import geodesic as GD
import snowflake.connector
import matplotlib.pyplot as plt
import seaborn as sns

class DataExtactionEngine:
    def __init__(self):
        pass

    def get_snowflake_SN2BG(self,username,password,sensor_list, gtw_list, date_range):
        '''
        Extracts SN to BG data from Snowflake based on the provided sensor list, gateway list, and date range.
        
        Parameters:
        - username: Snowflake username for authentication.
        - password: Snowflake password for authentication.
        - sensor_list: List of sensor IDs.
        - gtw_list: List of gateway IDs (either as strings or integers).
        - date_range: List of two dates specifying the start and end of the range.
        
        Returns:
        - A pandas DataFrame containing the extracted data.
        '''
        operator = ['>=','<=']

        # Convert sensor list to a SQL-compatible string with LIKE conditions
        sensor_like_str = " OR ".join([f"ENDDEVICE:id LIKE '%{sensor_id}%'" for sensor_id in sensor_list])
       
        if all(isinstance(bg_id, int) for bg_id in gtw_list):    # If all elements in gtw_list are integers
            bg_equals_str = " OR ".join([f"GATEWAYS[0]:timestamp = '{bg_id}'" for bg_id in gtw_list])

        elif all(isinstance(bg_id, str) for bg_id in gtw_list):  # If all elements in gtw_list are strings
            bg_equals_str = " OR ".join([f"GATEWAYS[0]:id LIKE '%{bg_id}%'" for bg_id in gtw_list])

        else:    # If gtw_list contains mixed types or is not int/str
            bg_equals_str = ""

        time_str =  " AND ".join([f"TIME {ops} '{date}'" for ops, date in zip(operator, date_range)])


        # SQL query template with placeholders
        query = f"""
        SELECT 
            ENDDEVICE:id::string AS sensor_id,
            ENDDEVICE:location.latitude::float AS sensor_lat,
            ENDDEVICE:location.longitude::float AS sensor_long,
            TIME AS Timestamp,
            GATEWAYS[0]:id::string AS bgtw_id,
            GATEWAYS[0]:timestamp::string AS mgtw_nr,
            GATEWAYS[0]:rssi::float AS bgtw_rssi,
            GATEWAYS[0]:snr::float AS bgtw_snr,  
            FRAMECOUNT AS frameCount,
            FRAMEPORT AS framePort
        FROM 
            DRYAD_HQ.DRYAD.SENSOR_MESSAGES_ID
        WHERE 
            ({sensor_like_str})
            AND ({time_str})
            AND ({bg_equals_str})
            AND frameport != 99

        ORDER BY 
            time DESC
            
        ;"""

        # Connect to Snowflake
        con = snowflake.connector.connect(
            user=username,
            password=password,
            account='bo02374.eu-central-1',
            warehouse='DRYAD_WH'
        )

        # Execute the query
        cur = con.cursor()
        cur.execute(query)

        # Fetch all results into a DataFrame
        df = cur.fetch_pandas_all()

        if df.empty:
            print("Error: The query returned an empty DataFrame. No data found for the given parameters.")
        else:
            # If the DataFrame is not empty, continue processing
            df.columns = df.columns.str.lower()

            return df
    



    def get_snowflake_SN2MG(self,username,password,sensor_list,gtw_list,date_range):
        '''
        Extracts SN to MG data from Snowflake based on the provided sensor list and date range.
        
        Parameters:
        - username: Snowflake username for authentication.
        - password: Snowflake password for authentication.
        - sensor_list: List of sensor IDs.
        - date_range: List of two dates specifying the start and end of the range.
        
        Returns:
        - A pandas DataFrame containing the extracted data.
        '''

        operator = ['>','<']

        # Convert sensor list to a SQL-compatible string with LIKE conditions
        sensor_like_str = " OR ".join([f"ENDDEVICE:id LIKE '%{sensor_id}%'" for sensor_id in sensor_list])

        if all(isinstance(mg_id, int) for mg_id in gtw_list):    # If all elements in gtw_list are integers
            mg_equals_str = " OR ".join([f"GATEWAYS[0]:timestamp = '{mg_id}'" for mg_id in gtw_list])


        else:    # If gtw_list contains mixed types or is not int/str
            print('Error: Incorrect Entry for Gateway ID')

        time_str =  " AND ".join([f"TIME {ops} '{date}'" for ops, date in zip(operator, date_range)])


        # SQL query template with placeholders
        query = f"""
        SELECT 
            ENDDEVICE:id::string AS sensor_id,
            ENDDEVICE:location.latitude::float AS sensor_lat,
            ENDDEVICE:location.longitude::float AS sensor_long,
            TIME AS Timestamp,
            GATEWAYS[0]:id::string AS bgtw_id,
            GATEWAYS[0]:timestamp::string AS mgtw_nr,
            GATEWAYS[0]:rssi::float AS bgtw_rssi,
            GATEWAYS[0]:snr::float AS bgtw_snr,  
            FRAMECOUNT AS frameCount,
            FRAMEPORT AS framePort
        FROM 
            DRYAD_HQ.DRYAD.SENSOR_MESSAGES_ID
        WHERE 
            ({sensor_like_str})
            AND ({time_str})
            AND ({mg_equals_str})
            AND frameport != 99
        ORDER BY 
            time DESC
            
        ;"""

        # Connect to Snowflake
        con = snowflake.connector.connect(
            user=username,
            password=password,
            account='bo02374.eu-central-1',
            warehouse='DRYAD_WH'
        )

        # Execute the query
        cur = con.cursor()
        cur.execute(query)

        # Fetch all results into a DataFrame
        df = cur.fetch_pandas_all()

        if df.empty:
            print("Error: The query returned an empty DataFrame. No data found for the given parameters.")
        else:
            # If the DataFrame is not empty, continue processing
            df.columns = df.columns.str.lower()

            required_columns = ['sensor_long','sensor_lat','mgtw_nr']

            for col in required_columns:
                df[col] = df[col].astype(str)

            return df



class DataCleaningEngine:
    def __init__(self):
        pass

    def SN2MG_df_generator(self):
        '''
        Generates a DataFrame mapping sensor IDs to their corresponding mesh gateway information.                  

        Returns:                                                                                                    
        - A pandas DataFrame containing sensor IDs, packet numbers, mesh gateway IDs, and their locations.
        '''        
        sensor_packs = {
            "sensor_id":   ["sn-silvav3n34", "sn-silvav3n920", "sn-silvav3n1093", "sn-silvav3n507", "sn-silvav3n1049", "sn-silvav3n130", "sn-silvav3n1000", # Gen 2
                            "sn-silvav3n1331", "sn-silvav3n1108", "sn-silvav3n166", "sn-silvav3n812", "sn-silvav3n918", "sn-silvav3n801", "sn-silvav3n822",
                            "sn-silvav3n10766", "sn-silvav3n1044", "sn-silvav3n1154", "sn-silvav3n111", "sn-silvav3n126", "sn-silvav3n276", "sn-silvav3n787", "sn-silvav3n10674", "sn-silvav3n360", "sn-silvav3n1116",
                            "sn-silvav3n726", "sn-silvav3n496", "sn-silvav3n10319", "sn-silvav3n1193", "sn-silvav3n213", "sn-silvav3n342", "sn-silvav3n1077", "sn-silvav3n154", "sn-silvav3n1083", "sn-silvav3n1213", "sn-silvav3n275",
                            "sn-silvav3n1076", "sn-silvav3n10754", "sn-silvav3n85", "sn-silvav3n1203", "sn-silvav3n62", "sn-silvav3n903",
                            "sn-silvav3n9494", "sn-silvav3n4", "sn-silvav3n221", "sn-silvav3n926", "sn-silvav3n8905", "sn-silvav3n294", "sn-silvav3n474", "sn-silvav3n491",
                            "sn-silvav3n921", "sn-silvav3n331", "sn-silvav3n1163", "sn-silvav3n1205", "sn-silvav3n688", "sn-silvav3n170", "sn-silvav3n990", "sn-silvav3n1192", "sn-silvav3n13",
                            "sn-silvav3n34", "sn-silvav3n920", "sn-silvav3n1093", "sn-silvav3n507", "sn-silvav3n1049", "sn-silvav3n130", "sn-silvav3n1000", # Gen 3
                            "sn-silvav3n1331", "sn-silvav3n1108", "sn-silvav3n166", "sn-silvav3n812", "sn-silvav3n918", "sn-silvav3n801", "sn-silvav3n822",
                            "sn-silvav3n10766", "sn-silvav3n1044", "sn-silvav3n1154", "sn-silvav3n111", "sn-silvav3n126", "sn-silvav3n276", "sn-silvav3n787", "sn-silvav3n10674", "sn-silvav3n360", "sn-silvav3n1116",
                            "sn-silvav3n726", "sn-silvav3n496", "sn-silvav3n10319", "sn-silvav3n1193", "sn-silvav3n213", "sn-silvav3n342", "sn-silvav3n1077", "sn-silvav3n154", "sn-silvav3n1083", "sn-silvav3n1213", "sn-silvav3n275",
                            "sn-silvav3n1076", "sn-silvav3n10754", "sn-silvav3n85", "sn-silvav3n1203", "sn-silvav3n62", "sn-silvav3n903",
                            "sn-silvav3n9494", "sn-silvav3n4", "sn-silvav3n221", "sn-silvav3n926", "sn-silvav3n8905", "sn-silvav3n294", "sn-silvav3n474", "sn-silvav3n491",
                            "sn-silvav3n921", "sn-silvav3n331", "sn-silvav3n1163", "sn-silvav3n1205", "sn-silvav3n688", "sn-silvav3n170", "sn-silvav3n990", "sn-silvav3n1192", "sn-silvav3n13"],

            "mgtw_id":  ["mg2-9","mg2-9","mg2-9","mg2-9","mg2-9","mg2-9","mg2-9", # Gen 2
                            "mg2-21","mg2-21","mg2-21","mg2-21","mg2-21","mg2-21","mg2-21",
                        "mg2-24","mg2-24","mg2-24","mg2-24","mg2-24","mg2-24","mg2-24","mg2-24","mg2-24","mg2-24",
                        "mg2-26","mg2-26","mg2-26","mg2-26","mg2-26","mg2-26","mg2-26","mg2-26","mg2-26","mg2-26","mg2-26",
                        "mg2-34","mg2-34","mg2-34","mg2-34","mg2-34","mg2-34",
                        "mg2-37","mg2-37","mg2-37","mg2-37","mg2-37","mg2-37","mg2-37","mg2-37",
                        "mg2-19","mg2-19","mg2-19","mg2-19","mg2-19","mg2-19","mg2-19","mg2-19","mg2-19",
                        "mg3-9","mg3-9","mg3-9","mg3-9","mg3-9","mg3-9","mg3-9", # Gen 3
                        "mg3-7","mg3-7","mg3-7","mg3-7","mg3-7","mg3-7","mg3-7",
                        "mg3-12","mg3-12","mg3-12","mg3-12","mg3-12","mg3-12","mg3-12","mg3-12","mg3-12","mg3-12",
                        "mg3-08","mg3-08","mg3-08","mg3-08","mg3-08","mg3-08","mg3-08","mg3-08","mg3-08","mg3-08","mg3-08",
                        "mg3-10","mg3-10","mg3-10","mg3-10","mg3-10","mg3-10",
                        "mg3-11","mg3-11","mg3-11","mg3-11","mg3-11","mg3-11","mg3-11","mg3-11",
                        "mg3-06","mg3-06","mg3-06","mg3-06","mg3-06","mg3-06","mg3-06","mg3-06","mg3-06"],

            "mgtw_nr": ["30172","30172","30172","30172","30172","30172","30172", # Gen 2
                        "31416","31416","31416","31416","31416","31416","31416",
                        "31419","31419","31419","31419","31419","31419","31419","31419","31419","31419",
                        "31421","31421","31421","31421","31421","31421","31421","31421","31421","31421","31421",
                        "31429","31429","31429","31429","31429","31429",
                        "31432","31432","31432","31432","31432","31432","31432","31432",
                        "31414","31414","31414","31414","31414","31414","31414","31414","31414",
                        "2057","2057","2057","2057","2057","2057","2057",  # Gen 3
                        "2071","2071","2071","2071","2071","2071","2071",
                        "2050","2050","2050","2050","2050","2050","2050","2050","2050","2050",
                        "2054","2054","2054","2054","2054","2054","2054","2054","2054","2054","2054",
                        "2072","2072","2072","2072","2072","2072",
                        "2064","2064","2064","2064","2064","2064","2064","2064",
                        "2058","2058","2058","2058","2058","2058","2058","2058","2058"],

            "mgtw_lat":    ["52.8563339","52.8563339","52.8563339","52.8563339","52.8563339","52.8563339","52.8563339", # Gen 2
                            "52.8562724","52.8562724","52.8562724","52.8562724","52.8562724","52.8562724","52.8562724",
                            "52.8576493","52.8576493","52.8576493","52.8576493","52.8576493","52.8576493","52.8576493","52.8576493","52.8576493","52.8576493",
                            "52.8597487","52.8597487","52.8597487","52.8597487","52.8597487","52.8597487","52.8597487","52.8597487","52.8597487","52.8597487","52.8597487",
                            "52.8611659","52.8611659","52.8611659","52.8611659","52.8611659","52.8611659",
                            "52.8613546","52.8613546","52.8613546","52.8613546","52.8613546","52.8613546","52.8613546","52.8613546",
                            "52.85566","52.85566","52.85566","52.85566","52.85566","52.85566","52.85566","52.85566","52.85566",
                            "52.8563339","52.8563339","52.8563339","52.8563339","52.8563339","52.8563339","52.8563339", # Gen 3
                            "52.8562724","52.8562724","52.8562724","52.8562724","52.8562724","52.8562724","52.8562724",
                            "52.8576493","52.8576493","52.8576493","52.8576493","52.8576493","52.8576493","52.8576493","52.8576493","52.8576493","52.8576493",
                            "52.8597487","52.8597487","52.8597487","52.8597487","52.8597487","52.8597487","52.8597487","52.8597487","52.8597487","52.8597487","52.8597487",
                            "52.8611659","52.8611659","52.8611659","52.8611659","52.8611659","52.8611659",
                            "52.8613546","52.8613546","52.8613546","52.8613546","52.8613546","52.8613546","52.8613546","52.8613546",
                            "52.85566","52.85566","52.85566","52.85566","52.85566","52.85566","52.85566","52.85566","52.85566"],

            "mgtw_long": ["13.7961379","13.7961379","13.7961379","13.7961379","13.7961379","13.7961379","13.7961379", # Gen 2
                            "13.8082174","13.8082174","13.8082174","13.8082174","13.8082174","13.8082174","13.8082174",
                            "13.8149841","13.8149841","13.8149841","13.8149841","13.8149841","13.8149841","13.8149841","13.8149841","13.8149841","13.8149841",
                            "13.8230774","13.8230774","13.8230774","13.8230774","13.8230774","13.8230774","13.8230774","13.8230774","13.8230774","13.8230774","13.8230774",
                            "13.8295595","13.8295595","13.8295595","13.8295595","13.8295595","13.8295595",
                            "13.8356098","13.8356098","13.8356098","13.8356098","13.8356098","13.8356098","13.8356098","13.8356098",
                            "13.80276","13.80276","13.80276","13.80276","13.80276","13.80276","13.80276","13.80276","13.80276",
                            "13.7961379","13.7961379","13.7961379","13.7961379","13.7961379","13.7961379","13.7961379", # Gen 3
                            "13.8082174","13.8082174","13.8082174","13.8082174","13.8082174","13.8082174","13.8082174",
                            "13.8149841","13.8149841","13.8149841","13.8149841","13.8149841","13.8149841","13.8149841","13.8149841","13.8149841","13.8149841",
                            "13.8230774","13.8230774","13.8230774","13.8230774","13.8230774","13.8230774","13.8230774","13.8230774","13.8230774","13.8230774","13.8230774",
                            "13.8295595","13.8295595","13.8295595","13.8295595","13.8295595","13.8295595",
                            "13.8356098","13.8356098","13.8356098","13.8356098","13.8356098","13.8356098","13.8356098","13.8356098",
                            "13.80276","13.80276","13.80276","13.80276","13.80276","13.80276","13.80276","13.80276","13.80276"]
            

            }
        
        mesh_df = pd.DataFrame(sensor_packs, columns= ["sensor_id","mgtw_id", "mgtw_nr", "mgtw_lat", "mgtw_long"])

        return mesh_df
    

    def process_sensor_string(self, value):
        '''
        Processes the sensor ID string to retain only the first two parts.
        
        Parameters:
        - value: Sensor ID string to process.
        
        Returns:
        - Processed sensor ID string.
        '''
        parts = value.split('-')

        return '-'.join(parts[:2])

    def process_bgtw_string(self, value):
        '''
        Processes the gateway ID string to retain specific parts based on the prefix.
        
        Parameters:
        - value: Gateway ID string to process.
        
        Returns:
        - Processed gateway ID string.
        '''
        
        if value.startswith('bg3'):  # Check if the value starts with 'bg3'
            parts = value.split('-')
            return '-'.join(parts[:3])  # Keep the first three parts
        
        elif value.startswith('bg'):  # Check if the value starts with 'bg'
            parts = value.split('-')
            return '-'.join(parts[:2])  # Keep the first two parts

        return value  # Return the value as is if no conditions match



    def clean_SN2BG(self, df):
        '''
        Cleans the DataFrame by processing the sensor and gateway ID strings.
        
        Parameters:
        - df: pandas DataFrame to clean.
        
        Returns:
        - Cleaned pandas DataFrame.        
        '''
        df['sensor_id'] = df['sensor_id'].apply(self.process_sensor_string)
        df['bgtw_id'] = df['bgtw_id'].apply(self.process_bgtw_string)

        return df  
    
    def clean_SN2Mesh(self, df):
        '''
        Cleans and processes the SN to MG data based on the provided packet number.
        
        Parameters:
        - df: pandas DataFrame containing the raw data.
        
        Returns:
        - A cleaned and merged pandas DataFrame with sensor and mesh gateway data.
        '''
        sn2mesh_df = self.SN2MG_df_generator()

        # Assign mesh list values to 'mgtw.nr' and process 'endDevice.id'
        df['sensor_id'] = df['sensor_id'].apply(self.process_sensor_string)
        df['bgtw_id'] = df['bgtw_id'].apply(self.process_bgtw_string)

        # Filter DataFrame by matching 'mgtw.nr' with the mesh DataFrame
        filtered_df = df.loc[df['mgtw_nr'].isin(sn2mesh_df['mgtw_nr'].tolist())].copy()

        # Merge sensor data with mesh information
        merged_df = pd.merge(filtered_df, sn2mesh_df, on=["sensor_id", "mgtw_nr"], how="left")
        merged_df.drop(['mgtw_nr'], axis=1, inplace=True)

        return merged_df  

class DataSummaryEngine:
    def __init__(self):
        self.data_cleaning_engine = DataCleaningEngine()


    def count_pckt_error_SN2BG(self, temp_df, freq):
        '''
        Counts the missing packets for SN to BG data by comparing frame counts.
        
        Parameters:
        - temp_df: pandas DataFrame containing the raw data.
        
        Returns:
        - A pandas DataFrame with the number of missing packets for each sensor and gateway pair per day.
        '''
        
        df = temp_df[temp_df['frameport'] != 99].copy()

        # Reset the index to make sure 'timestamp' becomes a column if it's the index
        df.reset_index(inplace=True)
        
        # Ensure 'timestamp' is in datetime format
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        
        # Extract the date from the 'timestamp' column to avoid duplication
        df['date'] = df['timestamp'].dt.date
        
        missing_packets_dict = {}
        
        grouped = df.groupby(['sensor_id', 'bgtw_id', 'date'])
        for (sensor_id, bgtw_id, date), group in grouped:
            group = group.sort_values('timestamp')  # Ensure the group is sorted by timestamp
            count = group['framecount'].iloc[0]  # Start with the first framecount
            missing = 0

            for i in range(1, len(group)):
                current_framecount = group['framecount'].iloc[i]
                previous_framecount = group['framecount'].iloc[i - 1]
                
                # Handle the reset case: when the current framecount is lower than the previous
                if current_framecount < previous_framecount:
                    count = current_framecount  # Reset the count
                    if count == 4:
                        missing += 1
                    else:
                        difference = current_framecount - 4 - 1
                        missing += difference
                elif current_framecount == previous_framecount + 1:
                    count += 1  # Increment count if the frame count sequence is correct
                else:
                    difference = current_framecount - previous_framecount - 1
                    missing += difference
                    count = current_framecount


            missing_packets_dict[(sensor_id, bgtw_id, date)] = int(missing)
    
        # Convert the dictionary to a DataFrame
        missing_df = pd.DataFrame([
            {'sensor_id': k[0], 'bgtw_id': k[1], 'timestamp': k[2], 'missing_pckts': v}
            for k, v in missing_packets_dict.items()
        ])
        
        # Set 'timestamp' as the index and ensure it's a DatetimeIndex
        missing_df.set_index('timestamp', inplace=True)
        
        # Ensure that the index is a DatetimeIndex (critical for pd.Grouper)
        missing_df.index = pd.to_datetime(missing_df.index)

        # Define grouping columns
        group_cols = ['sensor_id', 'bgtw_id']

        # Group data by 'sensor_id', 'bgtw_id', and resample by the specified frequency, then aggregate metrics
        summary_df = missing_df.groupby(group_cols + [pd.Grouper(freq=freq)]).agg(
            missing_pckts=('missing_pckts', 'sum'),
        ).reset_index()
        
        return summary_df

    def count_pckt_error_SN2MG(self, temp_df, freq):
        '''
        Counts the missing packets for SN to MG data by comparing frame counts.
        
        Parameters:
        - temp_df: pandas DataFrame containing the raw data.
        - freq: the frequency at which you want to resample the data (e.g., 'D' for daily).
        
        Returns:
        - A pandas DataFrame with the number of missing packets for each sensor and mesh gateway pair per frequency interval.
        '''
        # Filter the dataframe where 'frameport' is not equal to 99
        df = temp_df[temp_df['frameport'] != 99].copy()

        # Reset the index to make sure 'timestamp' becomes a column if it's the index
        df.reset_index(inplace=True)
        
        # Ensure 'timestamp' is in datetime format
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        
        # Extract the date from the 'timestamp' column to avoid duplication
        df['date'] = df['timestamp'].dt.date
        
        missing_packets_dict = {}
        
        # Group by sensor_id, mgtw_id, and date
        grouped = df.groupby(['sensor_id', 'mgtw_id', 'date'])
        for (sensor_id, mgtw_id, date), group in grouped:
            group = group.sort_values('timestamp')  # Ensure the group is sorted by timestamp
            count = group['framecount'].iloc[0]  # Start with the first framecount
            missing = 0

            for i in range(1, len(group)):
                current_framecount = group['framecount'].iloc[i]
                previous_framecount = group['framecount'].iloc[i - 1]
                
                # Handle the reset case: when the current framecount is lower than the previous
                if current_framecount < previous_framecount:
                    count = current_framecount  # Reset the count
                    if count == 4:
                        missing += 1
                    else:
                        difference = current_framecount - 4 - 1
                        missing += difference

                elif current_framecount == previous_framecount + 1:
                    count += 1  # Increment count if the frame count sequence is correct
                else:
                    difference = current_framecount - previous_framecount - 1
                    missing += difference
                    count = current_framecount

            missing_packets_dict[(sensor_id, mgtw_id, date)] = int(missing)

        # Convert the dictionary to a DataFrame
        missing_df = pd.DataFrame([
            {'sensor_id': k[0], 'mgtw_id': k[1], 'timestamp': pd.to_datetime(k[2]), 'missing_pckts': v}
            for k, v in missing_packets_dict.items()
        ])

        # Set 'timestamp' as the index and ensure it's a DatetimeIndex
        missing_df.set_index('timestamp', inplace=True)
        
        # Ensure that the index is a DatetimeIndex (critical for pd.Grouper)
        missing_df.index = pd.to_datetime(missing_df.index)

        # Define grouping columns
        group_cols = ['sensor_id', 'mgtw_id']

        # Group data by 'sensor_id', 'mgtw_id', and resample by the specified frequency, then aggregate metrics
        summary_df = missing_df.groupby(group_cols + [pd.Grouper(freq=freq)]).agg(
            missing_pckts=('missing_pckts', 'sum'),
        ).reset_index()
        
        return summary_df



    def calculate_SN2BG_summary(self, df, freq):
        '''
        Calculates the summary metrics for SN to BG data, including average RSSI, SNR, and packet error rate (PER).
        
        Parameters:
        - df: pandas DataFrame containing the raw data.
        
        Returns:
        - A pandas DataFrame with summarized daily metrics.
        '''

        missing_pckt = self.count_pckt_error_SN2BG(df,freq)

        # Convert 'timestamp' to datetime format if it's not already
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        
        # Set 'timestamp' as the index
        df.set_index('timestamp', inplace=True)

        df['pckt_nr'] = (df['frameport'] != 99).astype(int)

        # Define grouping columns
        group_cols = ['sensor_id', 'bgtw_id']

        # Group data by 'sensor_id', 'bgtw_id', and resample daily, then aggregate metrics
        summary_df = df.groupby(group_cols + [pd.Grouper(freq=freq)]).agg(
            avg_rssi=('bgtw_rssi', 'mean'),
            avg_snr=('bgtw_snr', 'mean'),
            pckt_nr=('pckt_nr', 'sum'),
        ).reset_index()

        # Round the 'avg_rssi' and 'avg_snr' values to 2 decimal places
        summary_df['avg_rssi'] = round(summary_df['avg_rssi'], 2)
        summary_df['avg_snr'] = round(summary_df['avg_snr'], 2)

        # Merge the missing packets DataFrame with the original DataFrame
        merged_df = pd.merge(summary_df, missing_pckt, on=['sensor_id', 'bgtw_id', 'timestamp'], how='left')

        # Calculate total_pckts as the sum of pckt_nr and missing_pckts
        merged_df['total_pckts'] = merged_df['pckt_nr'] + merged_df['missing_pckts']

        merged_df.drop(columns=['pckt_nr'], axis=1, inplace=True)
        
        # Calculate packet loss percentage
        merged_df['pckt_error_rate'] = round((merged_df['missing_pckts'] / merged_df['total_pckts'])*100,2)

        return merged_df  # Return the summarized DataFrame







    def calculate_distance(self, df):
        '''
        Calculates the geodesic distance between sensors and mesh gateways.
        
        Parameters:
        - df: pandas DataFrame containing the sensor and gateway locations.
        
        Returns:
        - A GeoDataFrame with the calculated distances.
        '''
        gdf = gpd.GeoDataFrame(df, geometry=gpd.points_from_xy(df['sensor_long'], df['sensor_lat']), crs="EPSG:4326")
        sensor_coord = list(zip(gdf['sensor_lat'], gdf['sensor_long']))
        mesh_coord = list(zip(gdf['mgtw_lat'], gdf['mgtw_long']))

        dist_list = [round(GD(sensor, mesh).m, 2) for sensor, mesh in zip(sensor_coord, mesh_coord)]
        gdf['SN2MG_distance_m'] = dist_list

        return gdf

    def calculate_SN2MG_summary(self, df, freq):
        missing_pckt = self.count_pckt_error_SN2MG(df, freq)
        '''
        Calculates the summary metrics for SN to MG data, including average RSSI, SNR, packet error rate (PER), 
        and the distance between sensors and mesh gateways.
        
        Parameters:
        - df: pandas DataFrame containing the raw data.
        
        Returns:
        - A pandas DataFrame with summarized daily metrics and distances.
        '''
        # Convert 'timestamp' to datetime format if it's not already
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        
        # Set 'timestamp' as the index
        df.set_index('timestamp', inplace=True)

        df['pckt_nr'] = (df['frameport'] != 99).astype(int)

        # Define the columns for grouping data
        group_cols = ['sensor_id', 'bgtw_id','mgtw_id', 'sensor_long', 'sensor_lat', 'mgtw_lat', 'mgtw_long']

        # Group data by sensor ID and mesh ID, resample daily, and calculate summary metrics
        summary_df = df.groupby(group_cols + [pd.Grouper(freq=freq)]).agg(
            avg_rssi=('bgtw_rssi', 'mean'),
            avg_snr=('bgtw_snr', 'mean'),
            pckt_nr=('pckt_nr', 'sum'),
        ).reset_index()

        # Round the 'avg_rssi' and 'avg_snr' values to two decimal places
        summary_df['avg_rssi'] = round(summary_df['avg_rssi'], 2)
        summary_df['avg_snr'] = round(summary_df['avg_snr'], 2)
        
        # Calculate distances between sensors and mesh gateways
        gdf = self.calculate_distance(summary_df)

        # Merge the missing packets DataFrame with the original DataFrame
        merged_df = pd.merge(gdf, missing_pckt, on=['sensor_id', 'mgtw_id', 'timestamp'], how='left')

        # Calculate total_pckts as the sum of pckt_nr and missing_pckts
        merged_df['total_pckts'] = merged_df['pckt_nr'] + merged_df['missing_pckts']


        merged_df.drop(["geometry","bgtw_id","pckt_nr", "sensor_long", "sensor_lat", "mgtw_lat", "mgtw_long"], axis=1, inplace=True)
        
        # Calculate packet loss percentage
        merged_df['pckt_error_rate'] = round((merged_df['missing_pckts'] / merged_df['total_pckts'])*100,2)

        merged_df = merged_df.sort_values(by=['timestamp','sensor_id', 'mgtw_id'])

        return merged_df  # Return summarized DataFrame
    

class DataVisualisationEngine:
    def __init__(self):
        pass

    def create_SN2BG_subplot(self, df):
        # Get unique values for the 'bgtw_id' column for the legend
        unique_bgtw_ids = df['bgtw_id'].unique()

        # Define the y-axis attributes for subplots
        y_attributes = ['avg_snr', 'avg_rssi', 'pckt_error_rate']  # Ensure these column names match your dataset
        y_labels = ['Average SNR [dB]', 'Average RSSI [dBm]', 'Average PER [%]']

        df['timestamp'] = pd.to_datetime(df['timestamp']).dt.date
        df = df.sort_values(by='timestamp')

        # If more than 2 unique 'bgtw_id', use FacetGrid (current setup)
        if len(unique_bgtw_ids) > 1:
        
            # Create a custom palette that matches the unique sensor_id values
            palette = sns.color_palette('Set1', n_colors=len(unique_bgtw_ids))
            bgtw_palette = dict(zip(unique_bgtw_ids, palette))

            # Set figure size to accommodate multiple subplots (adjust as needed)
            plt.figure(figsize=(12, 10))

            # Create subplots for each y-axis attribute (SNR, RSSI, PER)
            for i, y_attr in enumerate(y_attributes, 1):
                # Facet grid with timestamp as column
                g = sns.FacetGrid(df, col="timestamp", height=4, aspect=1.2, col_wrap=3, hue_order=unique_bgtw_ids)

                # Map the barplot with sensor_id, y_attr (SNR/RSSI/PER), and hue for bgtw_id
                g.map_dataframe(sns.barplot, x="sensor_id", y=y_attr, hue="bgtw_id", palette=bgtw_palette, dodge=True)

                # Rotate x-axis labels for all plots
                for ax in g.axes.flat:
                    for label in ax.get_xticklabels():
                        label.set_rotation(60)

                # Set axis labels
                g.set_axis_labels("Sensor ID", y_attr.replace("_", " ").upper())

                # Add title for the current plot
                g.fig.suptitle(f"{y_labels[i - 1]} by Sensor ID and Gateway ID Over Time", y=1.05, fontsize=14)

                # Add legend and place it outside the plot
                g.add_legend(title="Gateway ID", bbox_to_anchor=(1, 0.5), loc='center left')

                # Adjust layout to avoid overlap
                plt.tight_layout(rect=[0, 0, 0.85, 1])  # Reserve space on the right for the legend

                # Display subplot
                plt.show()

        else:
            unique_sensor_ids = df['sensor_id'].unique()

            # Create a custom palette that matches the unique sensor_id values
            palette = sns.color_palette('Set1', n_colors=len(unique_sensor_ids))
            sensor_palette = dict(zip(unique_sensor_ids, palette))

            # Apply the visual style after the else based on the example provided
            for y_attr, y_label in zip(y_attributes, y_labels):
                # Set up the figure
                plt.figure(figsize=(12, 6))

                ax = sns.barplot(data=df, x='timestamp', y=y_attr, hue='sensor_id', palette=sensor_palette)
                for i in ax.containers:
                    ax.bar_label(i,)

                # Set title and labels
                plt.title(f'{y_label} by Sensor and Gateway ID Over Time')
                plt.xlabel('Timestamp')
                plt.ylabel(y_label)

                # Rotate x-axis labels
                plt.xticks(rotation=45, ha="right")

                # Add legend and place it outside the plot
                plt.legend(title='Sensor ID', bbox_to_anchor=(1.05, 1), loc='upper left')
                
                # Adjust layout to avoid overlap
                plt.tight_layout()

                # Show the plot
                plt.show()

    def create_SN2MG_subplot(self, df):
        # Define the y-axis attributes for subplots
        y_attributes = ['avg_snr', 'avg_rssi', 'pckt_error_rate']  # Ensure these column names match your dataset
        y_labels = ['Average SNR', 'Average RSSI', 'Average PER']

        # Set figure size to accommodate multiple subplots (adjust as needed)
        plt.figure(figsize=(12, 15))

        df['timestamp'] = pd.to_datetime(df['timestamp']).dt.date
        # Sort the dataframe by timestamp
        df = df.sort_values(by='timestamp')

        # Get unique values for the 'mgtw_id' column for the legend
        unique_mgtw_ids = df['mgtw_id'].unique()

        # Create a custom palette that matches the unique mgtw_id values
        palette = sns.color_palette('Set1', n_colors=len(unique_mgtw_ids))
        mgtw_palette = dict(zip(unique_mgtw_ids, palette))

        # Create subplots for each y-axis attribute (SNR, RSSI, PER)
        for i, y_attr in enumerate(y_attributes, 1):
            # Facet grid with timestamp as column, using col_wrap to split the plots into rows
            g = sns.FacetGrid(df, col="timestamp", height=4, aspect=1.2, col_wrap=3, hue_order=unique_mgtw_ids)

            # Map the barplot with sensor_id, y_attr (SNR/RSSI/PER), and hue for mgtw_id
            g.map_dataframe(sns.barplot, x="sensor_id", y=y_attr, hue="mgtw_id", palette=mgtw_palette, dodge=True)

            # Rotate x-axis labels for all plots
            for ax in g.axes.flat:
                for label in ax.get_xticklabels():
                    label.set_rotation(60)

            # Set axis labels
            g.set_axis_labels("Sensor ID", y_attr.replace("_", " ").upper())  # Capitalize y attribute labels

            # Add title for the current plot
            g.fig.suptitle(f"{y_labels[i - 1]} by Sensor ID and Gateway ID Over Time", y=1.05, fontsize=14)

            # Add legend and place it outside the plot
            g.add_legend(title="Gateway ID", bbox_to_anchor=(1, 0.5), loc='center left')

            # Adjust layout to avoid overlap
            plt.tight_layout(rect=[0, 0, 1, 1])  # Reserve space on the right for the legend

            # Display subplot
            plt.show()


class DataPipelineEngine:
    def __init__(self):
        # Initialize the data extraction, cleaning, and summary engines
        self.data_extraction_engine = DataExtactionEngine()
        self.data_cleaning_engine = DataCleaningEngine()
        self.data_summarizing_engine = DataSummaryEngine()
        self.data_visualisation_engine = DataVisualisationEngine()
        
    def run_pipeline(self, username,password,sensor_list, gtw_list, date_range, gtw_type, freq, to_file=None):
        '''
        Runs the entire data pipeline, including extraction, cleaning, and summarizing.
        
        Parameters:
        - username: Snowflake username for authentication.
        - password: Snowflake password for authentication.
        - sensor_list: List of sensor IDs.
        - gtw_list: Optional; List of gateway IDs (either as strings or integers).
        - date_range: List of two dates specifying the start and end of the range.
        
        Returns:
        - Returns a tuple of (raw DataFrame, summarized DataFrame).
        '''
        if gtw_type == 0:

            if gtw_list is None:
                print("Error: No gateway list provided to query.")
            
            else:
                # Run the entire data pipeline: extraction, cleaning, and summary
                temp_df = self.data_extraction_engine.get_snowflake_SN2BG(username,password,sensor_list, gtw_list, date_range)  # Load Data
                        
                SN2BG = self.data_cleaning_engine.clean_SN2BG(temp_df)  # Clean Data

                SN2BG_summary = self.data_summarizing_engine.calculate_SN2BG_summary(SN2BG, freq)  # Calculate summary metrics

                self.data_visualisation_engine.create_SN2BG_subplot(SN2BG_summary)

                if to_file is None:
                    
                    return SN2BG, SN2BG_summary  # Return the final summarized DataFrame
                
                else:
                    SN2BG.to_csv(to_file+'SN2BG.csv')
                    SN2BG_summary.to_csv(to_file+'SN2BG_summary.csv')

                    return SN2BG, SN2BG_summary  # Return the final summarized DataFrame

        
        else:
            if gtw_type > 1:
                print("Error: Gateway type is out of range, please choose between 0 for BG, and 1 for MG")

            else:

                temp_df = self.data_extraction_engine.get_snowflake_SN2MG(username,password,sensor_list, gtw_list, date_range)

                SN2MG = self.data_cleaning_engine.clean_SN2Mesh(temp_df)

                SN2MG_summary = self.data_summarizing_engine.calculate_SN2MG_summary(SN2MG, freq)

                self.data_visualisation_engine.create_SN2MG_subplot(SN2MG_summary)

                if to_file is None:
                    
                    return SN2MG, SN2MG_summary  
                
                else:
                    SN2MG.to_csv(to_file+'SN2MG.csv')
                    SN2MG_summary.to_csv(to_file+'SN2MG_summary.csv')
                    
                    return SN2MG, SN2MG_summary 