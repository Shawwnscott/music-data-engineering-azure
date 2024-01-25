import numpy as np
from datetime import datetime, timedelta
import json
import pandas as pd

def generate_weekly_data(song_list):
    # Get the current year and week
    current_year, _, _ = datetime.now().isocalendar()

    for week_number in range(1, 53):
        weekly_data = {"songs": []}

        for song in song_list:
            # Generate random mean and standard deviation for downloads and streams
            downloads_mean = np.random.uniform(30000, 70000)
            downloads_std = np.random.uniform(10000, 30000)

            streams_mean = np.random.uniform(400000, 800000)
            streams_std = np.random.uniform(150000, 300000)

            # Generate random numbers for downloads and streams using a normal distribution
            total_weekly_downloads = int(np.random.normal(downloads_mean, downloads_std))
            total_weekly_streams = int(np.random.normal(streams_mean, streams_std))

            # Ensure the generated numbers are non-negative
            total_weekly_downloads = max(total_weekly_downloads, 0)
            total_weekly_streams = max(total_weekly_streams, 0)

            # Get the start and end dates of the week
            start_date, end_date = get_week_dates(current_year, week_number)

            # Append the data to the weekly_data list
            song_data = {
                "song_id": song,
                "weekly_data": {
                    "week": week_number,
                    "week_start_date": start_date.strftime('%Y-%m-%d'),
                    "week_end_date": end_date.strftime('%Y-%m-%d'),
                    "total_weekly_downloads": total_weekly_downloads,
                    "total_weekly_streams": total_weekly_streams
                }
            }

            weekly_data["songs"].append(song_data)

        # Save data for each week separately
        write_to_file(weekly_data, f'weekly_data_week_{week_number}.json')

# Rest of your code remains unchanged
def get_week_dates(year, week_number):
    start_date = datetime(year, 1, 1)
    start_of_week = start_date + timedelta(days=(week_number - 1) * 7 - start_date.weekday())
    end_of_week = start_of_week + timedelta(days=6)
    return start_of_week, end_of_week

def write_to_file(data, file_name):
    with open(file_name, 'w') as file:
        json.dump(data, file, indent=2)
        


def main():
    # Simulate running for 52 weeks
    song_list_df = pd.read_csv('ds_raw_track_ids.csv')
    song_list = song_list_df['SongID'].tolist()
    generate_weekly_data(song_list)
    print("Done")
    
if __name__ == "__main__":
    main()