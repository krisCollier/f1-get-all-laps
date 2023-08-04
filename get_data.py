import fastf1
import pandas as pd
import time

years = [2022, 2023]
RACES = 23
SESSIONS = 5


def generate_event_code(weekend, country, year):
    return str(weekend).zfill(2) + country.replace(' ', '')[:6] + str(year)[2:]

def get_all_laps():
    all_data = []
    errors = []
    tick = time.time()
    for year in years:
        for weekend in range(1, RACES + 1):
            for session in range(1, SESSIONS + 1):
                try:
                    # Get data from FastF1
                    session_data = fastf1.get_session(year, weekend, session)
                    session_data.load(telemetry=True, weather=True)

                    # Get the laps and weather data
                    laps = session_data.laps
                    weather_data = laps.get_weather_data()

                    # Merge the laps and weather data
                    laps = laps.reset_index(drop=True)
                    weather_data = weather_data.reset_index(drop=True)
                    laps_with_weather = pd.concat([laps, weather_data.loc[:, ~(weather_data.columns == 'Time')]],
                                                  axis=1)

                    # Fix some data types and add some columns to make data actually be useful
                    event_code = generate_event_code(session_data.event.RoundNumber, session_data.event.Country, year)
                    laps_with_weather['LapTime'] = laps_with_weather['LapTime'].dt.total_seconds()
                    laps_with_weather['Sector1Time'] = laps_with_weather['Sector1Time'].dt.total_seconds()
                    laps_with_weather['Sector2Time'] = laps_with_weather['Sector2Time'].dt.total_seconds()
                    laps_with_weather['Sector3Time'] = laps_with_weather['Sector3Time'].dt.total_seconds()

                    laps_with_weather.insert(1, 'EventCode', event_code)
                    laps_with_weather.insert(2, 'Country', session_data.event.Country)
                    laps_with_weather.insert(3, 'SessionName', session_data.name)

                    print(event_code, session_data.name)
                    all_data.append(laps_with_weather)
                except:
                    errors.append(f"{weekend}:{session}:{year}")
                    print("No data for", year, weekend, session)
                    continue

    all_laps = pd.concat(all_data, ignore_index=True)
    all_laps.to_csv("data.csv", index=True)
    tock = time.time()
    print(errors)
    print(f"Time taken: {tock - tick} seconds for {len(all_laps)} laps (at {(tock - tick) / len(all_laps)} secs per lap)")

get_all_laps()
