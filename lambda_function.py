import logging
import os
import re
import time
from datetime import date, datetime
from typing import List, Dict, Tuple

import boto3
from requests_html import HTMLSession, Element

HOUR_RANGE = (9, 16)  # range of hours to check wind for
MIN_WIND_SPEED = 20  # minimum wind speed to check for, in km/h

DATA_URL = 'https://www.hko.gov.hk/en/sports/windtable.shtml'
LOCATIONS = {'TMTWSC': 'Tai Mei Tuk',
             'S': 'Stanley',
             'TM': 'Tap Mun'}
EXPECTED_DIMENSIONS = (
    'Time (Hour)', 'Temperature (oC)', 'Wind Speed (km/h)', 'Wind Direction', '3-Hourly Rainfall (mm)')

SNS_TOPIC_ARN = os.environ.get('SNS_TOPIC_ARN')


class ThreeHourForecast:

    def __init__(self, hour: int, temperature: float, wind_speed: int, wind_direction: str, rainfall: float):
        self.hour = hour
        self.temperature = temperature
        self.wind_speed = wind_speed
        self.wind_direction = wind_direction
        self.rain_fall = rainfall

    @classmethod
    def from_strings(cls, hour_str: str, temperature_str: str, wind_speed_str: str, wind_direction_str: str,
                     rainfall_str: str):
        return cls(int(hour_str), float(temperature_str), int(wind_speed_str), wind_direction_str, float(rainfall_str))

    @classmethod
    def parse_forecast_table(cls, table: Element) -> Tuple[date, List['ThreeHourForecast']]:
        """
        Parse the html hourly forecast table.
        :param table: The html table
        :return: (forecast_date, forecast_data)
        """
        try:
            table_caption = table.find('caption', first=True).text
        except AttributeError:
            raise Exception(f'Failed to find table caption for parsing forecast date')
        try:
            match = re.match('Forecast Date: (\d{4}-\d{1,2}-\d{1,2})', table_caption)
            forecast_date = datetime.strptime(match.group(1), '%Y-%m-%d').date()
        except (AttributeError, ValueError):
            raise Exception(f'Failed to parse forecast date. Expected :"Forecast Date: YYYY-M-D", got: {table_caption}')

        rows = [[field.text for field in row.find('td, th')] for row in table.find('tr')]
        transposed = list(zip(*rows))

        # Check that table is in expected format
        if transposed[0] != EXPECTED_DIMENSIONS:
            raise Exception(f'Unexpected row headers. Expected :{EXPECTED_DIMENSIONS}, got :{transposed[0]}')

        # Given expected row formats, transform into suitable data types.
        day_forecast = []
        for hour, temperature, wind_speed, wind_direction, rainfall in transposed[1:]:
            day_forecast.append(cls.from_strings(hour, temperature, wind_speed, wind_direction, rainfall))

        return forecast_date, day_forecast

    @property
    def is_strong(self) -> bool:
        """
        Check if strong wind is forecasted for the hourly_forecast, and time is within range
        :return: True if there is strong wind
        """
        start_hour, end_hour = HOUR_RANGE
        return self.wind_speed >= MIN_WIND_SPEED and (start_hour <= self.hour <= end_hour)

    def summary(self) -> str:
        return f'{self.hour}:00 - {self.wind_speed} km/h'

    def __repr__(self):
        return f'ThreeHourForecast({self.summary()})'


def group_by_date_location(date_locations: List[Tuple[date, str, List[ThreeHourForecast]]]) \
        -> Dict[date, Dict[str, List[ThreeHourForecast]]]:
    """
    helper function to group list of (forecast_date, location, forecasts) by dates and locations
    :param date_locations: list of (forecast_date, location, forecasts) to be grouped
    :return: Dict of Dict {forecast_date: {location: forecasts}}
    """
    results = {}
    for forecast_date, location, forecasts in date_locations:
        date_data = results.setdefault(forecast_date, {})
        date_data[location] = forecasts

    return results


def generate_alert(date_locations: List[Tuple[date, str, List[ThreeHourForecast]]]) -> Tuple[str, str]:
    """
    Generaten alert email from the forecasts
    :param date_locations: list of (forecast_date, location_code, forecasts) to alert
    :return: message_subject, message_body
    """

    date_location_groups = group_by_date_location(date_locations)
    date_summary = [f'{forecast_date.strftime("%d/%m")} @{", ".join([LOCATIONS[location] for location in locations])}'
                    for forecast_date, locations in date_location_groups.items()]
    message_subject = f'Strong wind forecasted: {"; ".join(date_summary)}'

    message_body = ''
    for forecast_date, locations in date_location_groups.items():
        message_body += f'{forecast_date.strftime("%Y-%m-%d (%a)")}\n'
        for location, forecasts in locations.items():
            message_body += f'{LOCATIONS[location]}\n'
            message_body += '\n'.join(['\t' + forecast.summary() for forecast in forecasts])
            message_body += '\n'

    return message_subject, message_body


def send_alert(message_subject: str, message_body: str) -> None:
    """
    Send the alert using AWS SNS
    :param message_subject: email subject
    :param message_body: email body in html format
    """
    client = boto3.client('sns')
    response = client.publish(
        TopicArn=SNS_TOPIC_ARN,
        Subject=message_subject,
        Message=message_body
    )
    logging.info(f'Alert sent, message id = {response.get("MessageId", "Not Found")}')


def lambda_handler(event, context):
    if logging.getLogger().hasHandlers():
        # The Lambda environment pre-configures a handler logging to stderr. If a handler is already configured,
        # `.basicConfig` does not execute. Thus we set the level directly.
        logging.getLogger().setLevel(logging.INFO)
    else:
        logging.basicConfig(level=logging.INFO)

    strong_wind_date_locations = []
    session = HTMLSession()

    s = time.time()
    for location in LOCATIONS:
        logging.info(f'Checking forecast for {LOCATIONS[location]}')
        r = session.get(DATA_URL, params={'stn': location})
        tables = r.html.find('table')

        for table in tables[1:]:
            forecast_date, forecasts = ThreeHourForecast.parse_forecast_table(table)
            strong_wind_hours = [forecast for forecast in forecasts if forecast.is_strong]
            if strong_wind_hours:  # at least 3 hours of strong wind
                strong_wind_date_locations.append((forecast_date, location, strong_wind_hours))
                logging.info(f'Strong wind detected: {forecast_date.strftime("%d/%m")}\n'
                             f'{", ".join([forecast.summary() for forecast in strong_wind_hours])}')
    logging.info(f'All forecasts checked in {time.time() - s:.2f}s')

    if strong_wind_date_locations:  # at least one date and location with strong wind
        logging.info(f'Strong wind found on {len(strong_wind_date_locations)} location-day(s)')
        message_subject, message_body = generate_alert(strong_wind_date_locations)
        send_alert(message_subject, message_body)
        logging.info(f'Alerted: {message_subject}')
    else:
        logging.info(f'No strong wind forecasted at all {len(LOCATIONS)} locations')


if __name__ == '__main__':
    lambda_handler(None, None)
