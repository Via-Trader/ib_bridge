import re

class AlertProcessor:
    def __init__(self, time_period_dict, time_period):
        """
        Initialize the AlertProcessor with a dictionary of time period mappings and a base time period.
        :param time_period_dict: A dictionary mapping keywords (e.g., "three month") to the number of base units.
        :param time_period: The base time period multiplier (e.g., 15, 30, etc.).
        """
        self.time_period_dict = time_period_dict
        self.time_period = time_period

    def calculate_minutes(self, time_period):
        """
        Convert the time period to minutes using the time period dictionary and base multiplier.
        :param time_period: The time period string (e.g., "three month").
        :return: The equivalent number of minutes.
        """
        base_units = self.time_period_dict.get(time_period.lower(), None)
        if base_units is not None:
            return base_units * self.time_period
        return None

    def format_duration(self, total_minutes):
        """
        Format the total minutes into natural language, choosing the most human-readable phrasing.
        :param total_minutes: The total minutes to format.
        :return: A string representation in days, hours, or minutes.
        """
        if total_minutes >= 1440:  # 1440 minutes = 1 day
            days = total_minutes // 1440
            return f"around {days} days" if days > 1 else "a day"
        elif total_minutes >= 60:  # Format as hours
            hours = total_minutes // 60
            return f"around {hours} hours" if hours > 1 else "an hour"
        else:
            return f"{total_minutes} minutes"

    def replace_duration(self, alert_text):
        """
        Replace durations in the alert text using the time period dictionary and base multiplier.
        :param alert_text: The alert text to process.
        :return: The processed alert text with replacements.
        """
        def replace_match(match):
            duration = match.group(0)
            base_units = self.time_period_dict.get(duration.lower(), None)
            if base_units is not None:
                total_minutes = base_units * self.time_period
                return self.format_duration(total_minutes)
            return duration  # Default to original if no match

        # Build a regex pattern from the dictionary keys
        pattern = r"|".join(re.escape(key) for key in self.time_period_dict.keys())
        return re.sub(pattern, replace_match, alert_text)

    def process_alerts(self, alert_data):
        """
        Process a list of alert data by replacing durations in the alert descriptions.
        :param alert_data: List of alert entries, where each entry is a dictionary with a 'description' key.
        :return: List of processed alert entries.
        """
        for alert in alert_data:
            alert['description'] = self.replace_duration(alert['description'])
        return alert_data

# Example usage
if __name__ == "__main__":
    # Define a dictionary for replacements (time period to base units)
    time_period_dict = {
        "twelve month": 365,      # 12 months (365 days)
        "one month": 30,         # 1 month (30 days)
        "one week": 7,           # 1 week (7 days)
        "three month": 3 * 31,   # 3 months base (31 days each)
        "six month": 6 * 31,     # 6 months base (31 days each)
        "15 minutes": 1,         # 15 minutes as base unit
        "30 minutes": 2,         # 30 minutes (2 * 15 minutes)
        "60 minutes": 4,         # 1 hour (4 * 15 minutes)
        "75 minutes": 5,         # 1 hour 15 minutes (5 * 15 minutes)
        "155 minutes": 10        # 2 hours 35 minutes (10 * 15 minutes)
    }

    # Base time period multiplier (e.g., 10 for 10-minute periods)
    base_time_period = 10

    # Sample alert data
    alert_data = [
        {"description": "DJTrading near the twelve month low"},
        {"description": "DJTrading near the one month low"},
        {"description": "Biggest rise in 155 minutes"},
        {"description": "Close down to break one week winning streak"}
    ]

    processor = AlertProcessor(time_period_dict, base_time_period)
    processed_alerts = processor.process_alerts(alert_data)

    for alert in processed_alerts:
        print(alert['description'])
