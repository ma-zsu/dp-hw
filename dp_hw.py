import csv
from collections import defaultdict
from datetime import datetime, timedelta
import operator

# Function to convert event time to datetime object
def parse_event_time(event_time):
    return datetime.strptime(event_time, "%Y-%m-%dT%H:%M:%S.%fZ")

# Function to calculate session length
def calculate_session_length(in_time, out_time):
    return (out_time - in_time).total_seconds() / 3600

# Function to parse CSV data and perform analytics
def process_csv(input_file):
    # Initialize dictionaries to store data
    time_spent = defaultdict(float)  # Total time spent by each user
    days_spent = defaultdict(set)     # Days each user was present
    session_lengths = defaultdict(float)  # Session length for each user

    # Read CSV file and process data
    with open(input_file, 'r', newline='') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            # Filter for February and gate_in/gate_out events
            event_time = parse_event_time(row['event_time'])
            if event_time.month != 2 or row['event_type'].lower() not in ['gate_in', 'gate_out']:
                continue

            user_id = row['user_id']
            if row['event_type'].lower() == 'gate_in':
                in_time = event_time
            elif row['event_type'].lower() == 'gate_out':
                out_time = event_time
                session_lengths[user_id] += calculate_session_length(in_time, out_time)
                in_time = out_time  # Update in_time for next session

            days_spent[user_id].add(event_time.date())
        
        # Calculate total time spent by each user
        for user_id, dates in days_spent.items():
            time_spent[user_id] = sum(session_lengths[user_id] for _ in dates)

    # Sort users based on average time per day
    sorted_users = sorted(time_spent.items(), key=lambda x: x[1] / len(days_spent[x[0]]), reverse=True)

    # Calculate rank
    ranked_users = []
    rank = 1
    for user_id, total_time in sorted_users:
        days = len(days_spent[user_id])
        average_per_day = total_time / days
        ranked_users.append((user_id, total_time, days, average_per_day, rank))
        rank += 1

    # Sort session lengths
    sorted_sessions = sorted(session_lengths.items(), key=lambda x: x[1], reverse=True)

    return ranked_users, sorted_sessions

# Function to write data to CSV
def write_to_csv(data, output_file, header):
    with open(output_file, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(header)
        for row in data:
            writer.writerow(row)

if __name__ == "__main__":
    input_file = "input/dp_hw.csv"
    
    # Process CSV data
    ranked_users, sorted_sessions = process_csv(input_file)

    # Write data to CSV files
    write_to_csv(ranked_users, "output/first.csv", ("user_id", "time", "days", "average_per_day", "rank"))
    write_to_csv([sorted_sessions[0]], "output/second.csv", ("user_id", "session_length"))
  