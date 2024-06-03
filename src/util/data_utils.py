import csv
import os


def read_csv():
    # Get the directory of the current script
    script_dir = os.path.dirname(os.path.abspath(__file__))

    # Path to the CSV file relative to the current script
    csv_file_path = os.path.join(script_dir, "../../data/language_codes.csv")

    language_code_to_name = {}
    language_name_to_criterion_id = {}
    all_languages = []

    with open(csv_file_path, mode="r") as file:
        reader = csv.DictReader(file)
        for row in reader:
            language_name = row["Language name"]
            language_code = row["Language code"]
            criterion_id = row["Criterion ID"]

            language_code_to_name[language_code] = language_name
            language_name_to_criterion_id[language_name] = criterion_id
            all_languages.append(language_name)

    return language_code_to_name, language_name_to_criterion_id, all_languages


# Reading the CSV file and creating dictionaries and list
language_code_to_name, language_name_to_criterion_id, all_languages = read_csv()


# Function to get the criterion ID based on the language name
def get_criterion_id(language_name):
    return language_name_to_criterion_id.get(language_name, "Language name not found")
