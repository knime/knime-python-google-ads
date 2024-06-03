import csv
import os
import knime.extension as knext


def read_csv():
    # Get the directory of the current script
    script_dir = os.path.dirname(os.path.abspath(__file__))

    # Path to the CSV file relative to the current script
    csv_file_path = os.path.join(script_dir, "../../data/language_codes.csv")

    language_name_to_criterion_id = {}

    with open(csv_file_path, mode="r") as file:
        reader = csv.DictReader(file)
        for row in reader:
            language_name = row["Language name"]
            criterion_id = row["Criterion ID"]

            language_name_to_criterion_id[language_name] = criterion_id

    return language_name_to_criterion_id


# Reading the CSV file and creating dictionaries and list
language_name_to_criterion_id = read_csv()


# Function to get the criterion ID based on the language name
def get_criterion_id(language_name):
    language_name = language_name.title()
    return language_name_to_criterion_id.get(language_name, "Language name not found")


# Function to convert comma-separated string to list
def convert_to_list(location_ids):
    return [id.strip() for id in location_ids.split(",")]


class LanguageSelection(knext.EnumParameterOptions):
    """
    Subclass of knext.EnumParameterOptions to handle language options from a CSV file.
    """

    ARABIC = ("Arabic", "The Arabic language (1019)")
    BENGALI = ("Bengali", "The Bengali language (1056)")
    BULGARIAN = ("Bulgarian", "The Bulgarian language (1020)")
    CATALAN = ("Catalan", "The Catalan language (1038)")
    CHINESE_SIMPLIFIED = (
        "Chinese (simplified)",
        "The Chinese (simplified) language (1017)",
    )
    CHINESE_TRADITIONAL = (
        "Chinese (traditional)",
        "The Chinese (traditional) language (1018)",
    )
    CROATIAN = ("Croatian", "The Croatian language (1039)")
    CZECH = ("Czech", "The Czech language (1021)")
    DANISH = ("Danish", "The Danish language (1009)")
    DUTCH = ("Dutch", "The Dutch language (1010)")
    ENGLISH = ("English", "The English language (1000)")
    ESTONIAN = ("Estonian", "The Estonian language (1043)")
    FILIPINO = ("Filipino", "The Filipino language (1042)")
    FINNISH = ("Finnish", "The Finnish language (1011)")
    FRENCH = ("French", "The French language (1002)")
    GERMAN = ("German", "The German language (1001)")
    GREEK = ("Greek", "The Greek language (1022)")
    GUJARATI = ("Gujarati", "The Gujarati language (1072)")
    HEBREW = ("Hebrew", "The Hebrew language (1027)")
    HINDI = ("Hindi", "The Hindi language (1023)")
    HUNGARIAN = ("Hungarian", "The Hungarian language (1024)")
    ICELANDIC = ("Icelandic", "The Icelandic language (1026)")
    INDONESIAN = ("Indonesian", "The Indonesian language (1025)")
    ITALIAN = ("Italian", "The Italian language (1004)")
    JAPANESE = ("Japanese", "The Japanese language (1005)")
    KANNADA = ("Kannada", "The Kannada language (1086)")
    KOREAN = ("Korean", "The Korean language (1012)")
    LATVIAN = ("Latvian", "The Latvian language (1028)")
    LITHUANIAN = ("Lithuanian", "The Lithuanian language (1029)")
    MALAY = ("Malay", "The Malay language (1102)")
    MALAYALAM = ("Malayalam", "The Malayalam language (1098)")
    MARATHI = ("Marathi", "The Marathi language (1101)")
    NORWEGIAN = ("Norwegian", "The Norwegian language (1013)")
    PERSIAN = ("Persian", "The Persian language (1064)")
    POLISH = ("Polish", "The Polish language (1030)")
    PORTUGUESE = ("Portuguese", "The Portuguese language (1014)")
    PUNJABI = ("Punjabi", "The Punjabi language (1110)")
    ROMANIAN = ("Romanian", "The Romanian language (1032)")
    RUSSIAN = ("Russian", "The Russian language (1031)")
    SERBIAN = ("Serbian", "The Serbian language (1035)")
    SLOVAK = ("Slovak", "The Slovak language (1033)")
    SLOVENIAN = ("Slovenian", "The Slovenian language (1034)")
    SPANISH = ("Spanish", "The Spanish language (1003)")
    SWEDISH = ("Swedish", "The Swedish language (1015)")
    TAMIL = ("Tamil", "The Tamil language (1130)")
    TELUGU = ("Telugu", "The Telugu language (1131)")
    THAI = ("Thai", "The Thai language (1044)")
    TURKISH = ("Turkish", "The Turkish language (1037)")
    UKRAINIAN = ("Ukrainian", "The Ukrainian language (1036)")
    URDU = ("Urdu", "The Urdu language (1041)")
    VIETNAMESE = ("Vietnamese", "The Vietnamese language (1040)")
