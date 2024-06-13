import knime.extension as knext

country_target_type_query = """SELECT 
  geo_target_constant.name, 
  geo_target_constant.id, 
  geo_target_constant.target_type, 
  geo_target_constant.status, 
  geo_target_constant.parent_geo_target, 
  geo_target_constant.resource_name, 
  geo_target_constant.country_code, 
  geo_target_constant.canonical_name 
FROM geo_target_constant 
WHERE 
  geo_target_constant.target_type = '$$target_type$$'
  AND geo_target_constant.country_code = '$$country_code$$' 
  AND geo_target_constant.status = 'ENABLED'"""

country_target_all_type_query = """SELECT 
  geo_target_constant.name, 
  geo_target_constant.id, 
  geo_target_constant.target_type, 
  geo_target_constant.status, 
  geo_target_constant.parent_geo_target, 
  geo_target_constant.resource_name, 
  geo_target_constant.country_code, 
  geo_target_constant.canonical_name 
FROM geo_target_constant 
WHERE 
  geo_target_constant.country_code = '$$country_code$$' 
  AND geo_target_constant.status = 'ENABLED' """


def get_country_type_query(country_code, target_type):
    if target_type != "ALL":
        # If target_type is provided, use country_target_type_query
        return country_target_type_query.replace(
            "$$country_code$$",
            country_code,
            # Convert target_type to title case because the query expects exact match and it is case sensitive
        ).replace("$$target_type$$", target_type.title())
    else:
        # If target_type is not provided, use country_all_target_query
        return country_target_all_type_query.replace("$$country_code$$", country_code)


class TargetTypeOptions(knext.EnumParameterOptions):
    ALL = ("All", "Output all locations available for the selected country")
    AIRPORT = ("Airport", "Output locations labeled as airports")
    CITY = ("City", "Output locations labeled as cities")
    CITY_REGION = ("City Region", "Output locations labeled as city regions")
    CONGRESSIONAL_DISTRICT = (
        "Congressional District",
        "Output locations labeled as congressional districts",
    )
    COUNTRY = ("Country", "Output locations labeled as countries")
    COUNTY = ("County", "Output locations labeled as counties")
    DEPARTMENT = ("Department", "Output locations labeled as departments")
    DISTRICT = ("District", "Output locations labeled as districts")
    GOVERNORATE = ("Governorate", "Output locations labeled as governorates")
    MUNICIPALITY = ("Municipality", "Output locations labeled as municipalities")
    NATIONAL_PARK = ("National Park", "Output locations labeled as national parks")
    NEIGHBORHOOD = ("Neighborhood", "Output locations labeled as neighborhoods")
    OKRUG = ("Okrug", "Output locations labeled as okrugs")
    POSTAL_CODE = ("Postal Code", "Output locations labeled as postal codes")
    PREFECTURE = ("Prefecture", "Output locations labeled as prefectures")
    PROVINCE = ("Province", "Output locations labeled as provinces")
    REGION = ("Region", "Output locations labeled as regions")
    STATE = ("State", "Output locations labeled as states")
    TERRITORY = ("Territory", "Output locations labeled as territories")
    TV_REGION = ("TV Region", "Output locations labeled as TV regions")
    UNION_TERRITORY = (
        "Union Territory",
        "Output locations labeled as union territories",
    )
    UNIVERSITY = ("University", "Output locations labeled as universities")


class CountryOptions(knext.EnumParameterOptions):
    AF = ("Afghanistan", "Output the locations available in Afghanistan")
    AL = ("Albania", "Output the locations available in Albania")
    DZ = ("Algeria", "Output the locations available in Algeria")
    AS = ("American Samoa", "Output the locations available in American Samoa")
    AD = ("Andorra", "Output the locations available in Andorra")
    AO = ("Angola", "Output the locations available in Angola")
    AQ = ("Antarctica", "Output the locations available in Antarctica")
    AG = (
        "Antigua and Barbuda",
        "Output the locations available in Antigua and Barbuda",
    )
    AR = ("Argentina", "Output the locations available in Argentina")
    AM = ("Armenia", "Output the locations available in Armenia")
    AU = ("Australia", "Output the locations available in Australia")
    AT = ("Austria", "Output the locations available in Austria")
    AZ = ("Azerbaijan", "Output the locations available in Azerbaijan")
    BH = ("Bahrain", "Output the locations available in Bahrain")
    BD = ("Bangladesh", "Output the locations available in Bangladesh")
    BB = ("Barbados", "Output the locations available in Barbados")
    BY = ("Belarus", "Output the locations available in Belarus")
    BE = ("Belgium", "Output the locations available in Belgium")
    BZ = ("Belize", "Output the locations available in Belize")
    BJ = ("Benin", "Output the locations available in Benin")
    BT = ("Bhutan", "Output the locations available in Bhutan")
    BO = ("Bolivia", "Output the locations available in Bolivia")
    BA = (
        "Bosnia and Herzegovina",
        "Output the locations available in Bosnia and Herzegovina",
    )
    BW = ("Botswana", "Output the locations available in Botswana")
    BR = ("Brazil", "Output the locations available in Brazil")
    BN = ("Brunei", "Output the locations available in Brunei")
    BG = ("Bulgaria", "Output the locations available in Bulgaria")
    BF = ("Burkina Faso", "Output the locations available in Burkina Faso")
    BI = ("Burundi", "Output the locations available in Burundi")
    CV = ("Cabo Verde", "Output the locations available in Cabo Verde")
    KH = ("Cambodia", "Output the locations available in Cambodia")
    CM = ("Cameroon", "Output the locations available in Cameroon")
    CA = ("Canada", "Output the locations available in Canada")
    BQ = (
        "Caribbean Netherlands",
        "Output the locations available in Caribbean Netherlands",
    )
    CF = (
        "Central African Republic",
        "Output the locations available in Central African Republic",
    )
    TD = ("Chad", "Output the locations available in Chad")
    CL = ("Chile", "Output the locations available in Chile")
    CN = ("China", "Output the locations available in China")
    CX = ("Christmas Island", "Output the locations available in Christmas Island")
    CC = (
        "Cocos (Keeling) Islands",
        "Output the locations available in Cocos (Keeling) Islands",
    )
    CO = ("Colombia", "Output the locations available in Colombia")
    KM = ("Comoros", "Output the locations available in Comoros")
    CK = ("Cook Islands", "Output the locations available in Cook Islands")
    CR = ("Costa Rica", "Output the locations available in Costa Rica")
    CI = ("Cote d'Ivoire", "Output the locations available in Cote d'Ivoire")
    HR = ("Croatia", "Output the locations available in Croatia")
    CW = ("Curacao", "Output the locations available in Curacao")
    CY = ("Cyprus", "Output the locations available in Cyprus")
    CZ = ("Czechia", "Output the locations available in Czechia")
    CD = (
        "Democratic Republic of the Congo",
        "Output the locations available in Democratic Republic of the Congo",
    )
    DK = ("Denmark", "Output the locations available in Denmark")
    DJ = ("Djibouti", "Output the locations available in Djibouti")
    DM = ("Dominica", "Output the locations available in Dominica")
    DO = ("Dominican Republic", "Output the locations available in Dominican Republic")
    EC = ("Ecuador", "Output the locations available in Ecuador")
    EG = ("Egypt", "Output the locations available in Egypt")
    SV = ("El Salvador", "Output the locations available in El Salvador")
    GQ = ("Equatorial Guinea", "Output the locations available in Equatorial Guinea")
    ER = ("Eritrea", "Output the locations available in Eritrea")
    EE = ("Estonia", "Output the locations available in Estonia")
    SZ = ("Eswatini", "Output the locations available in Eswatini")
    ET = ("Ethiopia", "Output the locations available in Ethiopia")
    FJ = ("Fiji", "Output the locations available in Fiji")
    FI = ("Finland", "Output the locations available in Finland")
    FR = ("France", "Output the locations available in France")
    PF = ("French Polynesia", "Output the locations available in French Polynesia")
    TF = (
        "French Southern and Antarctic Lands",
        "Output the locations available in French Southern and Antarctic Lands",
    )
    GA = ("Gabon", "Output the locations available in Gabon")
    GE = ("Georgia", "Output the locations available in Georgia")
    DE = ("Germany", "Output the locations available in Germany")
    GH = ("Ghana", "Output the locations available in Ghana")
    GR = ("Greece", "Output the locations available in Greece")
    GD = ("Grenada", "Output the locations available in Grenada")
    GU = ("Guam", "Output the locations available in Guam")
    GT = ("Guatemala", "Output the locations available in Guatemala")
    GG = ("Guernsey", "Output the locations available in Guernsey")
    GN = ("Guinea", "Output the locations available in Guinea")
    GW = ("Guinea-Bissau", "Output the locations available in Guinea-Bissau")
    GY = ("Guyana", "Output the locations available in Guyana")
    HT = ("Haiti", "Output the locations available in Haiti")
    HM = (
        "Heard Island and McDonald Islands",
        "Output the locations available in Heard Island and McDonald Islands",
    )
    HN = ("Honduras", "Output the locations available in Honduras")
    HU = ("Hungary", "Output the locations available in Hungary")
    IS = ("Iceland", "Output the locations available in Iceland")
    IN = ("India", "Output the locations available in India")
    ID = ("Indonesia", "Output the locations available in Indonesia")
    IQ = ("Iraq", "Output the locations available in Iraq")
    IE = ("Ireland", "Output the locations available in Ireland")
    IM = ("Isle of Man", "Output the locations available in Isle of Man")
    IL = ("Israel", "Output the locations available in Israel")
    IT = ("Italy", "Output the locations available in Italy")
    JM = ("Jamaica", "Output the locations available in Jamaica")
    JP = ("Japan", "Output the locations available in Japan")
    JE = ("Jersey", "Output the locations available in Jersey")
    JO = ("Jordan", "Output the locations available in Jordan")
    KZ = ("Kazakhstan", "Output the locations available in Kazakhstan")
    KE = ("Kenya", "Output the locations available in Kenya")
    KI = ("Kiribati", "Output the locations available in Kiribati")
    KW = ("Kuwait", "Output the locations available in Kuwait")
    KG = ("Kyrgyzstan", "Output the locations available in Kyrgyzstan")
    LA = ("Laos", "Output the locations available in Laos")
    LV = ("Latvia", "Output the locations available in Latvia")
    LB = ("Lebanon", "Output the locations available in Lebanon")
    LS = ("Lesotho", "Output the locations available in Lesotho")
    LR = ("Liberia", "Output the locations available in Liberia")
    LY = ("Libya", "Output the locations available in Libya")
    LI = ("Liechtenstein", "Output the locations available in Liechtenstein")
    LT = ("Lithuania", "Output the locations available in Lithuania")
    LU = ("Luxembourg", "Output the locations available in Luxembourg")
    MG = ("Madagascar", "Output the locations available in Madagascar")
    MW = ("Malawi", "Output the locations available in Malawi")
    MY = ("Malaysia", "Output the locations available in Malaysia")
    MV = ("Maldives", "Output the locations available in Maldives")
    ML = ("Mali", "Output the locations available in Mali")
    MT = ("Malta", "Output the locations available in Malta")
    MH = ("Marshall Islands", "Output the locations available in Marshall Islands")
    MR = ("Mauritania", "Output the locations available in Mauritania")
    MU = ("Mauritius", "Output the locations available in Mauritius")
    MX = ("Mexico", "Output the locations available in Mexico")
    FM = ("Micronesia", "Output the locations available in Micronesia")
    MD = ("Moldova", "Output the locations available in Moldova")
    MC = ("Monaco", "Output the locations available in Monaco")
    MN = ("Mongolia", "Output the locations available in Mongolia")
    ME = ("Montenegro", "Output the locations available in Montenegro")
    MA = ("Morocco", "Output the locations available in Morocco")
    MZ = ("Mozambique", "Output the locations available in Mozambique")
    MM = ("Myanmar (Burma)", "Output the locations available in Myanmar (Burma)")
    NA = ("Namibia", "Output the locations available in Namibia")
    NR = ("Nauru", "Output the locations available in Nauru")
    NP = ("Nepal", "Output the locations available in Nepal")
    NL = ("Netherlands", "Output the locations available in Netherlands")
    NC = ("New Caledonia", "Output the locations available in New Caledonia")
    NZ = ("New Zealand", "Output the locations available in New Zealand")
    NI = ("Nicaragua", "Output the locations available in Nicaragua")
    NE = ("Niger", "Output the locations available in Niger")
    NG = ("Nigeria", "Output the locations available in Nigeria")
    NU = ("Niue", "Output the locations available in Niue")
    NF = ("Norfolk Island", "Output the locations available in Norfolk Island")
    MK = ("North Macedonia", "Output the locations available in North Macedonia")
    MP = (
        "Northern Mariana Islands",
        "Output the locations available in Northern Mariana Islands",
    )
    NO = ("Norway", "Output the locations available in Norway")
    OM = ("Oman", "Output the locations available in Oman")
    PK = ("Pakistan", "Output the locations available in Pakistan")
    PW = ("Palau", "Output the locations available in Palau")
    PA = ("Panama", "Output the locations available in Panama")
    PG = ("Papua New Guinea", "Output the locations available in Papua New Guinea")
    PY = ("Paraguay", "Output the locations available in Paraguay")
    PE = ("Peru", "Output the locations available in Peru")
    PH = ("Philippines", "Output the locations available in Philippines")
    PN = ("Pitcairn Islands", "Output the locations available in Pitcairn Islands")
    PL = ("Poland", "Output the locations available in Poland")
    PT = ("Portugal", "Output the locations available in Portugal")
    QA = ("Qatar", "Output the locations available in Qatar")
    CG = (
        "Republic of the Congo",
        "Output the locations available in Republic of the Congo",
    )
    RO = ("Romania", "Output the locations available in Romania")
    RU = ("Russia", "Output the locations available in Russia")
    RW = ("Rwanda", "Output the locations available in Rwanda")
    BL = ("Saint Barthelemy", "Output the locations available in Saint Barthelemy")
    SH = (
        "Saint Helena, Ascension and Tristan da Cunha",
        "Output the locations available in Saint Helena, Ascension and Tristan da Cunha",
    )
    KN = (
        "Saint Kitts and Nevis",
        "Output the locations available in Saint Kitts and Nevis",
    )
    LC = ("Saint Lucia", "Output the locations available in Saint Lucia")
    MF = ("Saint Martin", "Output the locations available in Saint Martin")
    PM = (
        "Saint Pierre and Miquelon",
        "Output the locations available in Saint Pierre and Miquelon",
    )
    VC = (
        "Saint Vincent and the Grenadines",
        "Output the locations available in Saint Vincent and the Grenadines",
    )
    WS = ("Samoa", "Output the locations available in Samoa")
    SM = ("San Marino", "Output the locations available in San Marino")
    ST = (
        "Sao Tome and Principe",
        "Output the locations available in Sao Tome and Principe",
    )
    SA = ("Saudi Arabia", "Output the locations available in Saudi Arabia")
    SN = ("Senegal", "Output the locations available in Senegal")
    RS = ("Serbia", "Output the locations available in Serbia")
    SC = ("Seychelles", "Output the locations available in Seychelles")
    SL = ("Sierra Leone", "Output the locations available in Sierra Leone")
    SG = ("Singapore", "Output the locations available in Singapore")
    SX = ("Sint Maarten", "Output the locations available in Sint Maarten")
    SK = ("Slovakia", "Output the locations available in Slovakia")
    SI = ("Slovenia", "Output the locations available in Slovenia")
    SB = ("Solomon Islands", "Output the locations available in Solomon Islands")
    SO = ("Somalia", "Output the locations available in Somalia")
    ZA = ("South Africa", "Output the locations available in South Africa")
    GS = (
        "South Georgia and the South Sandwich Islands",
        "Output the locations available in South Georgia and the South Sandwich Islands",
    )
    KR = ("South Korea", "Output the locations available in South Korea")
    SS = ("South Sudan", "Output the locations available in South Sudan")
    ES = ("Spain", "Output the locations available in Spain")
    LK = ("Sri Lanka", "Output the locations available in Sri Lanka")
    SD = ("Sudan", "Output the locations available in Sudan")
    SR = ("Suriname", "Output the locations available in Suriname")
    SE = ("Sweden", "Output the locations available in Sweden")
    CH = ("Switzerland", "Output the locations available in Switzerland")
    TJ = ("Tajikistan", "Output the locations available in Tajikistan")
    TZ = ("Tanzania", "Output the locations available in Tanzania")
    TH = ("Thailand", "Output the locations available in Thailand")
    BS = ("The Bahamas", "Output the locations available in The Bahamas")
    GM = ("The Gambia", "Output the locations available in The Gambia")
    TL = ("Timor-Leste", "Output the locations available in Timor-Leste")
    TG = ("Togo", "Output the locations available in Togo")
    TK = ("Tokelau", "Output the locations available in Tokelau")
    TO = ("Tonga", "Output the locations available in Tonga")
    TT = (
        "Trinidad and Tobago",
        "Output the locations available in Trinidad and Tobago",
    )
    TN = ("Tunisia", "Output the locations available in Tunisia")
    TR = ("Turkiye", "Output the locations available in Turkiye")
    TM = ("Turkmenistan", "Output the locations available in Turkmenistan")
    TV = ("Tuvalu", "Output the locations available in Tuvalu")
    UG = ("Uganda", "Output the locations available in Uganda")
    UA = ("Ukraine", "Output the locations available in Ukraine")
    AE = (
        "United Arab Emirates",
        "Output the locations available in United Arab Emirates",
    )
    GB = ("United Kingdom", "Output the locations available in United Kingdom")
    US = ("United States", "Output the locations available in United States")
    UM = (
        "United States Minor Outlying Islands",
        "Output the locations available in United States Minor Outlying Islands",
    )
    UY = ("Uruguay", "Output the locations available in Uruguay")
    UZ = ("Uzbekistan", "Output the locations available in Uzbekistan")
    VU = ("Vanuatu", "Output the locations available in Vanuatu")
    VA = ("Vatican City", "Output the locations available in Vatican City")
    VE = ("Venezuela", "Output the locations available in Venezuela")
    VN = ("Vietnam", "Output the locations available in Vietnam")
    WF = ("Wallis and Futuna", "Output the locations available in Wallis and Futuna")
    YE = ("Yemen", "Output the locations available in Yemen")
    ZM = ("Zambia", "Output the locations available in Zambia")
    ZW = ("Zimbabwe", "Output the locations available in Zimbabwe")
