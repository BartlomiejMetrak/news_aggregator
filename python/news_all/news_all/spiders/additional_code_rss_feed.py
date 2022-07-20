from urllib.parse import urlparse
from dateutil import parser, tz
from datetime import datetime
from bs4 import BeautifulSoup
from pytz import timezone


date = datetime.now()

def try_parse_date(entry, name):
    POSSIBLE_DATE_FORMATS = ['%a, %d %b %Y %H:%M:%S', '%Y-%m-%d %H:%M:%S', '%a, %d %b %Y %H:%M:%S %z',
                             '%Y-%m-%d %H:%M:%S%z']  # all the formats the date might be in
    parsed_date = date  # jeśli nie ma takiego formatu daty to wtedy wstawiany Nan
    try:
        date_object = entry[name]
        if date_object is None:
            date_object = date  #jesli jest none to też przyjmujemy obecną datę
        date_object = parser.parse(date_object, tzinfos=whois_timezone_info)
    except Exception as e:
        print(f"Błąd przy parsowaniu daty dla rss feed {e}")
        date_object = date  #jeśli nie ma daty to przyjmujemy obecną

    try:  # jeśli nie ma daty na liście
        raw_string_date = str(date_object)
        raw_string_date = convert_CET_CEST(raw_string_date)
    except Exception as e:
        print(f"Błąd przy parsowaniu daty dla rss feed {e}")
        raw_string_date = date  #w ostateczności próbujemy tak

    for date_format in POSSIBLE_DATE_FORMATS:
        try:
            parsed_date_def = datetime.strptime(raw_string_date, date_format)  # try to get the date
            zone = parsed_date_def.tzinfo
            parsed_date = date_timezone(zone, parsed_date_def)

            break  # if correct format, don't test any other formats
        except (ValueError, TypeError):
            pass  # if incorrect format, keep trying other formats
    return parsed_date


def date_timezone(zone, parsed_date_def):
    if zone is not None:
        to_zone = tz.tzlocal()
        utc = parsed_date_def.replace(tzinfo=zone)  # Tell the datetime object that it's in UTC time zone since datetime objects are 'naive' by default
        parsed_date = utc.astimezone(to_zone)  # Convert time zone

        return parsed_date.replace(tzinfo=None)
    else:
        return parsed_date_def


def convert_CET_CEST(raw_string_date):  # funkcja do konwesrtowania stref czasowych CEST i CET
    split = (raw_string_date.split(' '))
    lst = ['CEST', 'CET', 'GMT']
    if any(i in split for i in lst):  # jeśli w dacie jest taka strefa to wtedy konwesrtujemy ją na UTC
        tzmapping = {'CET': tz.gettz('Europe/Warsaw'),
                     'CEST': tz.gettz('Europe/Warsaw')}  # wyjątki dla stref czasowych - bo przy datach publikacji artykułów czasami wyskakiwał błąd
        date = parser.parse(raw_string_date, tzinfos=tzmapping)  # .astimezone(timezone.utc)

        dt = date.replace(tzinfo=timezone('UTC'))
        dt = dt.astimezone(timezone('Europe/Warsaw'))
    else:  # jesli nie ma to bez zmian zwracamy string
        dt = raw_string_date
    return str(dt)


''' ======================== '''

def try_parse_summary(entry, name, source):  # pobieranie summary oddzielny try i oddzielny dla strefy inwestorów
    try:
        summary = entry[name]
        soup = BeautifulSoup(summary, 'html.parser')

        if source == 10:  #wyjątek dla strefy inwestorów bo złączały się dwa fragmenty tekstów
            summ_field = soup.find(class_='field field-name-body field-type-text-with-summary field-label-hidden')
            sum_text = summ_field.find(class_='field-item even')
            summary = sum_text.text
        elif source == 13:  #wyjątek dla fxmag
            text_string = soup.text  #bierzemy pierwsze x znaków dla fxmag
            summary = ' '.join(text_string.split()[:50])  #bierzemy pierwsze 70 słów do summary dla fxmag
        else:  # reszta jest bez zmian
            summary = soup.text
        return summary
    except Exception as e:
        print(f"Błąd przy pobieraniu summary: {e}")
        var = None  # jak niem a summary to puste pole
        return var


''' ======================== '''

def try_parse(entry, name):  # w przypadku gdyby jakaś informacja nie była dostepna
    try:
        var = entry[name]
    except:
        var = None
    return var


''' ======================== '''

def try_parse_link(entry, name):  # w przypadku gdyby jakaś informacja nie była dostepna
    try:
        link = entry[name]
        components = urlparse(link)
        var = components.scheme + '://' + components.netloc + components.path
    except:
        var = None
    return var



# tabela ze strefami czasowymi do zamiany - pewnie można połączyć z tzmapping wyżej ale w wolnej chwili później

whois_timezone_info = {
    "A": 1 * 3600,
    "ACDT": 10.5 * 3600,
    "ACST": 9.5 * 3600,
    "ACT": -5 * 3600,
    "ACWST": 8.75 * 3600,
    "ADT": 4 * 3600,
    "AEDT": 11 * 3600,
    "AEST": 10 * 3600,
    "AET": 10 * 3600,
    "AFT": 4.5 * 3600,
    "AKDT": -8 * 3600,
    "AKST": -9 * 3600,
    "ALMT": 6 * 3600,
    "AMST": -3 * 3600,
    "AMT": -4 * 3600,
    "ANAST": 12 * 3600,
    "ANAT": 12 * 3600,
    "AQTT": 5 * 3600,
    "ART": -3 * 3600,
    "AST": 3 * 3600,
    "AT": -4 * 3600,
    "AWDT": 9 * 3600,
    "AWST": 8 * 3600,
    "AZOST": 0 * 3600,
    "AZOT": -1 * 3600,
    "AZST": 5 * 3600,
    "AZT": 4 * 3600,
    "AoE": -12 * 3600,
    "B": 2 * 3600,
    "BNT": 8 * 3600,
    "BOT": -4 * 3600,
    "BRST": -2 * 3600,
    "BRT": -3 * 3600,
    "BST": 6 * 3600,
    "BTT": 6 * 3600,
    "C": 3 * 3600,
    "CAST": 8 * 3600,
    "CAT": 2 * 3600,
    "CCT": 6.5 * 3600,
    "CDT": -5 * 3600,
    "CEST": 2 * 3600,
    "CET": 1 * 3600,
    "CHADT": 13.75 * 3600,
    "CHAST": 12.75 * 3600,
    "CHOST": 9 * 3600,
    "CHOT": 8 * 3600,
    "CHUT": 10 * 3600,
    "CIDST": -4 * 3600,
    "CIST": -5 * 3600,
    "CKT": -10 * 3600,
    "CLST": -3 * 3600,
    "CLT": -4 * 3600,
    "COT": -5 * 3600,
    "CST": -6 * 3600,
    "CT": -6 * 3600,
    "CVT": -1 * 3600,
    "CXT": 7 * 3600,
    "ChST": 10 * 3600,
    "D": 4 * 3600,
    "DAVT": 7 * 3600,
    "DDUT": 10 * 3600,
    "E": 5 * 3600,
    "EASST": -5 * 3600,
    "EAST": -6 * 3600,
    "EAT": 3 * 3600,
    "ECT": -5 * 3600,
    "EDT": -4 * 3600,
    "EEST": 3 * 3600,
    "EET": 2 * 3600,
    "EGST": 0 * 3600,
    "EGT": -1 * 3600,
    "EST": -5 * 3600,
    "ET": -5 * 3600,
    "F": 6 * 3600,
    "FET": 3 * 3600,
    "FJST": 13 * 3600,
    "FJT": 12 * 3600,
    "FKST": -3 * 3600,
    "FKT": -4 * 3600,
    "FNT": -2 * 3600,
    "G": 7 * 3600,
    "GALT": -6 * 3600,
    "GAMT": -9 * 3600,
    "GET": 4 * 3600,
    "GFT": -3 * 3600,
    "GILT": 12 * 3600,
    "GMT": 0 * 3600,
    "GST": 4 * 3600,
    "GYT": -4 * 3600,
    "H": 8 * 3600,
    "HDT": -9 * 3600,
    "HKT": 8 * 3600,
    "HOVST": 8 * 3600,
    "HOVT": 7 * 3600,
    "HST": -10 * 3600,
    "I": 9 * 3600,
    "ICT": 7 * 3600,
    "IDT": 3 * 3600,
    "IOT": 6 * 3600,
    "IRDT": 4.5 * 3600,
    "IRKST": 9 * 3600,
    "IRKT": 8 * 3600,
    "IRST": 3.5 * 3600,
    "IST": 5.5 * 3600,
    "JST": 9 * 3600,
    "K": 10 * 3600,
    "KGT": 6 * 3600,
    "KOST": 11 * 3600,
    "KRAST": 8 * 3600,
    "KRAT": 7 * 3600,
    "KST": 9 * 3600,
    "KUYT": 4 * 3600,
    "L": 11 * 3600,
    "LHDT": 11 * 3600,
    "LHST": 10.5 * 3600,
    "LINT": 14 * 3600,
    "M": 12 * 3600,
    "MAGST": 12 * 3600,
    "MAGT": 11 * 3600,
    "MART": 9.5 * 3600,
    "MAWT": 5 * 3600,
    "MDT": -6 * 3600,
    "MHT": 12 * 3600,
    "MMT": 6.5 * 3600,
    "MSD": 4 * 3600,
    "MSK": 3 * 3600,
    "MST": -7 * 3600,
    "MT": -7 * 3600,
    "MUT": 4 * 3600,
    "MVT": 5 * 3600,
    "MYT": 8 * 3600,
    "N": -1 * 3600,
    "NCT": 11 * 3600,
    "NDT": 2.5 * 3600,
    "NFT": 11 * 3600,
    "NOVST": 7 * 3600,
    "NOVT": 7 * 3600,
    "NPT": 5.5 * 3600,
    "NRT": 12 * 3600,
    "NST": 3.5 * 3600,
    "NUT": -11 * 3600,
    "NZDT": 13 * 3600,
    "NZST": 12 * 3600,
    "O": -2 * 3600,
    "OMSST": 7 * 3600,
    "OMST": 6 * 3600,
    "ORAT": 5 * 3600,
    "P": -3 * 3600,
    "PDT": -7 * 3600,
    "PET": -5 * 3600,
    "PETST": 12 * 3600,
    "PETT": 12 * 3600,
    "PGT": 10 * 3600,
    "PHOT": 13 * 3600,
    "PHT": 8 * 3600,
    "PKT": 5 * 3600,
    "PMDT": -2 * 3600,
    "PMST": -3 * 3600,
    "PONT": 11 * 3600,
    "PST": -8 * 3600,
    "PT": -8 * 3600,
    "PWT": 9 * 3600,
    "PYST": -3 * 3600,
    "PYT": -4 * 3600,
    "Q": -4 * 3600,
    "QYZT": 6 * 3600,
    "R": -5 * 3600,
    "RET": 4 * 3600,
    "ROTT": -3 * 3600,
    "S": -6 * 3600,
    "SAKT": 11 * 3600,
    "SAMT": 4 * 3600,
    "SAST": 2 * 3600,
    "SBT": 11 * 3600,
    "SCT": 4 * 3600,
    "SGT": 8 * 3600,
    "SRET": 11 * 3600,
    "SRT": -3 * 3600,
    "SST": -11 * 3600,
    "SYOT": 3 * 3600,
    "T": -7 * 3600,
    "TAHT": -10 * 3600,
    "TFT": 5 * 3600,
    "TJT": 5 * 3600,
    "TKT": 13 * 3600,
    "TLT": 9 * 3600,
    "TMT": 5 * 3600,
    "TOST": 14 * 3600,
    "TOT": 13 * 3600,
    "TRT": 3 * 3600,
    "TVT": 12 * 3600,
    "U": -8 * 3600,
    "ULAST": 9 * 3600,
    "ULAT": 8 * 3600,
    "UTC": 0 * 3600,
    "UYST": -2 * 3600,
    "UYT": -3 * 3600,
    "UZT": 5 * 3600,
    "V": -9 * 3600,
    "VET": -4 * 3600,
    "VLAST": 11 * 3600,
    "VLAT": 10 * 3600,
    "VOST": 6 * 3600,
    "VUT": 11 * 3600,
    "W": -10 * 3600,
    "WAKT": 12 * 3600,
    "WARST": -3 * 3600,
    "WAST": 2 * 3600,
    "WAT": 1 * 3600,
    "WEST": 1 * 3600,
    "WET": 0 * 3600,
    "WFT": 12 * 3600,
    "WGST": -2 * 3600,
    "WGT": -3 * 3600,
    "WIB": 7 * 3600,
    "WIT": 9 * 3600,
    "WITA": 8 * 3600,
    "WST": 14 * 3600,
    "WT": 0 * 3600,
    "X": -11 * 3600,
    "Y": -12 * 3600,
    "YAKST": 10 * 3600,
    "YAKT": 9 * 3600,
    "YAPT": 10 * 3600,
    "YEKST": 6 * 3600,
    "YEKT": 5 * 3600,
    "Z": 0 * 3600,
}
