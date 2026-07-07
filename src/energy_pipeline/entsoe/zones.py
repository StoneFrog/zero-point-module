"""ENTSO-E bidding-zone EIC codes.

A "bidding zone" is a geographical area where a uniform electricity wholesale
price is formed. Most countries are one zone; some (Italy, Norway, Sweden,
Denmark) are split into multiple price areas.

The EIC (Energy Identification Code) is a 16-character ID assigned by ENTSO-E.
Full reference: https://www.entsoe.eu/data/energy-identification-codes-eic/
"""

from dataclasses import dataclass


@dataclass(frozen=True)
class BiddingZone:
    code: str  # short human label, used as partition value
    eic: str  # ENTSO-E EIC code, used in API calls
    name: str
    timezone: str  # IANA timezone for local-time interpretation


# Curated set: all major EU + neighbours that publish day-ahead prices to ENTSO-E.
# Add or remove entries here — the pipeline iterates this list.
ZONES: tuple[BiddingZone, ...] = (
    # Central / Eastern Europe
    BiddingZone("PL", "10YPL-AREA-----S", "Poland", "Europe/Warsaw"),
    BiddingZone("DE_LU", "10Y1001A1001A82H", "Germany-Luxembourg", "Europe/Berlin"),
    BiddingZone("CZ", "10YCZ-CEPS-----N", "Czech Republic", "Europe/Prague"),
    BiddingZone("SK", "10YSK-SEPS-----K", "Slovakia", "Europe/Bratislava"),
    BiddingZone("AT", "10YAT-APG------L", "Austria", "Europe/Vienna"),
    BiddingZone("HU", "10YHU-MAVIR----U", "Hungary", "Europe/Budapest"),
    BiddingZone("RO", "10YRO-TEL------P", "Romania", "Europe/Bucharest"),
    BiddingZone("BG", "10YCA-BULGARIA-R", "Bulgaria", "Europe/Sofia"),
    BiddingZone("SI", "10YSI-ELES-----O", "Slovenia", "Europe/Ljubljana"),
    BiddingZone("HR", "10YHR-HEP------M", "Croatia", "Europe/Zagreb"),
    BiddingZone("RS", "10YCS-SERBIATSOV", "Serbia", "Europe/Belgrade"),
    # Western Europe
    BiddingZone("FR", "10YFR-RTE------C", "France", "Europe/Paris"),
    BiddingZone("BE", "10YBE----------2", "Belgium", "Europe/Brussels"),
    BiddingZone("NL", "10YNL----------L", "Netherlands", "Europe/Amsterdam"),
    BiddingZone("CH", "10YCH-SWISSGRIDZ", "Switzerland", "Europe/Zurich"),
    # Iberia
    BiddingZone("ES", "10YES-REE------0", "Spain", "Europe/Madrid"),
    BiddingZone("PT", "10YPT-REN------W", "Portugal", "Europe/Lisbon"),
    # Italy (multiple zones)
    BiddingZone("IT_NORD", "10Y1001A1001A73I", "Italy North", "Europe/Rome"),
    BiddingZone("IT_CNOR", "10Y1001A1001A70O", "Italy Centre-North", "Europe/Rome"),
    BiddingZone("IT_CSUD", "10Y1001A1001A71M", "Italy Centre-South", "Europe/Rome"),
    BiddingZone("IT_SUD", "10Y1001A1001A788", "Italy South", "Europe/Rome"),
    BiddingZone("IT_SICI", "10Y1001A1001A75E", "Italy Sicily", "Europe/Rome"),
    BiddingZone("IT_SARD", "10Y1001A1001A74G", "Italy Sardinia", "Europe/Rome"),
    # Nordics
    BiddingZone("DK_1", "10YDK-1--------W", "Denmark West", "Europe/Copenhagen"),
    BiddingZone("DK_2", "10YDK-2--------M", "Denmark East", "Europe/Copenhagen"),
    BiddingZone("NO_1", "10YNO-1--------2", "Norway 1 (Oslo)", "Europe/Oslo"),
    BiddingZone("NO_2", "10YNO-2--------T", "Norway 2 (Kristiansand)", "Europe/Oslo"),
    BiddingZone("NO_3", "10YNO-3--------J", "Norway 3 (Trondheim)", "Europe/Oslo"),
    BiddingZone("NO_4", "10YNO-4--------9", "Norway 4 (Tromsø)", "Europe/Oslo"),
    BiddingZone("NO_5", "10Y1001A1001A48H", "Norway 5 (Bergen)", "Europe/Oslo"),
    BiddingZone("SE_1", "10Y1001A1001A44P", "Sweden 1 (Luleå)", "Europe/Stockholm"),
    BiddingZone("SE_2", "10Y1001A1001A45N", "Sweden 2 (Sundsvall)", "Europe/Stockholm"),
    BiddingZone("SE_3", "10Y1001A1001A46L", "Sweden 3 (Stockholm)", "Europe/Stockholm"),
    BiddingZone("SE_4", "10Y1001A1001A47J", "Sweden 4 (Malmö)", "Europe/Stockholm"),
    BiddingZone("FI", "10YFI-1--------U", "Finland", "Europe/Helsinki"),
    # Baltics
    BiddingZone("EE", "10Y1001A1001A39I", "Estonia", "Europe/Tallinn"),
    BiddingZone("LV", "10YLV-1001A00074", "Latvia", "Europe/Riga"),
    BiddingZone("LT", "10YLT-1001A0008Q", "Lithuania", "Europe/Vilnius"),
    # British Isles
    BiddingZone("GB", "10YGB----------A", "Great Britain", "Europe/London"),
    BiddingZone("IE_SEM", "10Y1001A1001A59C", "Ireland (SEM)", "Europe/Dublin"),
    # South-East
    BiddingZone("GR", "10YGR-HTSO-----Y", "Greece", "Europe/Athens"),
)


ZONES_BY_CODE = {z.code: z for z in ZONES}
