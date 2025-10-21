select a.name as athlete_name, a.country_code as country, a.nationality_code as nationality
from(
    select a.code, a.name, c.Latitude, c.Longitude
    from athletes a
    join venues v on a.
)
