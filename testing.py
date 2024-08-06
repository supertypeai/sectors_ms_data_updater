# untuk kebutuhan testing apakah bisa atau ga

import requests

url = "https://morning-star.p.rapidapi.com/stock/v2/get-financials"

querystring = {"interval":"annual","performanceId":"0P0000OQN8","reportType":"A"}

headers = {
	"x-rapidapi-key": "48091acff8mshda25395bc680279p1c1e23jsnceb0e6936125",
	"x-rapidapi-host": "morning-star.p.rapidapi.com"
}

response = requests.get(url, headers=headers, params=querystring)

print(response.json())