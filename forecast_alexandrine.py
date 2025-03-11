import requests
from datetime import timedelta, datetime
from datetime import time as stime
import pandas as pd
from prophet import Prophet


def solar_forecast(pv_power, tstart, tend):
    country_code = 'BE'
    api_url = 'https://opendata.elia.be/api/records/1.0/search/'
    dataset = 'ods032'
    sort = 'datetime'
    q = f'datetime:[{tstart} TO {tend}]'
    rows = 288
    refine_region = 'Walloon-Brabant'
    params = {'dataset': dataset, 'q': q, 'refine.region': refine_region, 'rows': rows, 'sort': sort}

    raw_response = pd.json_normalize(requests.get(api_url, params=params).
                                     json()['records']).sort_values(by=['fields.datetime'])

    get_forecast = raw_response['fields.mostrecentforecast'] / raw_response['fields.monitoredcapacity']
    time_start = pd.to_datetime(raw_response['fields.datetime']).dt.strftime('%Y-%m-%d %H:%M:%S')
    time_end = (pd.to_datetime(raw_response['fields.datetime']) + timedelta(minutes=15)). \
        dt.strftime('%Y-%m-%d %H:%M:%S')
    # "datetime": "2023-10-24T22:00:00+00:00"

    solar_forecast = pd.DataFrame({'start_tstamp': time_start, 'end_tstamp': time_end,
                                 'value': get_forecast*pv_power, 'region': refine_region}).reset_index(drop=True)

    return solar_forecast

if __name__ == "__main__":
    # Paramètres de test
    pv_power = 3.5  # Puissance nominale (en kW)
    tstart = "2024-12-24T13:30:00"  # Timestamp de début
    tend = "2024-12-25T13:15:00"  # Timestamp de fin

    # Appeler la fonction
    forecast = solar_forecast(pv_power, tstart, tend)
    
    # Afficher les 5 premières lignes pour vérifier
    print(forecast.head())

# Exporter le tableau en fichier Excel
forecast.to_excel("solar_forecast.xlsx", index=False)
print("Fichier Excel exporté : solar_forecast.xlsx")

# Charger les données depuis Excel
data = pd.read_excel("0_topology_element_metrics_quarter_hourly.xlsx")
# Convertir la colonne 'bucket' en format datetime
data['bucket'] = pd.to_datetime(data['bucket'])
# Trier les données par ordre croissant en fonction de la colonne 'bucket'
data = data.sort_values(by='bucket').reset_index(drop=True)
# Afficher les 5 premières lignes pour vérifier
print(data.head())
# Exporter les données réorganisées si nécessaire
data.to_excel("1_sorted_consumption_data.xlsx", index=False)
print("Données réorganisées exportées : 1_sorted_consumption_data.xlsx")

# Préparer les données pour Prophet
prophet_data = data[['bucket', 'imported_energy']].rename(columns={'bucket': 'ds', 'imported_energy': 'y'})
print(prophet_data.head())
# Créer un modèle Prophet
model = Prophet()
# Ajuster le modèle avec les données historiques
model.fit(prophet_data)
# Créer un dataframe pour les 24 prochaines heures (15 min d'intervalle)
future = model.make_future_dataframe(periods=96, freq='15min')  # 96 quarts d'heure = 24 heures
# Faire les prévisions
forecast = model.predict(future)
# Afficher les prévisions
print(forecast[['ds', 'yhat', 'yhat_lower', 'yhat_upper']].tail(96))  # Dernières 96 lignes (24 heures)
# Exporter les prévisions si nécessaire
forecast.to_excel("2_forecast_consumption.xlsx", index=False)
print("Prévisions exportées : 2_forecast_consumption.xlsx")