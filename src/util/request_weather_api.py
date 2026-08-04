import requests
from airflow.exceptions import AirflowException


def request_weather_api(
    lats_str: str, lons_str: str, start_date: str, end_date: str, var_lst: list
) -> dict | list | None:

    # 參考文件: https://open-meteo.com/en/docs/historical-weather-api
    url = "https://archive-api.open-meteo.com/v1/archive"
    params = {
        "latitude": lats_str,
        "longitude": lons_str,
        "start_date": start_date,
        "end_date": end_date,
        "hourly": var_lst,
        "timezone": "Asia/Taipei",
    }

    header = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36"
    }
    try:
        print("發送請求中.......")
        print(f"Start date: {start_date}. End date: {end_date}")
        print(f"[緯度:{lats_str}, 經度{lons_str}].....")

        response = requests.get(url, params=params, timeout=120, headers=header)

        if response.status_code == 200:
            print("請求發送成功。")
            data = response.json()
            return data
        elif response.status_code == 429:
            print(f"Rate limit 429 reached at {lats_str}, {lons_str}")
            raise AirflowException("Rate limit hit 429!")

        response.raise_for_status()
    except requests.exceptions.Timeout:
        print("response.status_code: ", response.status_code)
        raise AirflowException("Timeout!")
    except requests.exceptions.ConnectionError:
        print("response.status_code: ", response.status_code)
        raise AirflowException("Connection error!")
    except requests.exceptions.HTTPError as e:
        print("response.status_code: ", response.status_code)
        raise AirflowException(f"HTTP error!{e}")
    except Exception as e:
        print("response.status_code: ", response.status_code)
        raise AirflowException(f"Unexpected error!{e}")
