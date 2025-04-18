import requests
import sys
import os
import gcsfs
import pandas as pd
from io import StringIO
from dotenv import load_dotenv
from google.cloud import storage

load_dotenv()

def upload_to_gcs(local_path: str, gcs_path: str):
    client = storage.Client()
    bucket_name, blob_path = gcs_path.replace("gs://", "").split("/", 1)
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_path)
    blob.upload_from_filename(local_path)

def convert_to_csv(text_data: str, local_path: str):
    lines = text_data.splitlines()
    data_lines = [line for line in lines if line.strip() and not line.startswith("#")]
    cleaned = "\n".join(data_lines)

    names = [
        'TM', 'STN', 'WD', 'WS', 'GST_WD', 'GST_WS', 'GST_TM',
        'PA', 'PS', 'PT', 'PR', 'TA', 'TD', 'HM', 'PV', 'RN',
        'RN_DAY', 'RN_JUN', 'RN_INT', 'SD_HR3', 'SD_DAY', 'SD_TOT',
        'WC', 'WP', 'WW', 'CA_TOT', 'CA_MID', 'CH_MIN', 'CT',
        'CT_TOP', 'CT_MID', 'CT_LOW', 'VS', 'SS', 'SI', 'ST_GD',
        'TS', 'TE_005', 'TE_01', 'TE_02', 'TE_03', 'ST_SEA', 'WH',
        'BF', 'IR', 'IX'
    ]

    df = pd.read_csv(StringIO(cleaned), sep=r"\s+", header=None)
    df.columns = names
    df.replace(["-9", -9, "-9.0", -9.0, "-"], value=pd.NA, inplace=True)
    
    df.to_csv(local_path, index=False, encoding="utf-8", lineterminator="\n")

def download_weather_raw_text(ds_nodash: str, local_path: str, gcs_path: str):
    auth_key = os.getenv("WEATHER_API_KEY")
    if not auth_key:
        raise ValueError("❌ WEATHER_API_KEY가 없습니다.")

    url = (
        f"https://apihub.kma.go.kr/api/typ01/url/kma_sfctm3.php?"
            f"tm1={ds_nodash}0000&tm2={ds_nodash}0000&stn=108&authKey={auth_key}"
    )
    response = requests.get(url)
    response.encoding = "utf-8"

    if response.status_code == 200:
        convert_to_csv(response.text, local_path)
        upload_to_gcs(local_path, gcs_path)

        print(f"✅ {ds_nodash} 날씨 데이터를 {save_path}로 저장 완료")
    else:
        print(f"❌ 요청 실패! 상태코드: {response.status_code}")
        print(response.text)

if __name__ == "__main__":
    ds_nodash = sys.argv[1]
    local_path = f"/home/joon/temp/weather/weather_raw-{ds_nodash}.csv"
    save_path = f"gs://weather_tunes/weather_raw-{ds_nodash}.csv"

    download_weather_raw_text(ds_nodash, local_path, save_path)
