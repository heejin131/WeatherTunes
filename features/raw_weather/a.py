import requests
import sys
import os
import gcsfs
import pandas as pd
from io import StringIO
from dotenv import load_dotenv

load_dotenv()

def convert_to_csv(text_data: str, save_path: str):
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

    df = pd.read_csv(StringIO(cleaned), delim_whitespace=True, header=None)
    df.columns = names
    df.replace(["-9", -9, "-9.0", -9.0, "-"], value=pd.NA, inplace=True)

    fs = gcsfs.GCSFileSystem()
    with fs.open(save_path, mode='wt', encoding='utf-8', newline='\n') as f:
        df.to_csv(f, index=False)

def download_weather_raw_text(ds_nodash: str, save_path: str):
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
        convert_to_csv(response.text, save_path)
        print(f"✅ {ds_nodash} 날씨 데이터를 {save_path}로 저장 완료")
    else:
        print(f"❌ 요청 실패! 상태코드: {response.status_code}")
        print(response.text)

if __name__ == "__main__":
    ds_nodash = sys.argv[1]
    save_path = f"gs://weather_tunes/weather_raw-{ds_nodash}.csv"

    download_weather_raw_text(ds_nodash, save_path)
