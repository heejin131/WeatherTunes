import sys
import os
import requests, json
from dotenv import load_dotenv

load_dotenv()

def display_msg(data, ds: str):
    songs = [(track["track_id"], track["artist_names"], track["track_name"]) for i, track in enumerate(data)]
    msg = f"""
    # 🎧 오늘의 감성 음악 추천 도착!  

    ### 📆 날짜: {ds[:4]}-{ds[4:6]}-{ds[6:]}
    
    ## 오늘 이런 음악 어떠세요?
    
    """
    for i in range(3):
        msg += f"""
        {i + 1}. 🎵 **{songs[i][2]}** - by {songs[i][1]}  
        🔗 [Spotify에서 듣기](https://open.spotify.com/track/{songs[i][0]})

        """    
    
    return msg + "### 💬 내일도 당신의 하루에 맞는 음악을 준비할게요!"

def send_noti(ds: str):
    WEBHOOK_ID = os.getenv('DISCORD_WEBHOOK_ID')
    WEBHOOK_TOKEN = os.getenv('DISCORD_WEBHOOK_TOKEN')
    WEBHOOK_URL = f"https://discordapp.com/api/webhooks/{WEBHOOK_ID}/{WEBHOOK_TOKEN}"
    ds_nodash = ds.replace("-", "")
    
    gcs_path = f"gs://jacob_weathertunes/tmp/recommend_{ds_nodash}.json"
    local_path = f"/home/joon/temp/recommend/recommend_{ds_nodash}.json"
    
    os.system(f"gsutil cp {gcs_path} {local_path}")
    with open(local_path, "r", encoding="utf-8") as f:
        data = json.load(f)
    
    msg = display_msg(data, ds)
    
    data = { "content": msg }
    response = requests.post(WEBHOOK_URL, json=data)
    
    status_code = response.status_code
    if status_code == 204:
        print("메시지가 성공적으로 전송되었습니다.")
    else:
        print(f"에러 발생: {response.status_code}, {response.text}")
    return status_code

if __name__ == "__main__":
    ds = sys.argv[1]

    send_noti(ds)
