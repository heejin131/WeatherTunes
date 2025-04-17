# 🌦️🎧 WeatherTunes
- **날씨에 따른 Spotify 음악 추천 시스템**

# **🎯 분석 목표**

> 날씨 및 기온에 따라 사람들이 어떤 유형의 음악을 즐겨 듣는지 분석
현재 및 내일 날씨에 맞는 유형의 음악 추천
> 

(특정 장르에 국한된 것이 아닌, BPM 위주로 신나는 곡인지 아닌지 판별)

<br>

# **🗒️ 요약**

- 일별 날씨 데이터 수집
- 일별 Spotify 기준 한국 Top 200 곡 데이터 수집
- 수집한 곡의 상세 데이터(BPM, Danceability 등) 수집
- 수집한 정보에서 필요한 정보만을 처리 후 적재
- 원하는 날씨에 사람들이 즐겨 듣는 음악과 비슷한 곡 추천

<br>

# **📚 내용**

### 데이터 수집 기간

`2023-01-01`- `2025-04-01`

### 수집 데이터 목록

<details>
<summary> 날씨 데이터 </summary>

- 기상청 API 허브 → 종관기상관측(ASOS) → 지상 관측자료 조회
- https://apihub.kma.go.kr/
- API url

```bash
https://apihub.kma.go.kr/api/typ01/url/kma_sfctm3.php?tm1=<YYYYMMDDHHmm>&tm2=<YYYYMMDDHHmm>&stn=108&help=1&authKey=<AUTH_KEY>
```

- 결과

```python
#--------------------------------------------------------------------------------------------------
#  기상청 지상관측 시간자료 [입력인수형태][예] ?tm=<YYYYMMDDHHmm>&stn=108&help=1
#--------------------------------------------------------------------------------------------------
#  1. TM     : 관측시각 (KST)
#  2. STN    : 국내 지점번호
#  3. WD     : 풍향 (16방위)
#  4. WS     : 풍속 (m/s)
#  5. GST_WD : 돌풍향 (16방위)
#  6. GST_WS : 돌풍속 (m/s)
#  7. GST_TM : 돌풍속이 관측된 시각 (시분)
#  8. PA     : 현지기압 (hPa)
#  9. PS     : 해면기압 (hPa)
# 10. PT     : 기압변화경향 (Code 0200) 
# 11. PR     : 기압변화량 (hPa)
# 12. TA     : 기온 (C)
# 13. TD     : 이슬점온도 (C)
# 14. HM     : 상대습도 (%)
# 15. PV     : 수증기압 (hPa)
# 16. RN     : 강수량 (mm) : 여름철에는 1시간강수량, 겨울철에는 3시간강수량
# 17. RN_DAY : 일강수량 (mm) : 해당시간까지 관측된 양(통계표)
# 18. RN_JUN : 일강수량 (mm) : 해당시간까지 관측된 양을 전문으로 입력한 값(전문)
# 19. RN_INT : 강수강도 (mm/h) : 관측하는 곳이 별로 없음
# 20. SD_HR3 : 3시간 신적설 (cm) : 3시간 동안 내린 신적설의 높이
# 21. SD_DAY : 일 신적설 (cm) : 00시00분부터 위 관측시간까지 내린 신적설의 높이
# 22. SD_TOT : 적설 (cm) : 치우지 않고 그냥 계속 쌓이도록 놔눈 경우의 적설의 높이
# 23. WC     : GTS 현재일기 (Code 4677)
# 24. WP     : GTS 과거일기 (Code 4561) .. 3(황사),4(안개),5(가랑비),6(비),7(눈),8(소나기),9(뇌전)
# 25. WW     : 국내식 일기코드 (문자열 22개) : 2자리씩 11개까지 기록 가능 (코드는 기상자원과 문의)
# 26. CA_TOT : 전운량 (1/10)
# 27. CA_MID : 중하층운량 (1/10)
# 28. CH_MIN : 최저운고 (100m)
# 29. CT     : 운형 (문자열 8개) : 2자리 코드로 4개까지 기록 가능
# 30. CT_TOP : GTS 상층운형 (Code 0509)
# 31. CT_MID : GTS 중층운형 (Code 0515)
# 32. CT_LOW : GTS 하층운형 (Code 0513)
# 33. VS     : 시정 (10m)
# 34. SS     : 일조 (hr)
# 35. SI     : 일사 (MJ/m2)
# 36. ST_GD  : 지면상태 코드 (코드는 기상자원과 문의)
# 37. TS     : 지면온도 (C)
# 38. TE_005 : 5cm 지중온도 (C)
# 39. TE_01  : 10cm 지중온도 (C)
# 40. TE_02  : 20cm 지중온도 (C)
# 41. TE_03  : 30cm 지중온도 (C)
# 42. ST_SEA : 해면상태 코드 (코드는 기상자원과 문의)
# 43. WH     : 파고 (m) : 해안관측소에서 목측한 값
# 44. BF     : Beaufart 최대풍력(GTS코드)
# 45. IR     : 강수자료 유무 (Code 1819) .. 1(Sec1에 포함), 2(Sec3에 포함), 3(무강수), 4(결측)
# 46. IX     : 유인관측/무인관측 및 일기 포함여부 (code 1860) .. 1,2,3(유인) 4,5,6(무인) / 1,4(포함), 2,5(생략), 3,6(결측)
#--------------------------------------------------------------------------------------------------
```

- 크기 예상: 하루 당 약 10KB

</details>

<details>
<summary> Spotify 곡 데이터 </summary>

### 1. Spotify Top 200 곡 정보 csv 파일 다운 (with Selenium)
    
- 링크: [https://charts.spotify.com/charts/view/regional-kr-daily/](https://charts.spotify.com/charts/view/regional-kr-daily/2025-04-10)<YYYY-MM-DD>
- csv 파일 정보
    
    <img width="691" alt="곡csv" src="https://github.com/user-attachments/assets/51d3d356-406c-44cf-ba0d-4798698cc3aa" />

    
- 크기 예상: 하루 당 약 20KB
    
### 2. Tunebat에서 1번에서 얻은 track_id를 이용하여 곡 정보 크롤링 (with Selenium)

- 링크: https://tunebat.com/Info/a/<TRACK_ID>
- Spotify audio-featrues API 지원 중단에 따라,
다음 사이트의 url에 Spotify 곡의 track_id를 추가하여 조회
- 저장할 데이터 정보
    
```json
{
    "track_id": <track_id>,
    "bpm": <bpm>,
    "energy": <energy>,
    "danceability": <danceability>
}
```
    
- 크기 예상: 하루 당 약 100B

</details>

<br>
    
#  🧺 Storage

> GCS 버킷
> 

```bash
bucket/
├── raw/                         ← 📥 API/크롤링 원본 저장소
│   ├── weather_raw/
│   │   └── partition: date
│   │   └── columns: raw weather data (TM, STN, WD, etc.)
│   │
│   └── songs_raw/
│       └── partition: date
│       └── columns: raw chart data (title, artist, rank, etc.)
│
BigQuery/
├── data/                        ← 🧹 전처리 완료된 분석 데이터
│   ├── weather_daily/           ← 🌦️ 평균기온, 범주화 등 정제 날씨
│   │   └── partition: date
│   │   └── columns: temperature, weather
│   │
│   ├── songs_top200/            ← 🎵 정제된 Top200 (track_id 포함)
│   │   └── partition: date
│   │   └── columns: track_id, artist_names, track_name, days_on_chart, streams
│   │
│   └── audio_features/          ← 🎧 Tunebat에서 추출한 곡 특성
│       └── partition: date
│       └── columns: track_id, BPM, Danceability, Happiness
│
└── meta/                        ← 🧠 날짜, 날씨, 기온별 곡에 대한 정보 (노이즈 x)
    └── meta_profile/
        └── partition: date
        └── subpartition: weather_main, temp_band
        └── columns: track_id, artist_names, track_name, BPM, Danceability, Happiness
```

<br>

# 🔄 데이터 파이프라인

![pipeline](https://github.com/user-attachments/assets/a4440711-9b1c-483b-8d05-269733b0dca4)


<br>

---

# 🤐 노이즈 

> 날씨랑 상관없이 항상 Top 200에 있는 `항상 인기 있는 곡(=노이즈)`은 분석에 방해가 된다.  
→ `기저효과(baseline effect)` 또는 `스테디셀러 노이즈`라고 불림

## 🚯 노이즈 제거 구분 (임시)

```python
df_filtered = df[df["days_on_chart"] < 30].reset_index(drop=True) # 30일 이상 곡들은 제외
```

| 필드명 | 내용 | 활용 |
| --- | --- | --- |
| days_on_chart | 차트 누적 진입일수 | 노이즈 제거 기준 |

<br>

# 🌤️ 날씨 구분

필요 정보: 기온, 전운량, 강수량, 신적설

- 기준 필드

| 필드명 | 내용 | 활용 |
| --- | --- | --- |
| CA_TOT | 전운량 (0~10) | ☀️ 맑음 / ☁️ 흐림 구분 |
| RN, RN_DAY | 강수량 (mm) | 🌧️ 비 판단 기준 |
| SD_DAY, SD_HR3 | 적설량 (cm) | ❄️ 눈 판단 기준 |

- 날씨 구분 코드

| 날씨 | 코드 |
| --- | --- |
| 맑음 | 0 |
| 흐림 | 1 |
| 비 | 2 |
| 눈 | 3 |

- 기온 구분 코드

| 기온 | 코드 |
| --- | --- |
| 0도 이하 | 0 |
| 0 - 10도 | 1 |
| 10 - 15도 | 2 |
| 15 - 20도 | 3 |
| 20 - 25도 | 4 |
| 25도 이상 | 5 |

---

# 🎼 추천 곡 알고리즘

1. parquet에서 곡 정보 불러오기
2. 평균값 및 표준편차 계산
3. z-score 방식으로 유사도 구하기
4. 가장 유사한 곡 Top 5 추출

---

# 📖 예상 시나리오

비가 오는 날 (weather=2), 기온 5℃ (temp=1) 평균

- BPM: 94.5
- Energy: 56
- Danceability: 54.75

맑은 날 (weather=0), 기온 -3℃(temp=0) 평균

- BPM: 111
- Energy: 69.5
- Danceability: 75.75

날씨가 비, 기온이 5℃인 경우,
`meta/weather=2/temp=1/*.parquet` 데이터를 통해 추천 곡 알고리즘을 실행하여 평균치와 비슷한 곡들을 추천


# 🔁 작업 흐름

- Raw 데이터 수집
- 전처리/데이터 파티셔닝
- Airflow
    - discord 메세지 예시(airflow 마지막 단계) (날씨정보 , 곡 추천)
        
        ```markup
        # 🎧 오늘의 감성 음악 추천 도착!  
        
        ### 📆 날짜: 2025-04-15 (화요일)  
        ### 🌤️ 날씨: 흐림 (평균 기온 18.5°C)
        
        ## 오늘 이런 음악 어떠세요?
        
        1. 🎵 **"Cloudy Memories"** - by Soft Season  
           🎚️ 템포: 102 BPM
           🔗 [Spotify에서 듣기](https://open.spotify.com/track/xxxxxxxx)
        
        2. 🎵 **"Raindrop Rhapsody"** - by Dream Keys  
           🎚️ 템포: 95 BPM
           🔗 [Spotify에서 듣기](https://open.spotify.com/track/yyyyyyyy)
        
        3. 🎵 **"Evening Glow"** - by Hana Lofi  
           🎚️ 템포: 108 BPM
           🔗 [Spotify에서 듣기](https://open.spotify.com/track/zzzzzzzz)
        
        ### 💬 내일도 당신의 하루에 맞는 음악을 준비할게요!
        
        ```
        
- Streamlit 시각화

## ⚒️ DAG 설계

### raw_weather

- start >> fetch_today_weather  >> end

### raw_songs

- start >> fetch_today_top200 >> end

### data_weather

- start >> clean_weather_data >> end

### data_songs

- start >> clean_song_data >> extract_audio_features >> end

### meta

- start >> generate_meta_profile >> copy_to_bigquery >> end

