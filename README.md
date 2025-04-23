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

`2023-01-01`- `2024-05-31`

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
│   │   └── 파일명 형식: weather_raw/weather_raw-*.csv       (예: weather_raw-20250401.csv)
│   │
│   └── songs_raw/
│       └── partition: date
│       └── 파일명 형식: songs_raw/*.csv                    (예: 2025-04-16.csv)
│
├── data/                        ← 🧹 전처리 완료된 분석 데이터
│   ├── weather_data/            ← 🌦️ 평균기온, 범주화 등 정제 날씨
│   │   └── partition: dt=YYYYMMDD
│   │   └── 파일명 형식: part-*.parquet                     (예: dt=20250402/part-00000-xxxx.parquet)
│   │
│   ├── songs_top200/            ← 🎵 정제된 Top200 (track_id 포함)
│   │   └── partition: dt=YYYYMMDD
│   │   └── 파일명 형식: part-*.parquet                     (예: dt=20250402/part-00000-xxxx.parquet)
│   │
│   └── audio_features/          ← 🎧 Tunebat에서 추출한 곡 특성
│       └── partition: 없음 (track_id 단위 저장)
│       └── 파일명 형식: <track_id>.parquet                (예: 7yZD4AVfQtAZD4cG8eRnPk.parquet)
│
├─── meta/                        ← 🧠 날짜, 날씨, 기온별 곡에 대한 정보 (노이즈 제거 완료)
│   └── meta_profile/
│       └── partition: dt=YYYYMMDD
│       └── subpartition: weather_code=N/temp_code=M
│       └── 파일명 형식: part-*.parquet                    (예: weather_code=0/temp_code=1/part-00000-80ead385-8190-4afe-8765-a3b0e975330a.c000.snappy.parquet))
│
└── tmp/                         ← 추천 곡 정보
    └── recommend/
        └── 파일명 형식: json (예: recommend_20230101.json)
```

<br>

# 📦 데이터 구성 안내

## 1. Raw 데이터

### 🟡 1.1 날씨 원본 (`raw/weather_raw/`)
- 수집 위치: `gs://jacob_weathertunes/raw/weather_raw/`
- 수집 방식: 기상청 OpenAPI
- 파티션: `dt=YYYYMMDD`
- 컬럼: API 응답 전체 포함 (TA, RN, SD_DAY, CA_TOT 등 포함 가능)
- 특징: 원본 수집용, 스키마 고정되지 않음
```
+------------+---+---+---+------+------+------+-----+------+----+----+----+----+----+----+----+------+------+------+------+------+------+----+----+----+------+------+------+----+------+------+------+----+----+----+-----+----+------+-----+-----+-----+------+----+----+----+----+
|         _c0|_c1|_c2|_c3|   _c4|   _c5|   _c6|  _c7|   _c8| _c9|_c10|_c11|_c12|_c13|_c14|_c15|  _c16|  _c17|  _c18|  _c19|  _c20|  _c21|_c22|_c23|_c24|  _c25|  _c26|  _c27|_c28|  _c29|  _c30|  _c31|_c32|_c33|_c34| _c35|_c36|  _c37| _c38| _c39| _c40|  _c41|_c42|_c43|_c44|_c45|
+------------+---+---+---+------+------+------+-----+------+----+----+----+----+----+----+----+------+------+------+------+------+------+----+----+----+------+------+------+----+------+------+------+----+----+----+-----+----+------+-----+-----+-----+------+----+----+----+----+
|          TM|STN| WD| WS|GST_WD|GST_WS|GST_TM|   PA|    PS|  PT|  PR|  TA|  TD|  HM|  PV|  RN|RN_DAY|RN_JUN|RN_INT|SD_HR3|SD_DAY|SD_TOT|  WC|  WP|  WW|CA_TOT|CA_MID|CH_MIN|  CT|CT_TOP|CT_MID|CT_LOW|  VS|  SS|  SI|ST_GD|  TS|TE_005|TE_01|TE_02|TE_03|ST_SEA|  WH|  BF|  IR|  IX|
|202308100000|108|  7|3.9|     7|  10.0|  2244|993.4|1003.1|   8|-0.5|25.1|21.8|82.0|26.1| 0.2|   0.2|   0.2|  NULL|  NULL|  NULL|  NULL|  60|  66|   1|    10|     5|    17|ScAs|  NULL|     2|     5|2000|NULL|NULL| NULL|24.3|  28.7| 28.9| 28.4| 28.4|  NULL|NULL|NULL|   4|   1|
|202308100100|108|  7|4.4|     7|  10.0|     6|992.3|1002.0|NULL|NULL|24.9|21.4|81.0|25.5| 0.0|   0.0|   0.0|  NULL|  NULL|  NULL|  NULL|  60|  66|   1|    10|     5|    17|ScAs|  NULL|     2|     5|2000|NULL|NULL| NULL|23.8|  28.3| 28.6| 28.2| 28.3|  NULL|NULL|NULL|   4|   1|
|202308100200|108|  5|3.0|  NULL|  NULL|  NULL|991.6|1001.2|NULL|NULL|24.7|21.6|83.0|25.8| 0.0|   0.0|   0.0|  NULL|  NULL|  NULL|  NULL|  60|  66|   1|    10|     3|    17|ScAs|  NULL|     2|     5|2000|NULL|NULL| NULL|23.9|  28.1| 28.4| 28.0| 28.2|  NULL|NULL|NULL|   4|   1|
|202308100300|108|  5|3.0|     7|  10.0|  2244|991.2|1000.8|   7|-2.3|24.2|21.3|84.0|25.3| 0.0|   0.0|   0.0|  NULL|  NULL|  NULL|  NULL|  60|  66|   1|    10|     4|    17|ScAs|  NULL|     2|     5|2000|NULL|NULL| NULL|23.6|  27.9| 28.2| 27.9| 28.2|  NULL|NULL|NULL|   4|   1|
|202308100400|108|  5|2.6|  NULL|  NULL|  NULL|990.9|1000.5|NULL|NULL|23.7|21.2|86.0|25.2| 0.3|   0.3|   0.3|  NULL|  NULL|  NULL|  NULL|  60|  66|   1|    10|     6|    15|ScAs|  NULL|     2|     5|2000|NULL|NULL| NULL|23.5|  27.7| 28.0| 27.7| 28.1|  NULL|NULL|NULL|   4|   1|
|202308100500|108|  5|4.6|  NULL|  NULL|  NULL|990.6|1000.3|NULL|NULL|23.1|21.1|89.0|25.0| 1.2|   1.5|   1.5|  NULL|  NULL|  NULL|  NULL|  61|  66|   1|    10|     7|    10|StNs|  NULL|     2|     7|1812|NULL|NULL| NULL|23.1|  27.4| 27.8| 27.6| 28.0|  NULL|NULL|NULL|   4|   1|
|202308100600|108|  5|3.5|     5|  10.0|   521|990.1| 999.8|   7|-1.0|22.8|20.7|88.0|24.4| 0.6|   2.1|   2.1|  NULL|  NULL|  NULL|  NULL|  61|  66|   1|    10|     7|    10|StNs|  NULL|     2|     7|2000| 0.0| 0.0| NULL|22.7|  27.1| 27.6| 27.4| 27.9|  NULL|NULL|NULL|   4|   1|
|202308100700|108|  7|2.5|  NULL|  NULL|  NULL|990.0| 999.7|NULL|NULL|22.9|20.7|88.0|24.4| 0.2|   2.3|   2.3|  NULL|  NULL|  NULL|  NULL|  61|  66|   1|    10|     7|    10|StNs|  NULL|     2|     7|2000| 0.0|0.03| NULL|23.1|  26.9| 27.3| 27.3| 27.8|  NULL|NULL|NULL|   4|   1|
|202308100800|108|  5|1.8|  NULL|  NULL|  NULL|990.0| 999.7|NULL|NULL|22.9|20.9|89.0|24.7| 1.7|   4.0|   4.0|  NULL|  NULL|  NULL|  NULL|  61|  66|   1|    10|     8|    10|StNs|  NULL|     2|     7|1023| 0.0|0.07| NULL|23.6|  26.8| 27.2| 27.1| 27.7|  NULL|NULL|NULL|   4|   1|
+------------+---+---+---+------+------+------+-----+------+----+----+----+----+----+----+----+------+------+------+------+------+------+----+----+----+------+------+------+----+------+------+------+----+----+----+-----+----+------+-----+-----+-----+------+----+----+----+----+
```

### 🟢 1.2 곡 원본 (`raw/songs_raw/`)
- 수집 위치: `gs://jacob_weathertunes/raw/songs_raw/`
- 수집 방식: Playwright + Spotify Charts
- 파티션: `dt=YYYYMMDD`
- 컬럼: 날짜, 아티스트명, 트랙명, 랭킹, 스트리밍 수 등 (형태 변경 가능)
- 특징: 차트 CSV 구조를 그대로 저장
```
+----+--------------------+--------------------+--------------------+--------------------+---------+-------------+-------------+-------+
| _c0|                 _c1|                 _c2|                 _c3|                 _c4|      _c5|          _c6|          _c7|    _c8|
+----+--------------------+--------------------+--------------------+--------------------+---------+-------------+-------------+-------+
|rank|                 uri|        artist_names|          track_name|              source|peak_rank|previous_rank|days_on_chart|streams|
|   1|spotify:track:3r8...|            NewJeans|               Ditto|                ADOR|        1|            1|           14|  75985|
|   2|spotify:track:0a4...|            NewJeans|            Hype Boy|                ADOR|        1|            2|          154|  34202|
|   3|spotify:track:4fs...|         LE SSERAFIM|         ANTIFRAGILE|        SOURCE MUSIC|        1|            3|           77|  26620|
|   4|spotify:track:6RB...|              Younha|       Event Horizon|    C9 Entertainment|        1|            4|           96|  23102|
|   5|spotify:track:2pI...|            NewJeans|           Attention|                ADOR|        1|            5|          154|  21934|
|   6|spotify:track:27b...|           NCT DREAM|               Candy|    SM Entertainment|        1|            6|           17|  20167|
|   7|spotify:track:1RD...|Jung Kook, BTS, F...|Dreamers [Music f...|Katara Studios | ...|        1|            7|           44|  19609|
|   8|spotify:track:2gY...|                 IVE|          After LIKE|Starship Entertai...|        1|            8|          133|  17484|
|   9|spotify:track:5Od...|Charlie Puth, Jun...|Left and Right (F...|    Atlantic Records|        1|           10|          192|  16952|
+----+--------------------+--------------------+--------------------+--------------------+---------+-------------+-------------+-------+
```
---

## 2. 정제 데이터 (Processed)

### 🔵 2.1 정제된 날씨 (`data/weather_daily/`)
- 수집 위치: `gs://jacob_weathertunes/data/weather_daily/`
- 전처리 방식: 기온 평균 및 날씨/기온 코드 부여
- 주요 컬럼: dt, temperature_avg, weather_code, temp_code
```
+------------+---------+--------+
|weather_code|temp_code|      dt|
+------------+---------+--------+
|           0|        0|20230104|
|           2|        4|20230714|
|           0|        1|20230223|
|           2|        5|20230727|
|           2|        0|20231217|
|           0|        0|20240127|
|           2|        4|20230830|
|           2|        1|20240220|
|           3|        0|20231224|
|           0|        4|20230603|
+------------+---------+--------+
```
### 🟣 2.2 정제된 Top200 (`data/songs_top200/`)
- 수집 위치: `gs://jacob_weathertunes/data/songs_top200/`
- 주요 컬럼: track_id, artist_names, track_name, streams, days_on_chart
- 기준: Spotify 기준 하루 단위 Top 200
  
```
+--------------------+--------------------+--------------------+-------------+-------+--------+
|            track_id|        artist_names|          track_name|days_on_chart|streams|      dt|
+--------------------+--------------------+--------------------+-------------+-------+--------+
|3r8RuvgbX9s7ammBn...|            NewJeans|               Ditto|           25|  84117|20230112|
|65FftemJ1DbbZ45DU...|            NewJeans|                 OMG|           11|  79214|20230112|
|0a4MMyCrzT0En247I...|            NewJeans|            Hype Boy|          165|  36789|20230112|
|2pIUpMhHL6L9Z5lnK...|            NewJeans|           Attention|          165|  25927|20230112|
|4fsQ0K37TOXa3hEQf...|         LE SSERAFIM|         ANTIFRAGILE|           88|  24301|20230112|
|1Qrg8KqiBpW07V7PN...|                 SZA|           Kill Bill|           35|  20321|20230112|
|27bIik73QCu8Xzt3x...|           NCT DREAM|               Candy|           28|  18926|20230112|
|6RBziRcDeiho3iTPd...|              Younha|       Event Horizon|          107|  18707|20230112|
|1RDvyOk4WtPCtoqci...|Jung Kook, BTS, F...|Dreamers [Music f...|           55|  18279|20230112|
|5Odq8ohlgIbQKMZiv...|Charlie Puth, Jun...|Left and Right (F...|          203|  17484|20230112|
+--------------------+--------------------+--------------------+-------------+-------+--------+
```

### 🟤 2.3 오디오 피처 (`data/audio_features/`)
- 수집 위치: `gs://jacob_weathertunes/data/audio_features/`
- 수집 방식: Tunebat + Selenium 크롤링
- 주요 컬럼: track_id, BPM, Danceability, Happiness
```
+--------------------+---+------------+---------+
|            track_id|BPM|Danceability|Happiness|
+--------------------+---+------------+---------+
|008hCrBLW4a7BbC6c...|155|          67|       38|
|00Apys6jYrWA0Bse9...|110|          63|       29|
|0139mjI8ijLp4SYOG...|140|          76|       66|
|01d3IKaDzqxP50zPZ...| 94|          51|       37|
|01nC1XQoc7jqWDJdv...|150|          37|       36|
|01qFKNWq73UfEslI0...|108|          85|       89|
|02H58MSfVESkKyx4d...| 88|          77|       34|
|02MWAaffLxlfxAUY7...| 81|          76|       53|
|02U9EixxU2Znl6ils...|108|          83|       38|
|02VBYrHfVwfEWXk5D...|148|          59|       72|
+--------------------+---+------------+---------+
```

---

## 3. 메타데이터 (`meta/meta_profile/`)

- 저장 위치: `gs://jacob_weathertunes/meta/dt=YYYYMMDD/weather_code=N/temp_code=M/`
- 주요 컬럼: track_id, artist_names, track_name, streams, BPM, Danceability, Happiness, dt, weather_code, temp_code
```
+--------------------+--------------------+--------------------+-------+---+------------+---------+--------+------------+---------+
|            track_id|        artist_names|          track_name|streams|BPM|danceability|happiness|      dt|weather_code|temp_code|
+--------------------+--------------------+--------------------+-------+---+------------+---------+--------+------------+---------+
|51vRumtqbkNW9wrKf...|LE SSERAFIM, Nile...|UNFORGIVEN (feat....|  60448|104|          80|       38|20230503|           1|        3|
|70t7Q6AYG6ZgTYmJW...|                 IVE|                I AM|  49756|122|          68|       38|20230503|           1|        3|
|3AOf6YEpxQ894Fmrw...|           SEVENTEEN|               Super|  27278|137|          77|       35|20230503|           1|        3|
|7ovUcF5uHTBRzUpB6...|             YOASOBI|            アイドル|  22317|166|          57|       84|20230503|           1|        3|
|4bjN59DRXFRxBE1g5...|             Agust D|             Haegeum|  20270| 85|          70|       83|20230503|           1|        3|
|6DSGb5CmwHX4pvclq...|         LE SSERAFIM|No-Return (Into t...|  15963|158|          75|       95|20230503|           1|        3|
|4QhnNyKDsAkXPwHkS...|         LE SSERAFIM|Eve, Psyche & The...|  15579|143|          66|       63|20230503|           1|        3|
|0jd4aa9XgV5eom0ez...|       NCT DOJAEJUNG|             Perfume|  15515|106|          72|       70|20230503|           1|        3|
|41JPN7pZMTp1sumBO...|         Agust D, IU|People Pt.2 (feat...|  14877| 89|          73|       44|20230503|           1|        3|
|6qVqWJxIpsabUKBIL...|        aespa, nævis|Welcome To MY Wor...|  13378|152|          60|       37|20230503|           1|        3|
+--------------------+--------------------+--------------------+-------+---+------------+---------+--------+------------+---------+
```

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
![image](https://github.com/user-attachments/assets/b7aff9c5-2014-4811-936b-c9dc86e5235f)

- Raw 데이터 수집
- 전처리/데이터 파티셔닝
- Airflow
    - discord 메세지 예시(airflow 마지막 단계) (날씨정보 , 곡 추천)
      
![image](https://github.com/user-attachments/assets/efd050b8-294a-42af-8235-a12f510ace01)


## ⚒️ DAG 설계

### raw_weather

- start >> fetch_today_weather >> end

### raw_songs

- start >> scrape_today_top200 >> end

### data_weather

- start >> clean_weather_data >> end

### data_songs

- start >> clean_songs_top200 >> end

### data_audio_features

- start >> scrape_audio_features >> end

### meta

- start >> generate_meta_profile >> copy_to_bigquery >> end

### Recommendation

- start >> load_recommened_songs >> send_nofitication >> end

# 🌿 Git 브랜치 전략

> 기능 단위로 나눠 개발되며, `0.1/develop` 브랜치에 통합됩니다.

| 브랜치명                    | 설명 |
|-----------------------------|------|
| `0.1/develop`               | 전체 통합 개발 브랜치 |
| `0.1/feature-data_weather`  | 기상청 날씨 데이터 수집 및 전처리 |
| `0.1/feature-raw_songs`     | Spotify Top 200 곡 수집 |
| `0.1/feature-data_songs`    | Spotify Top 200 곡 전처리|
| `0.1/feature-data_songs`    | Tunebat 에서 audio feat수집|
| `0.1/features-meta`         | 날씨+곡 결합 메타데이터 생성 |
| `0.1/features-recommendation` | 추천 알고리즘 개발 및 시각화 연동 |

> 브랜치 병합은 PR(Pull Request)을 통해 단계적으로 리뷰 후 수행됩니다.
