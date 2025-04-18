import pandas as pd
import os

date_str = "2025-04-17"
mock_path = f"/home/airflow/gcs/data/songs_top200/dt={date_str}/songs_top200.parquet"

os.makedirs(os.path.dirname(mock_path), exist_ok=True)

df_mock = pd.DataFrame({
    "track_id": [
        "1qCHUbe8BuHymkHuzHEYoi",  # Seethru
        # 여기에 다른 Spotify 트랙 ID를 더 추가해도 됨
    ],
    "track_name": ["Seethru"],
    "artist_names": ["Primary, Zion.T, Gaeko"],
    "days_on_chart": [5],
})

df_mock.to_parquet(mock_path, index=False)
print(f"✅ 테스트용 parquet 저장 완료: {mock_path}")
