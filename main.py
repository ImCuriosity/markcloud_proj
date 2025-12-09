import pandas as pd
import os

# 데이터 파일이 있는 폴더 경로
data_dir = './data/'

# data 폴더 내의 모든 파일 목록을 가져옵니다.
file_list = os.listdir(data_dir)

# .xlsx 파일만 필터링합니다.
xlsx_files = [f for f in file_list if f.endswith('.xlsx')]

print("--- 데이터 로드 시작 ---")

# 각 .xlsx 파일을 순회하며 로드하고 정보를 출력합니다.
for file_name in xlsx_files:
    file_path = os.path.join(data_dir, file_name)
    print(f"\n✅ 파일 로드 중: **{file_name}**")
    
    try:
        # Pandas의 read_excel 함수를 사용하여 데이터를 DataFrame으로 로드합니다.
        # 첫 번째 시트를 로드하며, 시트 이름이 필요하다면 sheet_name='시트이름'을 사용합니다.
        df = pd.read_excel(file_path)
        
        # 데이터프레임의 기본 정보 출력
        print("💡 데이터프레임 정보 (df.info()):")
        df.info()
        
        # 데이터의 처음 5줄 출력
        print("\n💡 데이터의 처음 5줄 (df.head()):")
        print(df.head())
        
        print(f"\n📊 {file_name} 파일 로드 완료! (총 {len(df)} 행)")

    except FileNotFoundError:
        print(f"❌ 오류: 파일을 찾을 수 없습니다. 경로를 확인해주세요: {file_path}")
    except Exception as e:
        print(f"❌ 오류: 파일을 읽는 중 문제가 발생했습니다: {e}")

print("\n--- 데이터 로드 완료 ---")