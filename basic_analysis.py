import pandas as pd
import os
import re
import sys
from collections import defaultdict
from io import StringIO

# --- 설정 ---
DATA_DIR = './data/'
OUTPUT_DIR = './outputs/basic/'
LOG_FILE = os.path.join(OUTPUT_DIR, 'analysis_results.txt')

NICE_CLASS_DESC = {
    '1': '화학품', '2': '도료/염료', '3': '화장품/세정제', '4': '산업용 유지', 
    '5': '약제/의약품/위생재', '6': '금속제품', '7': '기계/공작기계', '8': '수공구', 
    '9': '과학/전자/컴퓨터 하드웨어 및 소프트웨어', '10': '의료용 기기/용품', '11': '조명/냉난방/건조 장치', 
    '12': '탈것', '14': '귀금속/보석/시계', '16': '종이/문구', '18': '피혁/가죽제품', 
    '20': '가구/거울/액자', '21': '가정용구/유리/자기', '25': '의류/신발/모자', 
    '29': '가공식품/육류/유제품', '30': '커피/차/제과', '31': '농산물/비가공 식품/동물사료', 
    '35': '광고/경영관리', '36': '보험/금융', '38': '통신', '41': '교육/오락/스포츠', 
    '42': '과학/기술 서비스/IT 서비스', '43': '음식점업/임시숙박업', '44': '의료/미용/농업 서비스',
    '45': '법률/보안/개인 서비스', '기타': '기타 분류'
}


# pandas DataFrame이나 Series를 print할 때, 터미널/파일 출력 포맷을 조정
# DataFrame의 모든 행과 열을 출력하도록 설정 (Truncation 방지)
pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.width', 1000)
pd.set_option('display.colheader_justify', 'left')
pd.set_option('display.precision', 2) # 소수점 자리수 조정

# 기존 함수들 (load_all_data, preprocess_data, analyze_time_series, analyze_category, analyze_comparison, analyze_text)
# 는 요청하신 대로 유지됩니다. 여기서는 파일 크기 문제로 생략합니다.

def load_all_data(data_dir):
    """data 폴더 내의 모든 .xlsx 파일을 로드하고 하나의 DataFrame으로 통합합니다."""
    all_dfs = []
    file_list = [f for f in os.listdir(data_dir) if f.endswith('.xlsx')]
    
    country_map = {f: f.split('_')[0].replace('DATA.xlsx', '').replace('.xlsx', '') for f in file_list}

    print("### 1. 데이터 로드 및 통합 시작 ###")
    for file_name in file_list:
        file_path = os.path.join(data_dir, file_name)
        country_name = country_map.get(file_name, 'Unknown')
        
        try:
            df = pd.read_excel(file_path)
            df['국가'] = country_name
            all_dfs.append(df)
            print(f"-> 로드 완료: {file_name} (총 {len(df)} 행)")
        except Exception as e:
            print(f"-> 오류 발생: {file_name} 로드 실패 - {e}")

    if all_dfs:
        combined_df = pd.concat(all_dfs, ignore_index=True)
        print(f"\n총 통합 데이터프레임 크기: {len(combined_df)} 행")
        return combined_df
    else:
        print("경고: 로드할 파일이 없습니다.")
        return pd.DataFrame()


def preprocess_data(df):
    """통합 데이터에 대한 전처리를 수행합니다."""
    print("\n### 2. 데이터 전처리 ###")
    
    df['출원일자'] = pd.to_datetime(df['출원일자'], errors='coerce')
    print(f"-> '출원일자' 컬럼을 datetime 형식으로 변환 완료. (변환 불가한 값: {df['출원일자'].isna().sum()}개)")
    
    df['주요_류'] = df['류'].astype(str).apply(lambda x: x.split('//')[0].strip())
    df['주요_류'] = df['주요_류'].str.extract(r'(\d+)').fillna('기타').astype(str)
    print("-> '류' 컬럼 정제하여 '주요_류' 컬럼 생성 완료.")
    
    df['상표명칭'].fillna('(상표명칭 정보 없음)', inplace=True)
    print("-> '상표명칭' 컬럼 결측치 처리 완료.")
    
    return df


def analyze_time_series(df):
    """연도별 출원 트렌드를 분석하고, 국가별 Top 5 출원 연도를 출력합니다."""
    print("\n### 3. 시계열 트렌드 분석 ###")
    
    df_ts = df.dropna(subset=['출원일자']).copy()
    df_ts['출원연도'] = df_ts['출원일자'].dt.year
    
    yearly_counts = df_ts.groupby(['출원연도', '국가']).size().reset_index(name='출원수')
    
    print("💡 국가별 출원 건수 Top 5 연도:\n")
    
    top_5_yearly_counts = yearly_counts.groupby('국가').apply(
        lambda x: x.sort_values(by='출원수', ascending=False).head(5)
    ).reset_index(drop=True)
    
    for country in top_5_yearly_counts['국가'].unique():
        print(f"**--- {country} ---**")
        output_df = top_5_yearly_counts[top_5_yearly_counts['국가'] == country].sort_values(
            by='출원수', ascending=False
        ).reset_index(drop=True)
        print(output_df[['출원연도', '출원수']])
        print("---")

    max_year = yearly_counts['출원연도'].max()
    start_year = max_year - 4 
    
    cagr_results = []
    for country in yearly_counts['국가'].unique():
        country_data = yearly_counts[yearly_counts['국가'] == country]
        
        start_count_row = country_data[country_data['출원연도'] == start_year]
        end_count_row = country_data[country_data['출원연도'] == max_year]
        
        if not start_count_row.empty and not end_count_row.empty:
            beginning_value = start_count_row['출원수'].iloc[0]
            ending_value = end_count_row['출원수'].iloc[0]
            n = max_year - start_year
            
            if beginning_value > 0:
                cagr = (ending_value / beginning_value) ** (1/n) - 1
                cagr_results.append({'국가': country, f'{start_year}-{max_year} CAGR': f'{cagr * 100:.2f}%'})

    print(f"\n💡 최근 5년 CAGR ({start_year}년 대비 {max_year}년):\n", pd.DataFrame(cagr_results))


def analyze_category(df):
    """주요 류(Class)를 기반으로 국가별 산업 특성을 분석하고 류 설명을 함께 표시합니다."""
    print("\n### 4. 산업 및 분류 분석 (주요_류 기준) ###")
    
    country_class_counts = df.groupby('국가')['주요_류'].value_counts(normalize=True).mul(100).rename('비중(%)').reset_index()
    
    country_class_counts['류_설명'] = country_class_counts['주요_류'].astype(str).map(NICE_CLASS_DESC).fillna('설명 없음')
    
    top_classes = country_class_counts.groupby('국가').head(5).sort_values(by=['국가', '비중(%)'], ascending=[True, False])

    print("💡 국가별 상위 5개 주요_류 비중 및 설명:\n")
    
    for country in top_classes['국가'].unique():
        print(f"**--- {country} ---**")
        output_df = top_classes[top_classes['국가'] == country][['주요_류', '류_설명', '비중(%)']]
        print(output_df)
        print("---")


def analyze_comparison(df):
    """국가별 포트폴리오 다양성과 지정상품 개수를 비교 분석합니다."""
    print("\n### 5. 글로벌 비교 분석 ###")
    
    diversity_data = []
    for country in df['국가'].unique():
        country_df = df[df['국가'] == country]
        unique_classes = sorted(country_df['주요_류'].unique().tolist())
        
        class_with_desc = []
        for class_code in unique_classes:
            desc = NICE_CLASS_DESC.get(class_code, '설명 없음')
            class_with_desc.append(f"{class_code} ({desc})")
        
        diversity_data.append({
            '국가': country,
            '고유_류_개수': len(unique_classes),
            '포함된_류_종류': ', '.join(class_with_desc)
        })
        
    diversity_df = pd.DataFrame(diversity_data).sort_values(by='고유_류_개수', ascending=False).reset_index(drop=True)
    print("💡 국가별 포트폴리오 다양성 (고유 류 개수 및 종류):\n", diversity_df)
    
    df['지정상품_개수'] = df['지정상품'].astype(str).apply(lambda x: len(re.split(r'//|,|\n', x)))
    
    avg_goods = df.groupby('국가')['지정상품_개수'].mean().sort_values(ascending=False).reset_index(name='평균_지정상품_수')
    print("\n💡 국가별 출원 건당 평균 지정상품 수:\n", avg_goods)


def analyze_text(df):
    """상표명 길이, 상표명 키워드, 지정상품 키워드 분석을 수행합니다."""
    print("\n### 6. 텍스트 마이닝 (Text Mining & NLP) ###")
    
    df['상표명_길이'] = df['상표명칭'].astype(str).apply(lambda x: len(re.sub(r'\s|\(|\)', '', x)))
    length_summary = df.groupby('국가')['상표명_길이'].agg(['mean', 'median', 'min', 'max']).sort_values(by='mean', ascending=False)
    print("💡 1-1. 국가별 상표명 길이 요약 통계:\n", length_summary)
    
    print("\n💡 2. 국가별 상표명 상위 키워드 트렌드 (빈도 분석):")
    
    STOP_WORDS = ['the', 'and', 'of', 'for', 'in', 'a', 'trade', 'mark', 'ltd', 'inc', 'co', 'group']
    
    for country in df['국가'].unique():
        country_names = df[df['국가'] == country]['상표명칭'].astype(str).str.lower()
        
        all_words = []
        for name in country_names:
            words = re.sub(r'[^a-z0-9\s]', '', name).split()
            words = [word for word in words if word not in STOP_WORDS and len(word) > 2]
            all_words.extend(words)
            
        word_counts = pd.Series(all_words).value_counts().head(10)
        
        if not word_counts.empty:
            print(f"**--- {country} 상표명 Top 10 키워드 ---**")
            print(word_counts)
        else:
            print(f"**--- {country} ---** 키워드 분석 불가 또는 데이터 부족 (주로 비영문 데이터인 경우)")
    
    print("\n💡 3. 지정상품 상위 키워드 분석 (국가별 Top 10):")
    
    for country in df['국가'].unique():
        country_goods = df[df['국가'] == country]['지정상품'].astype(str).str.lower()
        
        keyword_counts = defaultdict(int)
        
        for text in country_goods:
            goods_list = re.split(r'[//,\n]', text)
            for good in goods_list:
                good = good.strip()
                if good and len(good) > 5 and good not in STOP_WORDS:
                    keyword_counts[good] += 1
                    
        top_goods_keywords = pd.Series(keyword_counts).sort_values(ascending=False).head(10)
        
        if not top_goods_keywords.empty:
            print(f"\n**--- {country} 지정상품 Top 10 키워드 ---**")
            print(top_goods_keywords)
        else:
            print(f"\n**--- {country} ---** 지정상품 키워드 분석 불가")


# --- 메인 실행 함수 (출력 파일 저장 로직 추가) ---
if __name__ == "__main__":
    
    # 1. 출력 디렉토리 생성
    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR)
    
    # 표준 출력(sys.stdout)을 메모리 버퍼로 리다이렉션
    original_stdout = sys.stdout
    string_buffer = StringIO()
    sys.stdout = string_buffer

    try:
        # 분석 실행
        # 1. 모든 데이터 로드 및 통합
        all_data = load_all_data(DATA_DIR)

        if not all_data.empty:
            # 2. 데이터 전처리
            processed_data = preprocess_data(all_data)

            # 3. 시계열 트렌드 분석
            analyze_time_series(processed_data)
            
            # 4. 산업 및 분류 분석
            analyze_category(processed_data)

            # 5. 글로벌 비교 분석
            analyze_comparison(processed_data)

            # 6. 텍스트 마이닝
            analyze_text(processed_data)
            
            print("\n--- 기본 분석 완료 ---")

    finally:
        # 2. 파일 저장
        analysis_results = string_buffer.getvalue()
        with open(LOG_FILE, 'w', encoding='utf-8') as f:
            f.write(analysis_results)
        
        # 3. 표준 출력 복원
        sys.stdout = original_stdout
        
        print(f"\n✅ 분석 결과가 '{LOG_FILE}' 파일에 성공적으로 저장되었습니다.")
        print(f"   (출력 디렉토리: {os.path.abspath(OUTPUT_DIR)})")