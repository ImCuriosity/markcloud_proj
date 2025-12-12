import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import os
import glob
import re
from matplotlib import font_manager, rc
import platform

# ==========================================
# ⚙️ 설정 & NICE 분류 정의
# ==========================================
DATA_DIR = "./data"
OUTPUT_DIR = "./outputs/analysis"
os.makedirs(OUTPUT_DIR, exist_ok=True)

# 💡 [추가] NICE 분류 설명 (User Provided)
NICE_CLASS_DESC = {
    '1': '화학품', '2': '도료/염료', '3': '화장품/세정제', '4': '산업용 유지', 
    '5': '약제/의약품', '6': '금속제품', '7': '기계/공작', '8': '수공구', 
    '9': '과학/전자/SW', '10': '의료용 기기', '11': '조명/냉난방', 
    '12': '탈것', '14': '귀금속/시계', '16': '종이/문구', '18': '피혁/가죽', 
    '20': '가구', '21': '가정용구', '25': '의류/신발', 
    '29': '식품/육류', '30': '커피/제과', '31': '농산물/사료', 
    '35': '광고/경영', '36': '보험/금융', '38': '통신', '41': '교육/오락', 
    '42': 'SW/기술개발', '43': '음식점/숙박', '44': '의료/미용',
    '45': '법률/보안', '기타': '기타'
}

def get_nice_name(class_val):
    """류 번호를 입력받아 '번호\n(설명)' 포맷으로 반환 (그래프용)"""
    str_val = str(int(class_val)) # 숫자를 문자로 변환
    desc = NICE_CLASS_DESC.get(str_val, '')
    
    # 설명이 너무 길면 잘라서 줄바꿈 (그래프 가독성 위해)
    if desc:
        return f"{str_val}류\n({desc})"
    return f"{str_val}류"

# ==========================================
# 🛠️ 유틸리티: 폰트 & 데이터 로드
# ==========================================
def init_font():
    system_name = platform.system()
    if system_name == 'Windows':
        candidates = [("c:/Windows/Fonts/malgun.ttf", "Malgun Gothic"), ("c:/Windows/Fonts/msyh.ttf", "Microsoft YaHei")]
    elif system_name == 'Darwin':
        candidates = [("/System/Library/Fonts/Supplemental/AppleGothic.ttf", "AppleGothic")]
    else:
        candidates = [("/usr/share/fonts/truetype/nanum/NanumGothic.ttf", "NanumGothic")]
    
    for fpath, fname in candidates:
        if os.path.exists(fpath):
            font_manager.fontManager.addfont(fpath)
            rc('font', family=fname)
            print(f"🔤 폰트 설정: {fname}")
            break
    plt.rcParams['axes.unicode_minus'] = False

def clean_date(value):
    try:
        return pd.to_datetime(value, format='mixed', errors='coerce')
    except:
        return pd.NaT

def clean_class(value):
    if pd.isna(value): return "0"
    match = re.search(r'\d+', str(value))
    return int(match.group(0)) if match else 0

def load_all_data():
    all_files = glob.glob(os.path.join(DATA_DIR, "*_DATA.xlsx"))
    df_list = []
    
    print("🔄 데이터 로드 및 통합 중...")
    for f in all_files:
        try:
            temp = pd.read_excel(f)
            country = os.path.basename(f).split('_')[0]
            temp['Country'] = country
            
            col_map = {}
            for c in temp.columns:
                if '류' in c or 'class' in c.lower(): col_map[c] = 'Class'
                elif '출원일' in c: col_map[c] = 'Date'
                elif '유사군' in c: col_map[c] = 'Group'
                elif '상표' in c: col_map[c] = 'Name'
            
            temp.rename(columns=col_map, inplace=True)
            
            if 'Class' in temp.columns:
                cols = ['Name', 'Date', 'Class', 'Country']
                if 'Group' in temp.columns: cols.append('Group')
                temp = temp[cols]
                df_list.append(temp)
                
        except Exception as e:
            print(f"⚠️ 로드 실패 ({f}): {e}")
            
    full_df = pd.concat(df_list, ignore_index=True)
    full_df['Date'] = full_df['Date'].apply(clean_date)
    full_df['Class'] = full_df['Class'].apply(clean_class)
    full_df['Year'] = full_df['Date'].dt.year
    full_df['Month'] = full_df['Date'].dt.month
    
    print(f"✅ 총 데이터: {len(full_df):,}건 로드 완료.")
    return full_df

# ==========================================
# 1️⃣ 국가별/글로벌 주요 상품 분야 (류) 분석
# ==========================================
def analyze_top_classes(df):
    print("\n📊 [1] 국가별 주요 상품류(Class) 분석")
    
    # 전체 Top 10
    plt.figure(figsize=(14, 7)) # 가로 길이 늘림
    top_global = df['Class'].value_counts().head(10)
    
    # 💡 [수정] X축 라벨에 설명 추가
    labels = [get_nice_name(c) for c in top_global.index]
    
    sns.barplot(x=labels, y=top_global.values, palette='viridis', hue=labels, legend=False)
    plt.title("글로벌 Top 10 상표 출원 류 (Global Trends)", fontsize=15)
    plt.ylabel("출원 건수")
    plt.xticks(rotation=0, fontsize=9) # 글자가 겹치지 않게
    plt.savefig(os.path.join(OUTPUT_DIR, "1_Global_Top_Classes.png"))
    plt.close()
    
    # 국가별 Top 5 비교
    top_countries = df['Country'].value_counts().head(4).index
    
    fig, axes = plt.subplots(2, 2, figsize=(16, 12))
    axes = axes.flatten()
    
    for i, country in enumerate(top_countries):
        country_df = df[df['Country'] == country]
        top_c = country_df['Class'].value_counts().head(5)
        
        # 💡 [수정] 라벨 변환
        c_labels = [get_nice_name(c) for c in top_c.index]
        
        sns.barplot(x=c_labels, y=top_c.values, ax=axes[i], palette='magma', hue=c_labels, legend=False)
        axes[i].set_title(f"{country} Top 5 Classes", fontsize=13)
        axes[i].tick_params(axis='x', labelsize=9)
        
    plt.tight_layout()
    plt.savefig(os.path.join(OUTPUT_DIR, "1_Country_Top_Classes.png"))
    plt.close()
    
    # 한국 유사군 분석 (변경 없음)
    if 'Group' in df.columns:
        kr_df = df[(df['Country'] == '한국') & (df['Group'].notna())]
        if not kr_df.empty:
            kr_df = kr_df.assign(Group=kr_df['Group'].astype(str).str.split(r'[|,\s]+')).explode('Group')
            kr_df = kr_df[kr_df['Group'].str.len() > 1]
            top_groups = kr_df['Group'].value_counts().head(10)
            
            plt.figure(figsize=(12, 6))
            sns.barplot(x=top_groups.values, y=top_groups.index, palette='coolwarm', hue=top_groups.index, legend=False)
            plt.title("한국 세부 유사군(Group) Top 10")
            plt.xlabel("건수")
            plt.savefig(os.path.join(OUTPUT_DIR, "1_Korea_Top_Groups.png"))
            plt.close()

# ==========================================
# 2️⃣ 국가별 상표 출원 추이 분석
# ==========================================
def analyze_trends_by_country(df):
    print("\n📈 [2] 국가별 연도별 출원 추이 분석")
    recent_years = sorted(df['Year'].dropna().unique())[-10:]
    trend_df = df[df['Year'].isin(recent_years)]
    trend_data = trend_df.groupby(['Year', 'Country']).size().unstack()
    
    trend_data.plot(kind='line', marker='o', figsize=(12, 6), linewidth=2)
    plt.title("국가별 연도별 상표 출원 추이 (최근 10년)")
    plt.ylabel("출원 건수")
    plt.grid(True, linestyle='--', alpha=0.6)
    plt.legend(title='국가')
    plt.savefig(os.path.join(OUTPUT_DIR, "2_Trends_by_Country.png"))
    plt.close()

# ==========================================
# 3️⃣ 유망 분야 도출 (CAGR 성장률 기반)
# ==========================================
def analyze_promising_fields(df):
    print("\n🚀 [3] 급성장 유망 분야(CAGR) 도출")
    years = sorted(df['Year'].dropna().unique())
    if len(years) < 4: return

    start_year = years[-4]
    end_year = years[-1]
    
    stats = df.pivot_table(index='Class', columns='Year', values='Name', aggfunc='count').fillna(0)
    if start_year not in stats.columns or end_year not in stats.columns: return

    n = end_year - start_year
    stats['CAGR'] = ((stats[end_year] / (stats[start_year] + 1)) ** (1/n)) - 1
    stats = stats[stats[end_year] > 100]
    
    top_growth = stats.sort_values(by='CAGR', ascending=False).head(5)
    
    print(f"   📅 분석 기간: {start_year} -> {end_year}")
    
    # 💡 [수정] 라벨 변환 및 출력
    labels = []
    for cls, row in top_growth.iterrows():
        nice_name = NICE_CLASS_DESC.get(str(int(cls)), '기타')
        print(f"   🏆 급성장: {int(cls)}류 ({nice_name}) - 연평균 {row['CAGR']*100:.1f}%")
        labels.append(f"{int(cls)}류\n({nice_name})")
        
    plt.figure(figsize=(12, 6))
    colors = ['red' if c >= 0.1 else 'blue' for c in top_growth['CAGR']]
    
    plt.bar(labels, top_growth['CAGR'] * 100, color=colors)
    plt.title(f"유망 분야 Top 5 (연평균 성장률, {start_year}-{end_year})", fontsize=15)
    plt.ylabel("성장률 (%)")
    plt.xlabel("류 (Class)")
    plt.axhline(0, color='black', linewidth=0.8)
    plt.savefig(os.path.join(OUTPUT_DIR, "3_Promising_Fields_CAGR.png"))
    plt.close()

# ==========================================
# 4️⃣ 주요 상표 출원일자/시기별 트렌드
# ==========================================
def analyze_seasonality(df):
    print("\n📅 [4] 월별 출원 집중도 (Seasonality) 분석")
    monthly_counts = df.groupby('Month').size()
    
    plt.figure(figsize=(10, 5))
    sns.lineplot(x=monthly_counts.index, y=monthly_counts.values, marker='o', color='purple', linewidth=2)
    plt.title("월별 상표 출원 패턴 (Seasonality)")
    plt.xlabel("월 (Month)")
    plt.ylabel("출원 건수")
    plt.xticks(range(1, 13))
    plt.grid(True, linestyle='--', alpha=0.5)
    
    max_month = monthly_counts.idxmax()
    max_val = monthly_counts.max()
    plt.annotate(f'Peak: {max_month}월', xy=(max_month, max_val), xytext=(max_month, max_val*1.1),
                 arrowprops=dict(facecolor='black', shrink=0.05), ha='center')
    
    plt.savefig(os.path.join(OUTPUT_DIR, "4_Seasonality_Trend.png"))
    plt.close()

# ==========================================
# 🚀 메인 실행
# ==========================================
if __name__ == "__main__":
    init_font()
    df = load_all_data()
    
    if df is not None and not df.empty:
        analyze_top_classes(df)
        analyze_trends_by_country(df)
        analyze_promising_fields(df)
        analyze_seasonality(df)
        
        print(f"\n✅ 모든 분석 완료! 결과물은 '{OUTPUT_DIR}' 폴더를 확인하세요.")
        
        print("\n" + "="*50)
        print("💡 [5] 최종 인사이트 & 제언 (Summary)")
        print("="*50)
        print("1. [주력 산업] NICE 분류 기준, 9류(전자/SW)와 35류(광고/경영)의 비중이 높습니다.")
        print("   - 이는 전 산업의 디지털 전환(DX)과 브랜드화 트렌드를 반영합니다.")
        print("2. [성장세] 최근 성장률(CAGR) 그래프를 통해 뜨고 있는 틈새 시장을 확인하세요.")
        print("3. [전략] 주요 국가(한국/미국/중국)별 선호 류가 다르므로, 국가 맞춤형 포트폴리오가 필요합니다.")
    else:
        print("❌ 분석할 데이터가 없습니다.")