from flask import Flask, render_template, request, redirect, url_for, flash
import pandas as pd
import os
from datetime import datetime, timedelta

import requests
from bs4 import BeautifulSoup
import re  # 正規表現モジュール
import sys
import traceback
from get_match_url_from_schedule_patch import get_match_url_from_schedule

# Initialize the Flask application
app = Flask(__name__)
app.secret_key = 'your_secret_key_here' # Flaskのflashメッセージに必要

# --- Application Configuration ---
DATA_DIR = 'data'
CSV_FILE = os.path.join(DATA_DIR, 'matches.csv')
BACKUP_COUNTER_FILE = os.path.join(DATA_DIR, 'backup_counter.txt')

# CSVファイルの定義を更新
# 打者成績と投手成績の項目を明確に分離
CSV_HEADERS = [
    '日付', 'チーム名', 'ホーム/ビジター', '相手チーム', '得点', '失点', '勝敗', 'URL',
    # 自チームの打撃成績
    '自チーム_打数', '自チーム_安打', '自チーム_本塁打', '自チーム_盗塁', '自チーム_四球', '自チーム_死球', '自チーム_三振',
    # 自チームの投手成績
    '自チーム_被本塁打', '自チーム_与四球', '自チーム_与死球', '自チーム_奪三振', '自チーム_与暴投', '自チーム_与ボーク',
    # 相手チームの打撃成績
    '相手チーム_打数', '相手チーム_安打', '相手チーム_本塁打', '相手チーム_盗塁','試合時間','入場者数' , 'コメント'
]

# Ensure the data directory exists and initialize the CSV file if it's new or empty
if not os.path.exists(DATA_DIR):
    os.makedirs(DATA_DIR)
if not os.path.exists(CSV_FILE) or os.path.getsize(CSV_FILE) == 0:
    pd.DataFrame(columns=CSV_HEADERS).to_csv(CSV_FILE, index=False, encoding='utf-8-sig') # BOM付きUTF-8で保存

# バックアップカウンターの初期化
def initialize_backup_counter():
    """バックアップカウンターファイルを初期化"""
    if not os.path.exists(BACKUP_COUNTER_FILE):
        with open(BACKUP_COUNTER_FILE, 'w', encoding='utf-8') as f:
            f.write('0')

def get_backup_counter():
    """バックアップカウンターの値を取得"""
    try:
        with open(BACKUP_COUNTER_FILE, 'r', encoding='utf-8') as f:
            return int(f.read().strip())
    except (FileNotFoundError, ValueError):
        return 0

def increment_backup_counter():
    """バックアップカウンターを増やし、必要に応じてバックアップを実行"""
    counter = get_backup_counter()
    counter += 1
    
    # カウンターを保存
    with open(BACKUP_COUNTER_FILE, 'w', encoding='utf-8') as f:
        f.write(str(counter))
    
    # 10回に達したらバックアップを実行
    if counter % 10 == 0:
        create_backup()
    
    return counter

def create_backup():
    """matches.csvのバックアップを作成"""
    try:
        from datetime import datetime
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        backup_filename = f'matches_backup_{timestamp}.csv'
        backup_path = os.path.join(DATA_DIR, backup_filename)
        
        # ファイルが存在する場合のみバックアップを作成
        if os.path.exists(CSV_FILE):
            import shutil
            shutil.copy2(CSV_FILE, backup_path)
            print(f"バックアップを作成しました: {backup_filename}")
        else:
            print("matches.csvが見つからないため、バックアップを作成できませんでした。")
    except Exception as e:
        print(f"バックアップ作成中にエラーが発生しました: {e}")

# Mapping for NPB team names: Key is the display name, Value is the NPB website's short name
TEAM_NAME_MAPPING_NPB = {
    '中日ドラゴンズ': '中日',
    '読売ジャイアンツ': '巨人',
    '阪神タイガース': '阪神',
    '広島東洋カープ': '広島',
    '横浜DeNAベイスターズ': 'DeNA',
    '東京ヤクルトスワローズ': 'ヤクルト',
    'オリックス・バファローズ': 'オリックス',
    '福岡ソフトバンクホークス': 'ソフトバンク',
    '千葉ロッテマリーンズ': 'ロッテ',
    '東北楽天ゴールデンイーグルス': '楽天',
    '北海道日本ハムファイターズ': '日本ハム',
    '埼玉西武ライオンズ': '西武',
}

# CSVファイルが存在しない場合はヘッダーを作成
def initialize_csv():
    try:
        pd.read_csv(CSV_FILE)
    except (FileNotFoundError, pd.errors.EmptyDataError):
        df = pd.DataFrame(columns=CSV_HEADERS)
        df.to_csv(CSV_FILE, index=False, encoding='utf-8-sig')

def get_team_full_name(short_name):
    team_names = {
        "中日": "中日ドラゴンズ", "巨人": "読売ジャイアンツ", "ヤクルト": "東京ヤクルトスワローズ",
        "DeNA": "横浜DeNAベイスターズ", "阪神": "阪神タイガース", "広島": "広島東洋カープ",
        "オリックス": "オリックス・バファローズ", "ロッテ": "千葉ロッテマリーンズ", "楽天": "東北楽天ゴールデンイーグルス",
        "ソフトバンク": "福岡ソフトバンクホークス", "日本ハム": "北海道日本ハムファイターズ", "西武": "埼玉西武ライオンズ"
    }
    # 短縮名が見つからない場合は、フルネームが既に入っている可能性がある
    if short_name in team_names.values():
        return short_name
    # 短縮名からフルネームを取得
    return team_names.get(short_name, short_name)


def extract_match_stats(my_bat_tr, opp_pitch_tr, my_pitch_tr, opp_bat_tr):
    """
    各テーブルから成績を抽出し、my_stats辞書として返す。
    """
    my_stats = {}

    # 自分の打撃成績 (自分の打撃テーブルから)
    my_stats['自チーム_打数'] = get_stat_from_tr(my_bat_tr, 3)
    my_stats['自チーム_安打'] = get_stat_from_tr(my_bat_tr, 5)
    my_stats['自チーム_盗塁'] = get_stat_from_tr(my_bat_tr, 7)
    # 自分の打撃成績 (相手の投手テーブルから)
    my_stats['自チーム_本塁打'] = get_stat_from_tr(opp_pitch_tr, 7)
    my_stats['自チーム_四球'] = get_stat_from_tr(opp_pitch_tr, 8)
    my_stats['自チーム_死球'] = get_stat_from_tr(opp_pitch_tr, 9)
    my_stats['自チーム_三振'] = get_stat_from_tr(opp_pitch_tr, 10)

    # 自分の投手成績 (自分の投手テーブルから)
    my_stats['自チーム_奪三振'] = get_stat_from_tr(my_pitch_tr, 10)
    my_stats['自チーム_与暴投'] = get_stat_from_tr(my_pitch_tr, 11)
    my_stats['自チーム_与ボーク'] = get_stat_from_tr(my_pitch_tr, 12)
    # 自分の投手成績 (相手の打撃成績として記録されるもの)
    my_stats['自チーム_被本塁打'] = get_stat_from_tr(my_pitch_tr, 6)
    my_stats['自チーム_与四球'] = get_stat_from_tr(my_pitch_tr, 7)
    my_stats['自チーム_与死球'] = get_stat_from_tr(my_pitch_tr, 8)

    # 相手の成績 (各テーブルから抽出)
    my_stats['相手チーム_打数'] = get_stat_from_tr(opp_bat_tr, 3)
    my_stats['相手チーム_安打'] = get_stat_from_tr(opp_bat_tr, 5)
    my_stats['相手チーム_盗塁'] = get_stat_from_tr(opp_bat_tr, 7)
    my_stats['相手チーム_本塁打'] = my_stats['自チーム_被本塁打']
    my_stats['相手チーム_四球'] = my_stats['自チーム_与四球']
    my_stats['相手チーム_死球'] = my_stats['自チーム_与死球']
    my_stats['相手チーム_三振'] = my_stats['自チーム_奪三振']
    my_stats['相手チーム_被本塁打'] = my_stats['自チーム_本塁打']
    my_stats['相手チーム_与四球'] = my_stats['自チーム_四球']
    my_stats['相手チーム_与死球'] = my_stats['自チーム_死球']

    return my_stats



@app.route('/')
def top():
    """
    TOPページ（通算サマリーなど簡易情報）
    """
    summary = analyze_matches(CSV_FILE)
    return render_template('top.html', summary=summary)


@app.route('/record', methods=['GET', 'POST'])
def record():
    """
    試合記録ページ（フォーム＋記録一覧）
    """
    import pandas as pd
    def save_match_row(row, csv_path=CSV_FILE):
        import pandas as pd
        try:
            df = pd.read_csv(csv_path, encoding='utf-8-sig')
        except (FileNotFoundError, pd.errors.EmptyDataError):
            df = pd.DataFrame(columns=row.keys())
        df = pd.concat([df, pd.DataFrame([row])], ignore_index=True)
        df.to_csv(csv_path, index=False, encoding='utf-8-sig')
        # バックアップカウンターを増やす
        increment_backup_counter()

    if request.method == 'POST':
        team_name = request.form.get('team_name', '').strip()
        # 通常・その他で分岐
        if team_name == 'その他':
            date = request.form.get('target_date', '').strip()
            opp_team = request.form.get('opp_team', '').strip()
            home_away = request.form.get('home_away', '').strip()
            score = request.form.get('score', '').strip()
            lost = request.form.get('lost', '').strip()
            result = request.form.get('result', '').strip()
            comment = request.form.get('comment', '').strip()
            row = {
                '日付': date,
                'チーム名': team_name,
                'ホーム/ビジター': home_away,
                '相手チーム': opp_team,
                '得点': score,
                '失点': lost,
                '勝敗': result,
                'URL': '',
                # 成績系は空欄
                '自チーム_打数': '', '自チーム_安打': '', '自チーム_本塁打': '', '自チーム_盗塁': '', '自チーム_四球': '', '自チーム_死球': '', '自チーム_三振': '',
                '自チーム_被本塁打': '', '自チーム_与四球': '', '自チーム_与死球': '', '自チーム_奪三振': '', '自チーム_与暴投': '', '自チーム_与ボーク': '',
                '相手チーム_打数': '', '相手チーム_安打': '', '相手チーム_本塁打': '', '相手チーム_盗塁': '',
                '試合時間':'', '入場者数':'',
                'コメント': comment
            }
            save_match_row(row)
            return redirect(url_for('top'))
        else:
            if request.method == 'POST':
                target_date_str = request.form['target_date']
                team_name_input = request.form['team_name']
                comment = request.form.get('comment', '')

                match_url, home_away_status = get_match_url_from_schedule(target_date_str, team_name_input)

        if match_url and home_away_status:
            success, message = scrape_and_record_match_from_url(match_url, team_name_input, home_away_status, comment)
            if success:
                flash(message, 'success')
            else:
                flash(message, 'error')
        else:
            flash(f"'{team_name_input}' の試合が {target_date_str} の日程ページで見つかりませんでした。", 'error')
            
        return redirect(url_for('top'))
    teams = [
        "中日ドラゴンズ", "読売ジャイアンツ", "阪神タイガース", "広島東洋カープ",
        "横浜DeNAベイスターズ", "東京ヤクルトスワローズ",
        "オリックス・バファローズ", "福岡ソフトバンクホークス", "千葉ロッテマリーンズ",
        "東北楽天ゴールデンイーグルス", "北海道日本ハムファイターズ", "埼玉西武ライオンズ"
    ]
    return render_template('record.html', teams=teams, today=datetime.today().strftime('%Y-%m-%d'))

@app.route('/summary')
def summary():
    """
    通算成績ページ（試合数・勝敗・対戦チームごとの成績・全試合詳細）
    """
    matches_path = os.path.join(DATA_DIR, 'matches.csv')
    try:
        df = pd.read_csv(matches_path, encoding='utf-8-sig')
    except Exception:
        df = pd.DataFrame()

    # --- 基本集計 ---
    valid_df = df[df['勝敗'].notnull() & (df['勝敗'] != '')]
    # コメント列のNaNを空文字に
    if 'コメント' in df.columns:
        df['コメント'] = df['コメント'].fillna('')
    # 日付順に並べる（昇順）
    if '日付' in df.columns:
        df = df.copy()
        df['日付'] = pd.to_datetime(df['日付'], errors='coerce')
        df = df.sort_values('日付')
    total_games = len(valid_df)
    win_count = (valid_df['勝敗'] == '勝').sum()
    lose_count = (valid_df['勝敗'] == '敗').sum()
    draw_count = (valid_df['勝敗'] == '引分').sum()
    denominator = win_count + lose_count
    win_rate = round(win_count / denominator, 3) if denominator > 0 else 0
    # 累積勝敗リスト生成（全件・空欄0扱い）
    result_map = {'勝': 1, '敗': -1, '引分': 0}
    if '勝敗' in df.columns and len(df) > 0:
        result_values = df['勝敗'].map(result_map).fillna(0).tolist()
        cumulative_results = [0]
        for v in result_values:
            cumulative_results.append(cumulative_results[-1] + v)
    else:
        cumulative_results = []

    # --- 対戦チームごとの試合数・勝率 ---
    vs_team_stats = []
    if not valid_df.empty:
        grouped = valid_df.groupby('相手チーム')
        for team, group in grouped:
            games = len(group)
            win = (group['勝敗'] == '勝').sum()
            lose = (group['勝敗'] == '敗').sum()
            draw = (group['勝敗'] == '引分').sum()
            denominator = win + lose
            rate = round(win / denominator, 3) if denominator > 0 else 0
            vs_team_stats.append({
                'team': team,
                'games': games,
                'win': win,
                'lose': lose,
                'draw': draw,
                'win_rate': rate
            })
        vs_team_stats = sorted(vs_team_stats, key=lambda x: x['games'], reverse=True)

    # --- カテゴリ別合計・平均テーブル用データ ---
    # 打撃成績カテゴリ
    batting_columns = [
        '自チーム_打数', '自チーム_安打', '自チーム_本塁打', '自チーム_盗塁',
        '自チーム_四球', '自チーム_死球', '自チーム_三振'
    ]
    
    # 投手成績カテゴリ
    pitching_columns = [
        '自チーム_被本塁打', '自チーム_与四球', '自チーム_与死球', '自チーム_奪三振', 
        '自チーム_与暴投', '自チーム_与ボーク'
    ]
    
    # 相手チーム成績カテゴリ
    opponent_columns = [
        '相手チーム_打数', '相手チーム_安打', '相手チーム_本塁打', '相手チーム_盗塁'
    ]
    
    # 基本成績カテゴリ
    basic_columns = ['得点', '失点']
    
    # 「その他」チームを除外したデータフレームを作成
    filtered_df = valid_df.copy()
    if 'チーム名' in filtered_df.columns:
        filtered_df = filtered_df[~filtered_df['チーム名'].str.contains('その他', na=False)]
    
    # カテゴリ別の辞書を作成
    batting_sum = {}
    batting_avg = {}
    pitching_sum = {}
    pitching_avg = {}
    opponent_sum = {}
    opponent_avg = {}
    basic_sum = {}
    basic_avg = {}
    
    # 打撃成績の集計
    for col in batting_columns:
        if col in filtered_df.columns:
            vals = pd.to_numeric(filtered_df[col], errors='coerce').fillna(0)
            batting_sum[col.replace('自チーム_', '')] = int(vals.sum())
            batting_avg[col.replace('自チーム_', '')] = round(vals.mean(), 2) if len(vals) > 0 else 0
        else:
            batting_sum[col.replace('自チーム_', '')] = '-'
            batting_avg[col.replace('自チーム_', '')] = '-'
    
    # 投手成績の集計
    for col in pitching_columns:
        if col in filtered_df.columns:
            vals = pd.to_numeric(filtered_df[col], errors='coerce').fillna(0)
            pitching_sum[col.replace('自チーム_', '')] = int(vals.sum())
            pitching_avg[col.replace('自チーム_', '')] = round(vals.mean(), 2) if len(vals) > 0 else 0
        else:
            pitching_sum[col.replace('自チーム_', '')] = '-'
            pitching_avg[col.replace('自チーム_', '')] = '-'
    
    # 相手チーム成績の集計
    for col in opponent_columns:
        if col in filtered_df.columns:
            vals = pd.to_numeric(filtered_df[col], errors='coerce').fillna(0)
            opponent_sum[col.replace('相手チーム_', '')] = int(vals.sum())
            opponent_avg[col.replace('相手チーム_', '')] = round(vals.mean(), 2) if len(vals) > 0 else 0
        else:
            opponent_sum[col.replace('相手チーム_', '')] = '-'
            opponent_avg[col.replace('相手チーム_', '')] = '-'
    
    # 基本成績の集計
    for col in basic_columns:
        if col in filtered_df.columns:
            vals = pd.to_numeric(filtered_df[col], errors='coerce').fillna(0)
            basic_sum[col] = int(vals.sum())
            basic_avg[col] = round(vals.mean(), 2) if len(vals) > 0 else 0
        else:
            basic_sum[col] = '-'
            basic_avg[col] = '-'
    
    # 後方互換性のため、元のsum_dictとavg_dictも保持
    all_columns = batting_columns + pitching_columns + opponent_columns + basic_columns
    sum_dict = {}
    avg_dict = {}
    for col in all_columns:
        if col in filtered_df.columns:
            vals = pd.to_numeric(filtered_df[col], errors='coerce').fillna(0)
            sum_dict[col] = int(vals.sum())
            avg_dict[col] = round(vals.mean(), 2) if len(vals) > 0 else 0
        else:
            sum_dict[col] = '-'
            avg_dict[col] = '-'

    # 試合時間（平均・合計/分換算）
    avg_time = '-'
    sum_time = '-'
    if '試合時間' in df.columns and not df['試合時間'].isnull().all():
        import re
        times = []
        for t in df['試合時間'].dropna():
            m = re.match(r"(\d+)[^\d]?(\d+)?", str(t))
            if m:
                h = int(m.group(1))
                mi = int(m.group(2)) if m.group(2) else 0
                times.append(h*60+mi)
        if times:
            avg_time = f"{int(sum(times)/len(times)//60)}時間{int(sum(times)/len(times)%60)}分"
            total_min = sum(times)
            sum_time = f"{total_min//60}時間{total_min%60}分"

    # 入場者数（平均・合計/数値化）
    avg_att = '-'
    sum_att = '-'
    print(f"[DEBUG] 入場者数列の存在: {'入場者数' in df.columns}")
    if '入場者数' in df.columns:
        print(f"[DEBUG] 入場者数データ: {df['入場者数'].tolist()}")
        print(f"[DEBUG] 入場者数が全てnull: {df['入場者数'].isnull().all()}")
        if not df['入場者数'].isnull().all():
            nums = []
            for a in df['入場者数'].dropna():
                a = str(a).replace('人','').replace(',','').strip()
                print(f"[DEBUG] 処理中の入場者数: '{a}'")
                try:
                    # 浮動小数点数を整数に変換
                    nums.append(int(float(a)))
                except:
                    print(f"[DEBUG] 数値変換失敗: '{a}'")
                    continue
            print(f"[DEBUG] 有効な入場者数: {nums}")
            if nums:
                avg_att = int(sum(nums)/len(nums))
                sum_att = sum(nums)
                print(f"[DEBUG] 平均入場者数: {avg_att}, 合計入場者数: {sum_att}")
    
    # デバッグ情報を削除して、最終的な値を確認
    print(f"[DEBUG] 最終的なavg_att: {avg_att}")
    print(f"[DEBUG] 最終的なsum_att: {sum_att}")

    # --- 全試合詳細 ---
    all_matches = df.to_dict(orient='records') if not df.empty else []
    columns = list(df.columns) if not df.empty else []

    # --- 通算成績指標（「その他」チームを除外した計算） ---
    def safe_div(a, b):
        try:
            return a / b if b else 0
        except:
            return 0
    # 打率
    total_hits = sum_dict.get('自チーム_安打', 0)
    total_at_bats = sum_dict.get('自チーム_打数', 0)
    avg_batting = round(safe_div(total_hits, total_at_bats), 3) if total_at_bats else '-'
    # 本塁打率
    total_hr = sum_dict.get('自チーム_本塁打', 0)
    hr_rate_val = safe_div(total_hr, total_at_bats) if total_at_bats else None
    hr_rate = f"{round(hr_rate_val*100,2)}%" if hr_rate_val is not None else '-'
    # OPS計算
    total_bb = sum_dict.get('自チーム_四球', 0)
    total_hbp = sum_dict.get('自チーム_死球', 0)
    total_sf = 0  # 犠飛データがない場合は0として計算
    # 出塁率 = (安打 + 四球 + 死球) / (打数 + 四球 + 死球 + 犠飛)
    obp_den = total_at_bats + total_bb + total_hbp + total_sf
    obp = safe_div(total_hits + total_bb + total_hbp, obp_den) if obp_den > 0 else 0
    # 長打率 = (安打 + 本塁打*3) / 打数 (2塁打・3塁打データがないため近似)
    slg = safe_div(total_hits + total_hr * 3, total_at_bats) if total_at_bats > 0 else 0
    # OPS = 出塁率 + 長打率
    ops = round(obp + slg, 3) if total_at_bats > 0 else '-'
    # 防御率
    total_runs = sum_dict.get('失点', 0)
    innings = total_games * 9
    era = round(safe_div(total_runs * 9, innings), 2) if innings else '-'
    # 被打率
    opp_hits = sum_dict.get('相手チーム_安打', 0)
    opp_at_bats = sum_dict.get('相手チーム_打数', 0)
    opp_avg = round(safe_div(opp_hits, opp_at_bats), 3) if opp_at_bats else '-'
    # 被本塁打率
    opp_hr = sum_dict.get('相手チーム_本塁打', 0)
    opp_hr_rate_val = safe_div(opp_hr, opp_at_bats) if opp_at_bats else None
    opp_hr_rate = f"{round(opp_hr_rate_val*100,2)}%" if opp_hr_rate_val is not None else '-'

    # all_matchesの日付を必ず文字列化
    for m in all_matches:
        if '日付' in m and m['日付'] is not None:
            if hasattr(m['日付'], 'strftime'):
                m['日付'] = m['日付'].strftime('%Y-%m-%d')
            else:
                m['日付'] = str(m['日付'])[:10]

    # --- 年度ごとの試合数・勝率集計 ---
    yearly_stats = []
    if not valid_df.empty and '日付' in valid_df.columns:
        # 日付から年度を抽出
        valid_df_copy = valid_df.copy()
        valid_df_copy['年度'] = pd.to_datetime(valid_df_copy['日付'], errors='coerce').dt.year
        
        # 年度ごとにグループ化して集計
        yearly_grouped = valid_df_copy.groupby('年度')
        for year, group in yearly_grouped:
            if pd.isna(year):  # 年度が取得できない場合はスキップ
                continue
            
            year_games = len(group)
            year_win = (group['勝敗'] == '勝').sum()
            year_lose = (group['勝敗'] == '敗').sum()
            year_draw = (group['勝敗'] == '引分').sum()
            year_denominator = year_win + year_lose
            year_win_rate = round(year_win / year_denominator, 3) if year_denominator > 0 else 0
            
            # numpy型をPython標準型に変換
            yearly_stats.append({
                'year': int(year),
                'games': int(year_games),
                'win': int(year_win.item() if hasattr(year_win, 'item') else year_win),
                'lose': int(year_lose.item() if hasattr(year_lose, 'item') else year_lose),
                'draw': int(year_draw.item() if hasattr(year_draw, 'item') else year_draw),
                'win_rate': float(year_win_rate)
            })
        
        # 年度順にソート（新しい年度が上に）
        yearly_stats = sorted(yearly_stats, key=lambda x: x['year'], reverse=True)

    return render_template('summary.html',
        total_games=total_games,
        win_count=win_count,
        lose_count=lose_count,
        draw_count=draw_count,
        win_rate=win_rate,
        vs_team_stats=vs_team_stats,
        sum_dict=sum_dict,
        avg_dict=avg_dict,
        batting_sum=batting_sum,
        batting_avg=batting_avg,
        pitching_sum=pitching_sum,
        pitching_avg=pitching_avg,
        opponent_sum=opponent_sum,
        opponent_avg=opponent_avg,
        basic_sum=basic_sum,
        basic_avg=basic_avg,
        avg_time=avg_time,
        sum_time=sum_time,
        avg_att=avg_att,
        sum_att=sum_att,
        matches=all_matches,
        columns=columns,
        avg_batting=avg_batting,
        hr_rate=hr_rate,
        ops=ops,
        era=era,
        opp_avg=opp_avg,
        opp_hr_rate=opp_hr_rate,
        cumulative_results=cumulative_results,
        yearly_stats=yearly_stats
    )

@app.route('/edit_match/<int:row_id>', methods=['GET', 'POST'])
def edit_match(row_id):
    df = pd.read_csv(CSV_FILE, encoding='utf-8-sig')
    if row_id < 0 or row_id >= len(df):
        flash('該当する試合データがありません', 'danger')
        return redirect(url_for('summary'))
    if request.method == 'POST':
        new_comment = request.form.get('comment', '').strip()
        df.at[row_id, 'コメント'] = new_comment
        df.to_csv(CSV_FILE, index=False, encoding='utf-8-sig')
        flash('コメントを更新しました', 'success')
        return redirect(url_for('summary'))
    comment = df.at[row_id, 'コメント'] if 'コメント' in df.columns else ''
    return render_template('edit_match.html', comment=comment)

@app.route('/edit_match_by_date', methods=['GET', 'POST'])
def edit_match_by_date():
    """
    日付とチーム名で試合を特定してコメントを編集
    """
    if request.method == 'GET':
        # GETリクエストの場合は、URLパラメータから取得
        date = request.args.get('date')
        team = request.args.get('team')
    else:
        # POSTリクエストの場合は、フォームデータから取得
        date = request.form.get('date')
        team = request.form.get('team')
    
    if not date or not team:
        flash('日付とチーム名が必要です', 'danger')
        return redirect(url_for('summary'))
    
    df = pd.read_csv(CSV_FILE, encoding='utf-8-sig')
    
    # 日付とチーム名で試合を特定
    mask = (df['日付'] == date) & (df['チーム名'] == team)
    matching_rows = df[mask]
    
    if len(matching_rows) == 0:
        flash('該当する試合データがありません', 'danger')
        return redirect(url_for('summary'))
    
    # 最初のマッチする行を取得
    row_index = matching_rows.index[0]
    
    if request.method == 'POST':
        new_comment = request.form.get('comment', '').strip()
        df.at[row_index, 'コメント'] = new_comment
        df.to_csv(CSV_FILE, index=False, encoding='utf-8-sig')
        flash('コメントを更新しました', 'success')
        return redirect(url_for('summary'))
    
    comment = df.at[row_index, 'コメント'] if 'コメント' in df.columns else ''
    return render_template('edit_match.html', comment=comment, date=date, team=team)

@app.route('/players')
def players():
    """
    選手通算成績ページ（打者・投手成績）
    """
    import pandas as pd
    batters_df = pd.read_csv('data/batters_stats.csv', encoding='utf-8-sig')
    pitchers_df = pd.read_csv('data/pitchers_stats.csv', encoding='utf-8-sig')

    # 打者成績の計算
    batters_stats = []
    for row in batters_df.to_dict(orient='records'):
        try:
            ab = int(row.get('打数', 0))
            h = int(row.get('安打', 0))
            hr = int(row.get('本塁打', 0))
            bb = int(row.get('四球', 0))
            hbp = int(row.get('死球', 0))
            sf = int(row.get('犠飛', 0)) if '犠飛' in row else 0
            tb = h + hr * 3  # 2塁打・3塁打データがなければ近似
            # 打率
            row['打率'] = round(h / ab, 3) if ab > 0 else '-'
            # 出塁率
            obp_den = ab + bb + hbp + sf
            row['出塁率'] = round((h + bb + hbp) / obp_den, 3) if obp_den > 0 else '-'
            # OPS
            slg = round(tb / ab, 3) if ab > 0 else 0
            row['OPS'] = round((row['出塁率'] if isinstance(row['出塁率'], float) else 0) + slg, 3) if ab > 0 else '-'
        except Exception:
            row['打率'] = row['出塁率'] = row['OPS'] = '-'
        batters_stats.append(row)

    # 投手成績の計算
    pitchers_stats = []
    for row in pitchers_df.to_dict(orient='records'):
        try:
            ip_raw = str(row.get('投球回', '0'))
            # 投球回が x.y 形式（例: 12.2 = 12回2/3）
            if '.' in ip_raw:
                ip_main, ip_frac = ip_raw.split('.')
                ip = int(ip_main) + int(ip_frac) / 3
            else:
                ip = float(ip_raw)
            runs = int(row.get('失点', 0))
            so = int(row.get('奪三振', 0))
            # 防御率
            row['防御率'] = round(9 * runs / ip, 2) if ip > 0 else '-'
            # 奪三振率
            row['奪三振率'] = round(9 * so / ip, 2) if ip > 0 else '-'
        except Exception:
            row['防御率'] = row['奪三振率'] = '-'
        pitchers_stats.append(row)

    return render_template('players.html', batters_stats=batters_stats, pitchers_stats=pitchers_stats)



@app.route('/about')
def about():
    """
    その他サブページ
    """
    return render_template('about.html')

# 既存のindex, results, record_specific_match等のルートは一旦残す（リファクタ時に統合・整理）


@app.route('/record_manual', methods=['POST'])
def record_manual_match():
    """
    Handles manual match recording from the form submission.
    """
    date_str = request.form['date']
    home_team_input = request.form['home_team']
    away_team_input = request.form['away_team']
    home_score = request.form['home_score']
    away_score = request.form['away_score']

    try:
        home_score = int(home_score)
        away_score = int(away_score)
    except ValueError:
        flash("スコアは半角数字で入力してください。", 'error')
        return redirect(url_for('top'))

    win_loss_manual = '引分'
    if home_score > away_score:
        win_loss_manual = '勝'
    elif home_score < away_score:
        win_loss_manual = '敗'

    default_stats = {col: 0 for col in CSV_HEADERS if col not in ['日付', 'チーム名', 'ホーム/ビジター', '相手チーム', '得点', '失点', '勝敗', 'URL']}

    match_data_home_team = {
        '日付': date_str, 'チーム名': home_team_input, 'ホーム/ビジター': 'ホーム',
        '相手チーム': away_team_input, '得点': home_score, '失点': away_score,
        '勝敗': win_loss_manual, 'URL': '手動入力', **default_stats
    }

    win_loss_away_team = '引分'
    if win_loss_manual == '勝': win_loss_away_team = '敗'
    elif win_loss_manual == '敗': win_loss_away_team = '勝'

    match_data_away_team = {
        '日付': date_str, 'チーム名': away_team_input, 'ホーム/ビジター': 'ビジター',
        '相手チーム': home_team_input, '得点': away_score, '失点': home_score,
        '勝敗': win_loss_away_team, 'URL': '手動入力', **default_stats
    }

    new_data_df = pd.DataFrame([match_data_home_team, match_data_away_team])

    try:
        existing_df = pd.read_csv(CSV_FILE, encoding='utf-8-sig')
        # 既存データと結合する前に、重複する可能性のある古いデータを削除
        existing_df = existing_df[~((existing_df['日付'] == date_str) & (existing_df['チーム名'].isin([home_team_input, away_team_input])))]
        updated_df = pd.concat([existing_df, new_data_df], ignore_index=True)
        updated_df.to_csv(CSV_FILE, index=False, encoding='utf-8-sig')
        # バックアップカウンターを増やす
        increment_backup_counter()
        flash("試合結果を手動で記録しました！", 'success')
    except (FileNotFoundError, pd.errors.EmptyDataError):
        new_data_df.to_csv(CSV_FILE, index=False, encoding='utf-8-sig')
        # バックアップカウンターを増やす
        increment_backup_counter()
        flash("試合結果を手動で記録しました！", 'success')
    except Exception as e:
        flash(f"手動記録中にエラーが発生しました: {e}", 'error')
    
    return redirect(url_for('top'))


def scrape_and_record_match_from_url(match_url, selected_team_full_name, home_away_status, comment=None):
    """
    指定されたURLから試合データをスクレイピングし、CSVに記録する。
    ホーム/ビジターとdivのIDに基づく新しいロジックで実装。
    """
    import os
    import pandas as pd
    import requests
    from bs4 import BeautifulSoup
    import re
    from datetime import datetime

    # CSV保存先
    csv_path = CSV_FILE if 'CSV_FILE' in globals() else os.path.join(os.path.dirname(__file__), 'data', 'matches.csv')

    # URL補正
    headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'
    }
    full_url = "https://npb.jp" + match_url if match_url.startswith("/") else match_url
    response = requests.get(full_url,headers=headers)
    response.raise_for_status()
    response.encoding = 'utf-8'
    soup = BeautifulSoup(response.text, 'html.parser')

    # 試合日付取得
    game_tit_div = soup.find('div', class_='game_tit')
    if not game_tit_div or not game_tit_div.find('time'):
        return False, "試合タイトル部または日付が取得できませんでした。"
    game_date_elem = game_tit_div.find('time')
    date_match = re.search(r'(\d{4})年(\d{1,2})月(\d{1,2})日', game_date_elem.text)
    if not date_match:
        return False, "試合日付が取得できませんでした。"
    match_date = datetime(int(date_match.group(1)), int(date_match.group(2)), int(date_match.group(3))).strftime('%Y-%m-%d')
    game_info_p = soup.find('p', class_='game_info')
    info_text = game_info_p.get_text(strip=True) if game_info_p else ""
    # 試合時間は「5時間13分」→「5:13」の形で抽出・変換
    m = re.search(r'試合時間\s*([0-9]{1,2})時間([0-9]{1,2})分', info_text)
    if m:
        match_time = f"{int(m.group(1))}:{m.group(2).zfill(2)}"
    else:
        match_time = ""
    # 入場者数は数字のみ抽出（例: 36,292 → 36292）
    m = re.search(r'入場者\s*([0-9,]+)', info_text)
    attendance = m.group(1).replace(",", "") if m else ""
    # スコア取得
    linescore = soup.find('table', id='tablefix_ls')
    if not linescore:
        return False, "スコアテーブルが見つかりません。"
    away_row = linescore.find('tr', class_='top')
    home_row = linescore.find('tr', class_='bottom')
    if not away_row or not home_row:
        return False, "スコア行が見つかりません。"
    
    # デバッグ情報を追加
    print(f"[DEBUG] away_row HTML: {away_row}")
    print(f"[DEBUG] home_row HTML: {home_row}")
    
    # より詳細なデバッグ情報
    print(f"[DEBUG] away_row.find('span'): {away_row.find('span')}")
    print(f"[DEBUG] home_row.find('span'): {home_row.find('span')}")
    print(f"[DEBUG] away_row.find('th'): {away_row.find('th')}")
    print(f"[DEBUG] home_row.find('th'): {home_row.find('th')}")

    # チーム名取得をより堅牢に
    def extract_team_name(row):
        """複数の方法でチーム名を抽出"""
        # 方法1: span要素（クラス名に関係なく）
        team_span = row.find('span')
        if team_span:
            return team_span.text.strip()
        
        # 方法2: th要素内のspan
        team_th = row.find('th')
        if team_th:
            span_in_th = team_th.find('span')
            if span_in_th:
                return span_in_th.text.strip()
            else:
                return team_th.text.strip()
        
        # 方法3: 最初のtdから取得
        first_td = row.find('td')
        if first_td:
            return first_td.get_text(strip=True)
        
        # 方法4: 行全体から数字以外の部分を抽出
        row_text = row.get_text(strip=True)
        import re
        match = re.search(r'^([^\d]+)', row_text)
        if match:
            return match.group(1).strip()
        
        return "不明"
    
    away_team_text = extract_team_name(away_row)
    home_team_text = extract_team_name(home_row)
    
    print(f"[DEBUG] away_team_text: {away_team_text}")
    print(f"[DEBUG] home_team_text: {home_team_text}")
    
    away_team_full_name = get_team_full_name(away_team_text)
    home_team_full_name = get_team_full_name(home_team_text)

    def safe_int(text):
        try:
            return int(text.strip())
        except (ValueError, AttributeError):
            return 0

    away_score = safe_int(away_row.find('td', class_='total-1').text)
    home_score = safe_int(home_row.find('td', class_='total-1').text)

    # 成績抽出用関数
    def get_th_stats(div_id, indices):
        div = soup.find('div', id=div_id)
        if not div or not div.find('tfoot') or not div.find('tfoot').find('tr'):
            return [None]*len(indices)
        ths = div.find('tfoot').find('tr').find_all('th')
        return [ths[i].text.strip() if len(ths) > i else None for i in indices]

    # 記録用辞書
    stats = {
        '自チーム_打数': '',
        '自チーム_安打': '',
        '自チーム_本塁打': '',
        '自チーム_盗塁': '',
        '自チーム_四球': '',
        '自チーム_死球': '',
        '自チーム_三振': '',
        '自チーム_被本塁打': '',
        '自チーム_与四球': '',
        '自チーム_与死球': '',
        '自チーム_奪三振': '',
        '自チーム_与暴投': '',
        '自チーム_与ボーク': '',
        '相手チーム_打数': '',
        '相手チーム_安打': '',
        '相手チーム_本塁打': '',
        '相手チーム_盗塁': '',
        '相手チーム_四球': '',
        '相手チーム_死球': '',
        '相手チーム_三振': '',
        '相手チーム_被本塁打': '',
        '相手チーム_与四球': '',
        '相手チーム_与死球': '',
        '相手チーム_奪三振': '',
        '相手チーム_与暴投': '',
        '相手チーム_与ボーク': ''
    }

    # ホーム/ビジターで探索先を切り替え
    if home_away_status == 'ホーム':
        # 自チーム打撃
        my_bat = get_th_stats('table_bottom_b', [3, 5, 7])  # 4,6,8番目
        stats['自チーム_打数'], stats['自チーム_安打'], stats['自チーム_盗塁'] = my_bat
        # 相手投手
        opp_pitch = get_th_stats('table_bottom_p', [7, 8, 9, 10, 11, 12])  # 8,9,10,11,12,13番目
        print("[DEBUG] opp_pitch:", opp_pitch)
        stats['相手チーム_本塁打'], stats['自チーム_与四球'], stats['自チーム_与死球'], stats['自チーム_奪三振'], stats['自チーム_与暴投'], stats['自チーム_与ボーク'] = opp_pitch
        # 相手打撃
        opp_bat = get_th_stats('table_top_b', [3, 5, 7])  # 4,6,8番目
        stats['相手チーム_打数'], stats['相手チーム_安打'], stats['相手チーム_盗塁'] = opp_bat
        # 自チーム投手
        my_pitch = get_th_stats('table_top_p', [7, 8, 9, 10, 11, 12])  # 8,9,10,11,12,13番目
        print("[DEBUG] my_pitch:", my_pitch)
        stats['自チーム_本塁打'], stats['自チーム_四球'], stats['自チーム_死球'], stats['自チーム_三振'], stats['自チーム_被本塁打'], stats['相手チーム_奪三振'] = my_pitch[:6]  # 必要に応じてindex調整
    else:
        # 自チーム打撃
        my_bat = get_th_stats('table_top_b', [3, 5, 7])  # 4,6,8番目
        stats['自チーム_打数'], stats['自チーム_安打'], stats['自チーム_盗塁'] = my_bat
        # 相手投手
        opp_pitch = get_th_stats('table_top_p', [7, 8, 9, 10, 11, 12])  # 8,9,10,11,12,13番目
        print("[DEBUG] opp_pitch:", opp_pitch)
        stats['相手チーム_本塁打'], stats['自チーム_与四球'], stats['自チーム_与死球'], stats['自チーム_奪三振'], stats['自チーム_与暴投'], stats['自チーム_与ボーク'] = opp_pitch
        # 相手打撃
        opp_bat = get_th_stats('table_bottom_b', [3, 5, 7])  # 4,6,8番目
        stats['相手チーム_打数'], stats['相手チーム_安打'], stats['相手チーム_盗塁'] = opp_bat
        # 自チーム投手
        my_pitch = get_th_stats('table_bottom_p', [7, 8, 9, 10, 11, 12])  # 8,9,10,11,12,13番目
        print("[DEBUG] my_pitch:", my_pitch)
        stats['自チーム_本塁打'], stats['自チーム_四球'], stats['自チーム_死球'], stats['自チーム_三振'], stats['自チーム_被本塁打'], stats['相手チーム_奪三振'] = my_pitch[:6]

    # スコア・勝敗
    if home_away_status == 'ホーム':
        my_score, opp_score, opp_name = home_score, away_score, away_team_full_name
    else:
        my_score, opp_score, opp_name = away_score, home_score, home_team_full_name

    win_loss = "引分"
    if my_score > opp_score: win_loss = "勝"
    elif my_score < opp_score: win_loss = "敗"

    # CSV出力
    my_team_row = {
        '日付': match_date, 'チーム名': selected_team_full_name, 'ホーム/ビジター': home_away_status,
        '相手チーム': opp_name, '得点': my_score, '失点': opp_score,
        '勝敗': win_loss, 'URL': full_url, **stats, '試合時間': match_time, '入場者数': attendance,
        'コメント': comment if comment is not None else ''
    }

    # --- 個別選手成績も保存 ---
    try:
        batters, pitchers = scrape_player_stats_from_box(match_url, home_away_status)
        if batters:
            update_batter_stats(batters, selected_team_full_name)
        if pitchers:
            update_pitcher_stats(pitchers, selected_team_full_name)
    except Exception as e:
        print(f"[ERROR] 選手成績保存時にエラー: {e}")


    try:
        df = pd.read_csv(csv_path, encoding='utf-8-sig')
        df = df[~((df['日付'] == match_date) & (df['チーム名'] == selected_team_full_name))]
        df = pd.concat([df, pd.DataFrame([my_team_row])], ignore_index=True)
    except (FileNotFoundError, pd.errors.EmptyDataError, KeyError):
        df = pd.DataFrame([my_team_row])
    for col in CSV_HEADERS:
        if col not in df.columns:
            df[col] = ''
    df = df[CSV_HEADERS]
    df.to_csv(csv_path, index=False, encoding='utf-8-sig')
    
    # バックアップカウンターを増やす
    increment_backup_counter()
    
    return True, f"{match_date} の {selected_team_full_name} vs {opp_name} の試合結果を記録しました。"

@app.route('/record_specific_match', methods=['POST'])
def record_specific_match():
    if request.method == 'POST':
        target_date_str = request.form['target_date']
        team_name_input = request.form['team_name']
        comment = request.form.get('comment', '')

        match_url, home_away_status = get_match_url_from_schedule(target_date_str, team_name_input)

        if match_url and home_away_status:
            success, message = scrape_and_record_match_from_url(match_url, team_name_input, home_away_status, comment)
            if success:
                flash(message, 'success')
            else:
                flash(message, 'error')
        else:
            flash(f"'{team_name_input}' の試合が {target_date_str} の日程ページで見つかりませんでした。", 'error')
            
    return redirect(url_for('top'))

@app.route('/results')
def results():
    try:
        df = pd.read_csv(CSV_FILE, encoding='utf-8-sig')
        if not df.empty and '日付' in df.columns:
            df['日付'] = pd.to_datetime(df['日付'], errors='coerce')
            df = df.dropna(subset=['日付'])
            df = df.sort_values(by='日付', ascending=False)
            df['日付'] = df['日付'].dt.strftime('%Y-%m-%d')
    except (FileNotFoundError, pd.errors.EmptyDataError):
        df = pd.DataFrame(columns=CSV_HEADERS) # CSVがないか空の場合は空のDataFrameを作成
    # 表示するカラムを選択（必要に応じて調整してください）
    display_columns = [
     '日付', 'チーム名', 'ホーム/ビジター', '相手チーム', '得点', '失点', '勝敗', 'URL',
    # 自チームの打撃成績
    '自チーム_打数', '自チーム_安打', '自チーム_本塁打', '自チーム_盗塁', '自チーム_四球', '自チーム_死球', '自チーム_三振',
    # 自チームの投手成績
    '自チーム_被本塁打', '自チーム_与四球', '自チーム_与死球', '自チーム_奪三振', '自チーム_与暴投', '自チーム_与ボーク',
    # 相手チームの打撃成績
    '相手チーム_打数', '相手チーム_安打', '相手チーム_本塁打', '相手チーム_盗塁', '試合時間','入場者数' , 'コメント'
    ]
    
    # 実際のDataFrameに存在するカラムのみを選択
    available_display_columns = [col for col in display_columns if col in df.columns]
    
    # 集計サマリーを取得
    summary = analyze_matches(CSV_FILE)
    return render_template('results.html', 
                           matches=df[available_display_columns].to_dict(orient='records'),
                           columns=available_display_columns,
                           summary=summary)

# --- 選手成績スクレイピング ---
def scrape_player_stats_from_box(box_url, home_away_status):
    """
    指定チームのbox.htmlから打者・投手ごとの成績をリストで抽出する。
    戻り値: (batters, pitchers)
    batters: [{'選手名': str, '打数': int, '安打': int, '打点': int, '盗塁': int, '本塁打': int, '三振': int} ...]
    pitchers: [{'選手名': str, '投球回': int, '打者数': int, '被安打': int, '奪三振': int, '被本塁打': int, ...} ...]
    """
    import requests
    from bs4 import BeautifulSoup
    import re
    from urllib.parse import urljoin
    batters, pitchers = [], []
    # 相対パスなら絶対URL化
    box_url = urljoin('https://npb.jp', box_url)
    try:
        res = requests.get(box_url)
        res.encoding = 'utf-8'
        soup = BeautifulSoup(res.text, 'html.parser')
        # 打者成績
        if home_away_status == 'ホーム':
            bat_div = soup.find('div', id='table_bottom_b')
            bat_table = bat_div.find('table', id='tablefix_b_b') if bat_div else None
        else:
            bat_div = soup.find('div', id='table_top_b')
            bat_table = bat_div.find('table', id='tablefix_t_b') if bat_div else None
        if bat_table and bat_table.find('tbody'):
            for tr in bat_table.find('tbody').find_all('tr'):
                tds = tr.find_all('td', recursive=False)
                if not tds or len(tds) < 9:
                    continue
                player_td = tr.find('td', class_='player')
                if not player_td:
                    continue
                player_name = player_td.text.strip()
                # 必要なtd数が揃っているか
                batter = {
                    '選手名': player_name,
                    '打数': int(tds[3].text.strip()) if tds[3].text.strip().isdigit() else 0,
                    '安打': int(tds[5].text.strip()) if tds[5].text.strip().isdigit() else 0,
                    '打点': int(tds[6].text.strip()) if tds[6].text.strip().isdigit() else 0,
                    '盗塁': int(tds[7].text.strip()) if tds[7].text.strip().isdigit() else 0,
                    '本塁打': sum('本' in td.get_text() for td in tds[5:]),
                    '三振': sum('三　振' in td.get_text() for td in tds),
                    '四球': sum('四' in td.get_text() for td in tds),
                    '死球': sum('死　球' in td.get_text() for td in tds),
                    '犠打': sum('犠打' in td.get_text() for td in tds),
                    '犠飛': sum('犠飛' in td.get_text() for td in tds)
                }
                batters.append(batter)
        # 投手成績
        if home_away_status == 'ビジター':
            pitch_div = soup.find('div', id='table_top_p')
            pitch_table = pitch_div.find('table', id='tablefix_t_p') if pitch_div else None
        else:
            pitch_div = soup.find('div', id='table_bottom_p')
            pitch_table = pitch_div.find('table', id='tablefix_b_p') if pitch_div else None
        if pitch_table and pitch_table.find('tbody'):
            for tr in pitch_table.find('tbody').find_all('tr'):
                tds = tr.find_all('td', recursive=False)
                if not tds or len(tds) < 13:
                    continue
                player_td = tr.find('td', class_='player')
                if not player_td:
                    continue
                player_name = player_td.text.strip()
                # 投球回（5つ目<td>内の<th>から数字のみ）
                tokkyukai = 0
                th_in_td = tds[4].find('th') if tds[4] else None
                if th_in_td:
                    match = re.search(r'\d+', th_in_td.text)
                    if match:
                        tokkyukai = int(match.group())
                pitcher = {
                    '選手名': player_name,
                    '投球回': tokkyukai,
                    '投球数': int(tds[2].text.strip()) if tds[2].text.strip().isdigit() else 0,
                    '打者数': int(tds[3].text.strip()) if tds[3].text.strip().isdigit() else 0,
                    '被安打': int(tds[5].text.strip()) if tds[5].text.strip().isdigit() else 0,
                    '被本塁打': int(tds[6].text.strip()) if tds[6].text.strip().isdigit() else 0,
                    '与四球': int(tds[7].text.strip()) if tds[7].text.strip().isdigit() else 0,
                    '与死球': int(tds[8].text.strip()) if tds[8].text.strip().isdigit() else 0,
                    '奪三振': int(tds[9].text.strip()) if tds[9].text.strip().isdigit() else 0,
                    '暴投': int(tds[10].text.strip()) if tds[10].text.strip().isdigit() else 0,
                    'ボーク': int(tds[11].text.strip()) if tds[11].text.strip().isdigit() else 0,
                    '失点': int(tds[12].text.strip()) if tds[12].text.strip().isdigit() else 0
                }
                pitchers.append(pitcher)
    except Exception as e:
        print(f"[ERROR] 選手個人成績スクレイピング失敗: {e}")
    return batters, pitchers

def update_batter_stats(batters, team_full_name):
    """
    打者成績をdata/batters_stats.csvに累積加算で保存。
    """
    BATTERS_CSV = os.path.join('data', 'batters_stats.csv')
    COLUMNS = ['選手名','チーム名','打数','安打','打点','盗塁','本塁打','三振','四球','死球','犠打','犠飛']
    # 必要なカラムのみで初期化
    if not os.path.exists(BATTERS_CSV):
        df = pd.DataFrame(columns=COLUMNS)
        df.to_csv(BATTERS_CSV, index=False, encoding='utf-8-sig')
    else:
        df = pd.read_csv(BATTERS_CSV, encoding='utf-8-sig')
    for b in batters:
        # 選手名＋チーム名で一意
        mask = (df['選手名'] == b['選手名']) & (df['チーム名'] == team_full_name)
        if mask.any():
            df.loc[mask, '打数'] = df.loc[mask, '打数'].fillna(0).astype(int) + b.get('打数',0)
            df.loc[mask, '安打'] = df.loc[mask, '安打'].fillna(0).astype(int) + b.get('安打',0)
            df.loc[mask, '打点'] = df.loc[mask, '打点'].fillna(0).astype(int) + b.get('打点',0)
            df.loc[mask, '盗塁'] = df.loc[mask, '盗塁'].fillna(0).astype(int) + b.get('盗塁',0)
            df.loc[mask, '本塁打'] = df.loc[mask, '本塁打'].fillna(0).astype(int) + b.get('本塁打',0)
            df.loc[mask, '三振'] = df.loc[mask, '三振'].fillna(0).astype(int) + b.get('三振',0)
            df.loc[mask, '四球'] = df.loc[mask, '四球'].fillna(0).astype(int) + b.get('四球',0)
            df.loc[mask, '死球'] = df.loc[mask, '死球'].fillna(0).astype(int) + b.get('死球',0)
            df.loc[mask, '犠打'] = df.loc[mask, '犠打'].fillna(0).astype(int) + b.get('犠打',0)
            df.loc[mask, '犠飛'] = df.loc[mask, '犠飛'].fillna(0).astype(int) + b.get('犠飛',0)
        else:
            row = {
                '選手名': b['選手名'],
                'チーム名': team_full_name,
                '打数': b.get('打数',0),
                '安打': b.get('安打',0),
                '打点': b.get('打点',0),
                '盗塁': b.get('盗塁',0),
                '本塁打': b.get('本塁打',0),
                '三振': b.get('三振',0),
                '四球': b.get('四球',0),
                '死球': b.get('死球',0),
                '犠打': b.get('犠打',0),
                '犠飛': b.get('犠飛',0)
            }
            df = pd.concat([df, pd.DataFrame([row])], ignore_index=True)
    df.to_csv(BATTERS_CSV, index=False, encoding='utf-8-sig')

def update_pitcher_stats(pitchers, team_full_name):
    """
    投手成績をdata/pitchers_stats.csvに累積加算で保存。
    pitchers: [{'選手名', '投球回', '打者数', '被安打', '奪三振', '被本塁打'} ...]
    team_full_name: チーム名
    """
    import pandas as pd
    import os
    PITCHERS_CSV = os.path.join('data', 'pitchers_stats.csv')
    COLUMNS = ['選手名','チーム名','投球数','投球回','打者数','被安打','被本塁打','与四球','与死球','奪三振','暴投','ボーク','失点']
    if not os.path.exists(PITCHERS_CSV):
        df = pd.DataFrame(columns=COLUMNS)
        df.to_csv(PITCHERS_CSV, index=False, encoding='utf-8-sig')
    else:
        df = pd.read_csv(PITCHERS_CSV, encoding='utf-8-sig')
    for p in pitchers:
        mask = (df['選手名'] == p['選手名']) & (df['チーム名'] == team_full_name)
        if mask.any():
            df.loc[mask, '投球数'] = df.loc[mask, '投球数'].fillna(0).astype(int) + p.get('投球数',0)
            df.loc[mask, '投球回'] = df.loc[mask, '投球回'].fillna(0).astype(int) + p.get('投球回',0)
            df.loc[mask, '打者数'] = df.loc[mask, '打者数'].fillna(0).astype(int) + p.get('打者数',0)
            df.loc[mask, '被安打'] = df.loc[mask, '被安打'].fillna(0).astype(int) + p.get('被安打',0)
            df.loc[mask, '被本塁打'] = df.loc[mask, '被本塁打'].fillna(0).astype(int) + p.get('被本塁打',0)
            df.loc[mask, '与四球'] = df.loc[mask, '与四球'].fillna(0).astype(int) + p.get('与四球',0)
            df.loc[mask, '与死球'] = df.loc[mask, '与死球'].fillna(0).astype(int) + p.get('与死球',0)
            df.loc[mask, '奪三振'] = df.loc[mask, '奪三振'].fillna(0).astype(int) + p.get('奪三振',0)
            df.loc[mask, '暴投'] = df.loc[mask, '暴投'].fillna(0).astype(int) + p.get('暴投',0)
            df.loc[mask, 'ボーク'] = df.loc[mask, 'ボーク'].fillna(0).astype(int) + p.get('ボーク',0)
            df.loc[mask, '失点'] = df.loc[mask, '失点'].fillna(0).astype(int) + p.get('失点',0)
        else:
            row = {
                '選手名': p['選手名'], 'チーム名': team_full_name,
                '投球数': p.get('投球数',0), '投球回': p.get('投球回',0), '打者数': p.get('打者数',0), '被安打': p.get('被安打',0), '被本塁打': p.get('被本塁打',0),
                '与四球': p.get('与四球',0), '与死球': p.get('与死球',0), '奪三振': p.get('奪三振',0), '暴投': p.get('暴投',0), 'ボーク': p.get('ボーク',0), '失点': p.get('失点',0)
            }
            df = pd.concat([df, pd.DataFrame([row])], ignore_index=True)
    df.to_csv(PITCHERS_CSV, index=False, encoding='utf-8-sig')

def analyze_matches(csv_path):
    try:
        df = pd.read_csv(csv_path, encoding='utf-8-sig')
    except Exception:
        return {}

    # 空データ対応
    if df.empty:
        return {}

    # 日付でソート
    df['日付'] = pd.to_datetime(df['日付'], errors='coerce')
    df = df.dropna(subset=['日付'])
    df = df.sort_values(by='日付', ascending=False)

    # 観戦数
    total_games = len(df)

    # 勝敗カウント
    win = (df['勝敗'] == '勝').sum()
    lose = (df['勝敗'] == '敗').sum()
    draw = (df['勝敗'] == '引分').sum()

    # 勝率（引分は分母に入れない）
    denominator = win + lose
    win_rate = round(win / denominator, 3) if denominator > 0 else 0

    # 直近5試合（詳細: 日付・相手チーム・得点・失点・勝敗）
    recent5_df = df.head(5)[['日付', '相手チーム', '得点', '失点', '勝敗']].copy()
    recent5_df['日付'] = recent5_df['日付'].dt.strftime('%Y-%m-%d')
    recent5 = recent5_df.to_dict(orient='records')

    # 現在の波（連勝・連敗数）
    streak = 0
    last = None
    for result in df['勝敗']:
        if last is None:
            last = result
            streak = 1
        elif result == last:
            streak += 1
        else:
            break
    current_streak = f"{streak}{last}" if last else "-"

    # 本拠地/ビジター成績
    home_df = df[df['ホーム/ビジター'] == 'ホーム']
    visitor_df = df[df['ホーム/ビジター'] == 'ビジター']

    home_win = (home_df['勝敗'] == '勝').sum()
    home_lose = (home_df['勝敗'] == '敗').sum()
    home_draw = (home_df['勝敗'] == '引分').sum()

    visitor_win = (visitor_df['勝敗'] == '勝').sum()
    visitor_lose = (visitor_df['勝敗'] == '敗').sum()
    visitor_draw = (visitor_df['勝敗'] == '引分').sum()

    home_total = home_win + home_lose + home_draw
    visitor_total = visitor_win + visitor_lose + visitor_draw
    home_winrate = round(home_win / home_total, 3) if home_total > 0 else 0
    visitor_winrate = round(visitor_win / visitor_total, 3) if visitor_total > 0 else 0

    return {
        '通算観戦数': total_games,
        '通算成績': {'勝': win, '敗': lose, '引分': draw},
        '通算勝率': win_rate,
        '直近5試合': recent5,
        '現在の波': current_streak,
        'ホーム成績': {'勝': home_win, '敗': home_lose, '引分': home_draw, '勝率': f"{home_winrate:.3f}"},
        'ビジター成績': {'勝': visitor_win, '敗': visitor_lose, '引分': visitor_draw, '勝率': f"{visitor_winrate:.3f}"},
    }

@app.route('/totals')
def totals():
    matches_path = os.path.join(DATA_DIR, 'matches.csv')
    try:
        df = pd.read_csv(matches_path, encoding='utf-8-sig')
    except Exception:
        df = pd.DataFrame()

    # --- 基本集計 ---
    total_games = len(df)
    win = (df['勝敗'] == '勝').sum() if not df.empty else 0
    lose = (df['勝敗'] == '敗').sum() if not df.empty else 0
    draw = (df['勝敗'] == '引分').sum() if not df.empty else 0
    denominator = win + lose
    win_rate = round(win / denominator, 3) if denominator > 0 else 0

    # --- 通算打撃成績 ---
    total_at_bats = df['自チーム_打数'].sum() if not df.empty else 0
    total_hits = df['自チーム_安打'].sum() if not df.empty else 0
    total_hr = df['自チーム_本塁打'].sum() if not df.empty else 0
    avg = round(total_hits / total_at_bats, 3) if total_at_bats > 0 else 0
    hr_rate = round(total_hr / total_at_bats, 3) if total_at_bats > 0 else 0

    # --- 通算投手成績（防御率）---
    total_innings = 0
    # 打数＋四球＋死球＋犠打＋犠飛＝打席数（犠打・犠飛はデータにない場合は打数＋四球＋死球）
    # 1試合9イニング換算
    if not df.empty:
        total_innings = total_games * 9
    total_runs_allowed = df['失点'].sum() if not df.empty else 0
    era = round(total_runs_allowed * 9 / total_innings, 2) if total_innings > 0 else 0

    # --- 通算被打率・被本塁打数 ---
    opp_at_bats = df['相手チーム_打数'].sum() if not df.empty else 0
    opp_hits = df['相手チーム_安打'].sum() if not df.empty else 0
    opp_hr = df['相手チーム_本塁打'].sum() if not df.empty else 0
    opp_avg = round(opp_hits / opp_at_bats, 3) if opp_at_bats > 0 else 0
    # 被本塁打数は合計
    
    # --- 対戦チームごとの試合数・勝率 ---
    vs_team_stats = []
    if not df.empty:
        grouped = df.groupby('相手チーム')
        for team, group in grouped:
            games = len(group)
            win = (group['勝敗'] == '勝').sum()
            lose = (group['勝敗'] == '敗').sum()
            denominator = win + lose
            rate = round(win / denominator, 3) if denominator > 0 else 0
            vs_team_stats.append({
                'team': team,
                'games': games,
                'win_rate': rate
            })
        vs_team_stats = sorted(vs_team_stats, key=lambda x: x['games'], reverse=True)

    return render_template('totals.html',
        total_games=total_games,
        win=win,
        lose=lose,
        draw=draw,
        win_rate=win_rate,
        avg=avg,
        hr_rate=hr_rate,
        era=era,
        opp_avg=opp_avg,
        opp_hr=opp_hr,
        vs_team_stats=vs_team_stats
    )

@app.route('/delete_match/<int:row_id>', methods=['POST'])
def delete_match(row_id):
    """
    指定した行番号(row_id)の試合データをmatches.csvから削除する
    """
    import pandas as pd
    matches_path = os.path.join(DATA_DIR, 'matches.csv')
    try:
        df = pd.read_csv(matches_path, encoding='utf-8-sig')
        if 0 <= row_id < len(df):
            df = df.drop(df.index[row_id]).reset_index(drop=True)
            df.to_csv(matches_path, index=False, encoding='utf-8-sig')
            flash('試合データを削除しました', 'success')
        else:
            flash('指定された試合データが存在しません', 'error')
    except Exception as e:
        flash(f'削除中にエラー: {e}', 'error')
    return redirect(url_for('summary'))

if __name__ == '__main__':
    initialize_csv()
    initialize_backup_counter()
    app.run(debug=True)