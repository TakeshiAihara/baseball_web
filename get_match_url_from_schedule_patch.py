from datetime import datetime
import requests
from bs4 import BeautifulSoup
import re
import traceback

def get_match_url_from_schedule(target_date_str, team_name_input, TEAM_NAME_MAPPING_NPB=None):
    """
    NPB公式サイトの試合日程ページから、指定された日付とチーム名の試合URLを抽出する。
    TEAM_NAME_MAPPING_NPBは辞書型で、display名→NPB短縮名のマッピングを想定。
    """
    try:
        input_date = datetime.strptime(target_date_str, '%Y-%m-%d')
        npb_team_name = TEAM_NAME_MAPPING_NPB.get(team_name_input) if TEAM_NAME_MAPPING_NPB else team_name_input
        schedule_url = f"https://npb.jp/games/{input_date.year}/schedule_{input_date.month:02d}_detail.html"
        #print("schedule_url:", schedule_url)
        #print("npb_team_name:", npb_team_name)
        target_date_id_prefix = f"date{input_date.month:02d}{input_date.day:02d}"
        #print("target_date_id_prefix:", target_date_id_prefix)
        response = requests.get(schedule_url)
        response.raise_for_status()
        response.encoding = 'utf-8'
        soup = BeautifulSoup(response.text, 'html.parser')
        print("HTML取得長さ:", len(response.text))
        all_tr_tags_for_date = soup.find_all('tr', id=lambda x: x and x.startswith(target_date_id_prefix))
        #print("all_tr_tags_for_date件数:", len(all_tr_tags_for_date))
        for date_row_tr_tag in all_tr_tags_for_date:
            #print("trタグ:", date_row_tr_tag)
            game_cells = [td for td in date_row_tr_tag.find_all('td') if td.find('div', class_='team1') or td.find('div', class_='team2')]
            #print("game_cells件数:", len(game_cells))
            for cell in game_cells:
                team1_elem = cell.find('div', class_='team1') # ホーム
                team2_elem = cell.find('div', class_='team2') # ビジター
                team1_text = team1_elem.text.strip() if team1_elem else ""
                team2_text = team2_elem.text.strip() if team2_elem else ""
                #print("team1_text:", team1_text, ", team2_text:", team2_text)
                home_away_status = ""
                if team1_text in npb_team_name or npb_team_name in team1_text:
                    home_away_status = "ホーム"
                elif team2_text in npb_team_name or npb_team_name in team2_text:
                    home_away_status = "ビジター"
                #print("home_away_status:", home_away_status)
                if home_away_status:
                    match_link_tag = cell.find('a', href=re.compile(r'/scores/'))
                    #print("match_link_tag:", match_link_tag)
                    if match_link_tag and '/stats' not in match_link_tag.get('href'):
                        target_match_url = match_link_tag.get('href')
                        if not target_match_url.endswith('/box.html'):
                            target_match_url = target_match_url.rstrip('/') + '/box.html'
                        #print("target_match_url:", target_match_url)
                        return target_match_url, home_away_status
    except Exception as e:
        print(f"ERROR: 試合URL抽出中にエラーが発生: {e}")
        traceback.print_exc()
    return None, None
