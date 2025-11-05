import re
import emoji
import os
from pathlib import Path
import json

__location__ = Path(os.path.realpath(os.path.join(os.getcwd(), os.path.dirname(__file__))))


with open(__location__/'./data/ma_ck.json', encoding='utf-8') as f:
    data_ma_ck = json.load(f)

with open(__location__/'./data/ma_tt.json', encoding='utf-8') as f:
    data_ma_tt = json.load(f)

with open(__location__/'./data/ma_ck_giong_tn.json') as f:
    ma_ck_giong_tn = json.load(f)

def xoa_tag(string):
    list_1 = []
    list_2 = []
    list_3 = []
    for k in range(len(string)) :
        if k == len(string) - 1:
          break
        if string[k] == '@':
            list_1.append(k)
        if string[k] == " " and string[k+1].isupper() == False :
            list_2.append(k)
    for i in list_1:
        for k in list_2:
            if i < k:
                list_3.append(string[i:k])
                break
    for h in list_3:
        string = re.sub(h,"",string)
    string = string.strip()
    return string

def remove_emoji(text):
    text = re.sub(r"[-()\"#/@;:<>{}`+=~|.!?,]", " ", text)
    # return emoji.emoji_list ().sub(u'', text)
    return emoji.replace_emoji(text, replace='')

def xoa_cham_trong_so(text):
    for k in range(0,len(text)):
        if len(text) <4:
            break
        if k == len(text) - 3:
            break
        if text[k].isnumeric() == True and text[k+1] == "." and text[k+2] != " ":
            temp = list(text)
            temp[k+1] = " "
            text = "".join(temp)
    return text