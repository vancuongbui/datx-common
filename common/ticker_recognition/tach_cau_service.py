import re
from .utils import xoa_cham_trong_so, xoa_tag, remove_emoji, data_ma_ck, ma_ck_giong_tn, data_ma_tt

def check_mang(a,b):
    check = True
    for k in a:
        if k not in b:
            check = False
    if check == False:
        return False
    else:
        return True

class TachCauService:

    def set(text):
        danh_sach_1 = []
        text = text.replace('{','{+!@')
        text_test =  text.strip().split('{')
        # text_test = [text]

        for l in range(0,len(text_test)) :
            text = text_test[l].replace('+!@','{')
            text = text.replace("\n",".")
            cau_tag_cu_1 = []
            cau_tag_cu_2 = []
            text = re.sub(r'http\S+', '', text) # Xóa link
            text = xoa_cham_trong_so(text)
            list_ma_ck = []
            list_ma_ck_cu = []
            list_cau_co_ma = []
            danh_sach_cau = []
            vi_tri_ngoac = []
            vi_tri_cau_tag = []
            for g in range(0,len(text)):
                if text[g] == '{' or text[g] == '}':
                    vi_tri_ngoac.append(g)
            for g in range(0,len(vi_tri_ngoac)-1):
                if( g % 2 == 0):
                    cau_tag = text[vi_tri_ngoac[g]:vi_tri_ngoac[g+1]+1]
                    cau_tag_cu_1.append(cau_tag)
                    cau_tag_2 = cau_tag.replace('.',';').replace(',',';')
                    cau_tag_cu_2.append(cau_tag_2)
                    text = text.replace(cau_tag,cau_tag_2)
            text_sllit = text.strip().split('.')
            if len(text_sllit) > 1:
                for h in range(len(text_sllit)):
                    text_3 = xoa_tag(text_sllit[h])
                    str_tin_nhan = remove_emoji(text_3).upper().split()
                    content_xu_ly = remove_emoji(text_3).split()
                    for g in str_tin_nhan:
                        try:
                            if g in data_ma_ck:
                                if g in ma_ck_giong_tn and g not in content_xu_ly:
                                    break
                                if g not in list_ma_ck:
                                    list_ma_ck.append(g)
                        except Exception as e:
                            pass
                    if check_mang(list_ma_ck,list_ma_ck_cu) == False:
                        if len(list_ma_ck) > 0 and len(text_sllit[h]) > 8 and '{' not in text_sllit[h]:
                            list_cau_co_ma.append(h)
                            list_ma_ck_cu = list_ma_ck[:]
                    list_ma_ck.clear()
            if len(text_sllit) == 1:
                text_sllit = text.strip().split(',')
                for h in range(len(text_sllit)):
                    if len(text_sllit[h]) > 4:
                        text_3 = xoa_tag(text_sllit[h])
                        str_tin_nhan = remove_emoji(text_3).upper().split()
                        content_xu_ly = remove_emoji(text_3).split()
                        for g in str_tin_nhan:
                            try:
                                if g in data_ma_ck:
                                    if g in ma_ck_giong_tn and g not in content_xu_ly:
                                        break
                                    if g not in list_ma_ck:
                                        list_ma_ck.append(g)
                            except Exception as e:
                                pass
                    if check_mang(list_ma_ck,list_ma_ck_cu) == False:
                        if len(list_ma_ck) > 0 and len(text_sllit[h]) > 8 and '{' not in text_sllit[h]:
                            list_cau_co_ma.append(h)
                            list_ma_ck_cu = list_ma_ck[:]
                    list_ma_ck.clear()
            if len(list_cau_co_ma) > 0:
                for h in range(0, len(list_cau_co_ma)):
                    text_1 = ""
                    if len(list_cau_co_ma) == 1:
                        text_1 = "".join(text)
                        danh_sach_cau.append(text_1)
                        break
                    if h == 0:
                        for k in range(0, int(list_cau_co_ma[h+1])):
                            text_1 = text_1 + text_sllit[k] + "."
                        danh_sach_cau.append(text_1)
                        continue
                    if h == len(list_cau_co_ma) - 1:
                        for k in range(int(list_cau_co_ma[h]), len(text_sllit)):
                            text_1 = text_1 + text_sllit[k] + "."
                        danh_sach_cau.append(text_1)
                        break
                    for k in range(int(list_cau_co_ma[h]), int(list_cau_co_ma[h+1])):
                        text_1 = text_1 + text_sllit[k] + "."
                    danh_sach_cau.append(text_1)
            else:
                danh_sach_cau.append(text)

            for k in danh_sach_cau:
                text = k
                for h in range(0,len(cau_tag_cu_2)):
                    if cau_tag_cu_2[h] in text:
                        text = k.replace(cau_tag_cu_2[h],cau_tag_cu_1[h])
                danh_sach_1.append(text)
        danh_sach_new = []
        for k in danh_sach_1:
            if len(k) > 0:
                danh_sach_new.append(k)
        return danh_sach_new
