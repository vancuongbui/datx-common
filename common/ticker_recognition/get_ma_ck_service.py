import re
from .utils import xoa_tag, xoa_cham_trong_so, remove_emoji, data_ma_ck, ma_ck_giong_tn, data_ma_tt
from typing import Literal


class GetMaCKService:

    def set(contents, return_type: Literal["sector", "ticker"]):
        try:
            contents = xoa_tag(contents)  # Xoá tag
        except:
            pass
        contents = re.sub(r"http\S+", "", contents)  # Xóa link
        contents = xoa_cham_trong_so(contents)  # Xoá chấm trong số
        contents = contents.replace("\n", ".")  # Xoá xuống dòng thành .
        list_ma_ck = []
        list_nganh = []

        str_tin_nhan = remove_emoji(contents).upper().split()  # Viết hoa câu và tách câu thành mảng
        # print("++++++++++++++++++++++")
        # print(str_tin_nhan)
        content_xu_ly = remove_emoji(contents).split()  # Tách câu thành mảng

        # Lấy mảng key mã chứng khoán
        data_ma_ck_arr = []
        for key, value in data_ma_ck.items():
            data_ma_ck_arr.append(key)

        # Loop mảng mã thị trường
        for key, value in data_ma_tt.items():
            # Nếu mã thị trường nhiều hơn 1 từ
            if " " in key:
                # Tìm mã thị trường trong câu
                if contents.upper().find(key) != -1:
                    if value not in list_nganh:
                        list_nganh.append(value)
            # Nếu mã thị trường là 1 từ
            else:
                # Tìm mã thị trường trong mảng từ trong câu
                if key in str_tin_nhan:
                    if value not in list_nganh:
                        list_nganh.append(value)

        # Tìm phần tử chung của mảng key mã chứng khoán và mảng từ trong câu => ra mảng mã chứng khoán xuất hiện trong câu
        list_ma_ck = list(set(data_ma_ck_arr).intersection(str_tin_nhan))

        idxs = []
        # Loop mảng mã chứng khoán xuất hiện trong câu
        for idx, val in enumerate(list_ma_ck):
            # Nếu mã xuất hiện trong Mã chứng khoán giống tin nhắn và mã đó không viết hoa
            # Lưu lại index của mã trong mảng => xoá mã
            if val in ma_ck_giong_tn and val not in content_xu_ly:
                idxs.append(idx)
            else:
                if data_ma_ck[val] not in list_nganh:
                    list_nganh.append(data_ma_ck[val])

        for idx in reversed(idxs):
            list_ma_ck = list_ma_ck[:idx] + list_ma_ck[idx + 1 :]
        if len(list_ma_ck) == 0:
            ma_ck = None
        else:
            ma_ck = ",".join(list_ma_ck)

        if len(list_nganh) == 0:
            nganh = None
        else:
            nganh = ",".join(list_nganh)

        if return_type == "sector":
            return nganh
        elif return_type == "ticker":
            return ma_ck
