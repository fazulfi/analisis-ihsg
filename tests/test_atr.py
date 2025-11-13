import pandas as pd
from indicators.atr import compute_tr, compute_atr_wilder

def test_atr_wilder_n3():
    df = pd.DataFrame([
        {"date":"2025-11-01","open":1000,"high":1010,"low":1000,"close":1005},
        {"date":"2025-11-02","open":1005,"high":1017,"low":1005,"close":1007},
        {"date":"2025-11-03","open":1007,"high":1015,"low":1007,"close":1009},
        {"date":"2025-11-04","open":1009,"high":1023,"low":1009,"close":1010},
    ])
    df_tr = compute_tr(df)
    tr_expected = [10,12,8,14]
    assert list(df_tr['tr'].round(8).astype(float)) == tr_expected
    atr = compute_atr_wilder(df_tr['tr'], 3)
    assert pd.isna(atr.iloc[0])
    assert pd.isna(atr.iloc[1])
    assert abs(atr.iloc[2] - 10.0) <= 1e-8
    assert abs(atr.iloc[3] - (34.0/3.0)) <= 1e-8
