# -*- coding: utf-8 -*-
"""
FengHe 3D3C3T5M - 风和投资 3D3C3T5M 分析系统
数据源: akshare (东方财富datacenter + 同花顺 + 新浪 + 百度估值, 国内直连)
架构: Python标准库HTTP服务 + 3D3C3T5M规则评分引擎 + 原生HTML前端
评分模型: 综合分 = 0.15*3C + 0.30*3D + 0.40*5M + 0.15*3T
"""
import os
os.environ["TQDM_DISABLE"] = "1"

import json
import re
import statistics
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse, parse_qs
from pathlib import Path

import akshare as ak
import pandas as pd
import requests as _req

# ---- 全局 User-Agent 注入 (akshare 内部也走 requests, 全局生效) ----
_orig_req = _req.Session.request
def _patched_req(self, *a, **kw):
    kw.setdefault("headers", {})
    if isinstance(kw["headers"], dict):
        kw["headers"].setdefault("User-Agent",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
    return _orig_req(self, *a, **kw)
_req.Session.request = _patched_req

BASE_DIR = Path(__file__).parent
PORT = 8021
CACHE_TTL = 300

_cache = {}       # code -> (ts, result)
_cache_lock = threading.Lock()
_sv_cache = {}    # code -> DataFrame (A股/B股行情缓存)
_hk_cache = {}    # code -> {pe_df, pb_df, fin_df}
_name_cache = {}  # code -> name


def sf(x, default=None):
    """安全转 float, False/空串/None -> default"""
    if x is None:
        return default
    if isinstance(x, bool):
        return default
    try:
        if isinstance(x, str):
            x = x.strip().replace(",", "")
            if x.endswith("%"):
                x = x[:-1]
            if not x or x in ("-", "--", "None", "nan"):
                return default
        v = float(x)
        if v != v:
            return default
        return v
    except (ValueError, TypeError):
        return default


def parse_cn_amount(s):
    """解析 '5.05亿'/'3200万' 为数值(元)"""
    if s is None:
        return None
    s = str(s).strip()
    if not s:
        return None
    m = re.match(r"^-?([\d.]+)\s*([亿万]?)$", s)
    if not m:
        return sf(s)
    num = float(m.group(1))
    unit = m.group(2)
    if unit == "亿":
        return num * 1e8
    if unit == "万":
        return num * 1e4
    return num


def detect_market(code):
    """识别市场: 'A' / 'B' / 'HK'"""
    code = re.sub(r"\D", "", str(code))
    if len(code) == 6:
        if code.startswith(("200", "900")):
            return "B", code
        return "A", code
    if 1 <= len(code) <= 5:
        return "HK", code.lstrip("0").rjust(5, "0") if code != "0" * 5 else "00000"
    return "A", code.zfill(6)


def with_retry(func, *args, retries=3, base_delay=2, **kwargs):
    last_err = None
    for i in range(retries):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            last_err = e
            if i < retries - 1:
                time.sleep(base_delay * (i + 1))
    raise last_err


def clamp(v, lo=0.0, hi=10.0):
    return max(lo, min(hi, v))


# ---------- 数据层 ----------

def _get_sv(code):
    """A股: stock_value_em (datacenter, 内网稳定). 返回历史估值+价格 DataFrame"""
    if code not in _sv_cache:
        try:
            _sv_cache[code] = with_retry(ak.stock_value_em, symbol=code, retries=2)
        except Exception:
            try:
                _sv_cache[code] = ak.stock_value_em(symbol=code)
            except Exception:
                raise RuntimeError(
                    f"未找到代码 {code} 的数据。支持: 沪深A股(600/000/300/688开头)、"
                    f"B股(200/900开头)、港股(1-5位数字, 如00700)。请确认代码正确或未退市。")
    return _sv_cache[code]


def _get_hk_data(code):
    """港股: 百度估值PE/PB + 东财港股财务"""
    if code in _hk_cache:
        return _hk_cache[code]
    data = {}
    data["pe"] = with_retry(ak.stock_hk_valuation_baidu, symbol=code,
                            indicator="市盈率(TTM)", period="近三年", retries=3)
    time.sleep(0.5)
    data["pb"] = with_retry(ak.stock_hk_valuation_baidu, symbol=code,
                            indicator="市净率", period="近一年", retries=3)
    time.sleep(0.5)
    data["fin"] = with_retry(ak.stock_financial_hk_analysis_indicator_em,
                             symbol=code, retries=3)
    _hk_cache[code] = data
    return data


def _get_b_daily(code):
    """B股: 新浪日线"""
    key = "B" + code
    if key not in _sv_cache:
        prefix = "sz" if code.startswith("2") else "sh"
        _sv_cache[key] = with_retry(ak.stock_zh_b_daily, symbol=f"{prefix}{code}", retries=3)
    return _sv_cache[key]


def _get_b_spot():
    """B股: 新浪 spot (代码/名称/最新价/涨跌幅)"""
    return with_retry(ak.stock_zh_b_spot, retries=2)


def _pct_series(series, ref):
    """计算序列中小于 ref 的占比(%)作为分位, 要求至少50个正值样本"""
    s = pd.to_numeric(series, errors="coerce").dropna()
    s = s[s > 0]
    if len(s) < 50 or ref is None:
        return None
    tail = s.tail(min(750, len(s)))
    return round(float((tail < ref).mean() * 100), 1)


def get_history(code, market="A"):
    """价格/估值/分位/动量"""
    if market == "A":
        df = _get_sv(code)
        if df is None or len(df) == 0:
            raise RuntimeError("未找到行情数据")
        # 列名探测
        def pick(cands):
            for c in cands:
                if c in df.columns:
                    return c
            return None
        date_col = pick(["date", "日期", "数据日期"])
        close_col = pick(["收盘", "close", "最新价", "当日收盘价"])
        pe_col = pick(["市盈率-动态", "市盈率TTM", "pe_ttm", "市盈率(TTM)", "PE(TTM)"])
        pb_col = pick(["市净率", "pb", "PB"])
        mcap_col = pick(["总市值", "总市值元"])
        name_col = pick(["名称", "证券简称"])

        closes = pd.to_numeric(df[close_col], errors="coerce").dropna()
        if len(closes) == 0:
            raise RuntimeError("价格数据为空")
        last = float(closes.iloc[-1])
        chg = round((last / float(closes.iloc[-2]) - 1) * 100, 2) if len(closes) >= 2 else None
        ma20 = float(closes.tail(20).mean()) if len(closes) >= 20 else last
        ma60 = float(closes.tail(60).mean()) if len(closes) >= 60 else last
        win = min(244, len(closes))
        high52 = float(closes.tail(win).max())
        low52 = float(closes.tail(win).min())
        mom_1m = (last / float(closes.iloc[-22]) - 1) * 100 if len(closes) >= 22 else None
        mom_1y = (last / float(closes.iloc[-win]) - 1) * 100 if len(closes) > win else None

        pe_ttm = sf(df[pe_col].iloc[-1]) if pe_col else None
        pb = sf(df[pb_col].iloc[-1]) if pb_col else None
        pe_pct = _pct_series(df[pe_col], pe_ttm) if pe_col else None
        pb_pct = _pct_series(df[pb_col], pb) if pb_col else None
        mcap = sf(df[mcap_col].iloc[-1]) if mcap_col else None
        if mcap and mcap < 1e6:
            mcap = None
        name = str(df[name_col].iloc[-1]).strip() if name_col else None
        date = str(pd.to_datetime(df[date_col], errors="coerce").iloc[-1])[:10] if date_col else ""
        return {
            "price": round(last, 2), "change_pct": chg, "date": date,
            "ma20": round(ma20, 2), "ma60": round(ma60, 2),
            "high_52w": round(high52, 2), "low_52w": round(low52, 2),
            "off_high_pct": round((last / high52 - 1) * 100, 2) if high52 else None,
            "mom_1m": round(mom_1m, 2) if mom_1m is not None else None,
            "mom_1y": round(mom_1y, 2) if mom_1y is not None else None,
            "pe_ttm": round(pe_ttm, 2) if pe_ttm else None,
            "pb": round(pb, 2) if pb else None,
            "pe_pct_3y": pe_pct, "pb_pct_3y": pb_pct,
            "mcap": mcap, "name": name,
        }
    elif market == "B":
        df = _get_b_daily(code)
        closes = pd.to_numeric(df["close"], errors="coerce").dropna()
        if len(closes) == 0:
            raise RuntimeError("B股价格数据为空")
        last = float(closes.iloc[-1])
        chg = round((last / float(closes.iloc[-2]) - 1) * 100, 2) if len(closes) >= 2 else None
        ma20 = float(closes.tail(20).mean()) if len(closes) >= 20 else last
        ma60 = float(closes.tail(60).mean()) if len(closes) >= 60 else last
        win = min(244, len(closes))
        high52 = float(closes.tail(win).max())
        low52 = float(closes.tail(win).min())
        mom_1m = (last / float(closes.iloc[-22]) - 1) * 100 if len(closes) >= 22 else None
        mom_1y = (last / float(closes.iloc[-win]) - 1) * 100 if len(closes) > win else None
        price_pct = None
        if len(closes) > 100:
            recent = closes.tail(750)
            price_pct = round(float((recent < last).mean() * 100), 1)
        name = None
        try:
            spot = _get_b_spot()
            srow = spot[spot["代码"].astype(str).str.contains(code)]
            if len(srow) > 0:
                name = str(srow.iloc[0].get("名称", "")).strip()
        except Exception:
            pass
        return {
            "price": round(last, 2), "change_pct": chg,
            "date": str(df["date"].iloc[-1])[:10],
            "ma20": round(ma20, 2), "ma60": round(ma60, 2),
            "high_52w": round(high52, 2), "low_52w": round(low52, 2),
            "off_high_pct": round((last / high52 - 1) * 100, 2) if high52 else None,
            "mom_1m": round(mom_1m, 2) if mom_1m is not None else None,
            "mom_1y": round(mom_1y, 2) if mom_1y is not None else None,
            "pe_ttm": None, "pb": None,
            "pe_pct_3y": price_pct, "pb_pct_3y": None,
            "name": name,
        }
    else:  # HK
        data = _get_hk_data(code)
        pe_df, pb_df, fin = data["pe"], data["pb"], data["fin"]
        pe_series = pd.to_numeric(pe_df.iloc[:, 1], errors="coerce").dropna() if pe_df is not None and len(pe_df) > 0 else None
        pe_series = pe_series[pe_series > 0] if pe_series is not None else None
        pe_now = pe_pct = None
        mom_1 = mom_1y = None
        if pe_series is not None and len(pe_series) > 50:
            pe_now = float(pe_series.iloc[-1])
            pe_pct = round(float((pe_series < pe_now).mean() * 100), 1)
            if len(pe_series) >= 22:
                mom_1 = round((pe_now / float(pe_series.iloc[-22]) - 1) * 100, 2)
            if len(pe_series) >= 244:
                mom_1y = round((pe_now / float(pe_series.iloc[-244]) - 1) * 100, 2)
        pb_now = pb_pct = None
        if pb_df is not None and len(pb_df) > 0:
            pb_s = pd.to_numeric(pb_df.iloc[:, 1], errors="coerce").dropna()
            pb_s = pb_s[pb_s > 0]
            if len(pb_s) > 20:
                pb_now = float(pb_s.iloc[-1])
                pb_pct = round(float((pb_s < pb_now).mean() * 100), 1)
        name = None
        if fin is not None and len(fin) > 0 and "SECURITY_NAME_ABBR" in fin.columns:
            name = str(fin.iloc[0].get("SECURITY_NAME_ABBR", "")).strip()
        return {
            "pe_ttm": round(pe_now, 2) if pe_now else None,
            "pb": round(pb_now, 2) if pb_now else None,
            "pe_pct_3y": pe_pct, "pb_pct_3y": pb_pct,
            "mom_1m": mom_1, "mom_1y": mom_1y,
            "name": name,
        }


def get_financials(code, market="A"):
    """财务摘要, 返回最近N期(最新在前)"""
    if market in ("A", "B"):
        df = with_retry(ak.stock_financial_abstract_ths, symbol=code, retries=2)
        if df is None or len(df) == 0:
            raise RuntimeError("未获取到财务数据")
        out = []
        for _, row in df.iterrows():
            out.append({
                "period": str(row.get("报告期", ""))[:10],
                "roe": sf(row.get("净资产收益率")),
                "net_profit": parse_cn_amount(row.get("净利润")),
                "revenue": parse_cn_amount(row.get("营业总收入")),
                "np_yoy": sf(row.get("净利润同比增长率")),
                "rev_yoy": sf(row.get("营业总收入同比增长率")),
                "gross_margin": sf(row.get("销售毛利率")),
                "net_margin": sf(row.get("销售净利率")),
                "debt_ratio": sf(row.get("资产负债率")),
                "eps": sf(row.get("基本每股收益")),
                "bvps": sf(row.get("每股净资产")),
                "op_margin": sf(row.get("销售净利率")),
            })
        out.reverse()
        return out
    else:  # HK
        df = with_retry(ak.stock_financial_hk_analysis_indicator_em, symbol=code, retries=2)
        if df is None or len(df) == 0:
            raise RuntimeError("未获取到港股财务数据")
        out = []
        for _, row in df.iterrows():
            out.append({
                "period": str(row.get("REPORT_DATE", ""))[:10],
                "roe": sf(row.get("ROE_AVG")),
                "net_profit": None,
                "revenue": None,
                "np_yoy": sf(row.get("HOLDER_PROFIT_YOY")),
                "rev_yoy": sf(row.get("OPERATE_INCOME_YOY")),
                "gross_margin": sf(row.get("GROSS_PROFIT_RATIO")),
                "net_margin": sf(row.get("NET_PROFIT_RATIO")),
                "debt_ratio": sf(row.get("DEBT_ASSET_RATIO")),
                "eps": sf(row.get("EPS_TTM")),
                "bvps": None,
                "op_margin": None,
            })
        return out


# ---------- 3D3C3T5M 评分引擎 ----------

def score_roe(fins):
    """D1 价值为本: ROE 水平+趋势+稳定性 (内延成长, 匀速跑)"""
    roes = [f["roe"] for f in fins[:12] if f.get("roe") is not None]
    if not roes:
        return 5.0, None
    latest = roes[0]
    if latest >= 20:
        base = 10.0
    elif latest >= 15:
        base = 8.0
    elif latest >= 10:
        base = 6.0
    elif latest >= 5:
        base = 4.0
    else:
        base = 2.0
    trend = 0.0
    if len(roes) >= 3:
        half = roes[min(2, len(roes) - 1)]
        trend = clamp((latest - half) / 2.0, -2.0, 2.0)
    stab = 0.0
    if len(roes) >= 6:
        sd = statistics.pstdev(roes[:8])
        stab = 1.5 if sd < 3 else (0.5 if sd < 6 else 0.0)
    return round(clamp(base + trend + stab), 1), round(latest, 1)


def score_growth(fins):
    """D2/M2: 营收净利增速 + 毛利率变化"""
    if not fins:
        return 5.0, []
    rev_yoy = [f["rev_yoy"] for f in fins[:8] if f.get("rev_yoy") is not None]
    np_yoy = [f["np_yoy"] for f in fins[:8] if f.get("np_yoy") is not None]
    gm = [f["gross_margin"] for f in fins[:8] if f.get("gross_margin") is not None]
    base = 5.0
    if rev_yoy:
        base += clamp(rev_yoy[0] / 10.0, -2.5, 2.5)
    if np_yoy:
        base += clamp(np_yoy[0] / 12.0, -2.5, 2.5)
    if len(gm) >= 2:
        base += clamp((gm[0] - gm[-1]) / 5.0, -1.0, 1.0)
    signals = []
    if rev_yoy and rev_yoy[0] > 0 and np_yoy and np_yoy[0] > 0:
        signals.append("营收/净利双正增长")
    if len(gm) >= 2 and gm[0] >= gm[-1]:
        signals.append("毛利率改善(结构向好)")
    return round(clamp(base), 1), signals


def score_sentiment(hist):
    """D3 情绪: 估值分位 + 动量"""
    pe_pct = hist.get("pe_pct_3y")
    pb_pct = hist.get("pb_pct_3y")
    mom = hist.get("mom_1m")
    score = 5.0
    pct = pe_pct if pe_pct is not None else pb_pct
    if pct is not None:
        score += clamp((50 - pct) / 15.0, -3.0, 3.0)
    if mom is not None:
        if mom > 20:
            score -= 1.0
        elif mom < -15:
            score -= 0.5
        else:
            score += 0.5
    return round(clamp(score), 1)


def score_business(fins):
    """M4 商业模式: 净利率 + 负债率"""
    if not fins:
        return 5.0
    nm = fins[0].get("net_margin")
    debt = fins[0].get("debt_ratio")
    score = 5.0
    if nm is not None:
        score += clamp((nm - 10) / 8.0, -2.5, 2.5)
    if debt is not None:
        score += clamp((45 - debt) / 20.0, -1.5, 1.5)
    return round(clamp(score), 1)


def score_market(fins):
    """M1 目标市场: 营收增速"""
    if not fins or fins[0].get("rev_yoy") is None:
        return 5.0
    return round(clamp(5.0 + clamp(fins[0]["rev_yoy"] / 12.0, -2.5, 2.5)), 1)


def score_share(fins):
    """M2 市场份额: 营收增速(下行周期仍正增=抢份额)"""
    if not fins or fins[0].get("rev_yoy") is None:
        return 5.0
    return round(clamp(5.0 + clamp(fins[0]["rev_yoy"] / 15.0, -3.0, 3.0)), 1)


def score_opm(fins):
    """M3 利润率(OPM): 营业利润率/净利率"""
    if not fins:
        return 5.0
    op = fins[0].get("op_margin")
    nm = fins[0].get("net_margin")
    v = op if op is not None else nm
    if v is None:
        return 5.0
    return round(clamp(5.0 + clamp((v - 15) / 8.0, -3.0, 3.0)), 1)


def score_time(fins, hist):
    """3T: T1短期(0-3月)动量 / T2中期(3-15月)业绩 / T3长期(15月+)ROE"""
    t1 = 5.0
    mom = hist.get("mom_1m")
    if mom is not None:
        t1 = clamp(5.0 + (1.0 if mom > 0 else -1.0) + (1.0 if mom > 10 else 0.0), 0, 10)
    t2 = 5.0
    if fins and fins[0].get("np_yoy") is not None:
        t2 = clamp(5.0 + clamp(fins[0]["np_yoy"] / 12.0, -2.5, 2.5), 0, 10)
    t3 = 5.0
    if fins:
        roes = [f["roe"] for f in fins[:8] if f.get("roe") is not None]
        if roes:
            t3 = clamp(5.0 + clamp((roes[0] - 12) / 5.0, -3.0, 3.0), 0, 10)
    return round(t1, 1), round(t2, 1), round(t3, 1)


def score_management(fins):
    """M5 管理团队: 营收增速稳定性(执行力) + ROE水平(资本配置) + 低负债(治理审慎)"""
    if not fins:
        return 5.0
    score = 5.0
    # 执行力: 营收增速持续为正
    rev_yoy = [f["rev_yoy"] for f in fins[:8] if f.get("rev_yoy") is not None]
    if rev_yoy:
        pos = sum(1 for r in rev_yoy if r > 0)
        score += clamp(pos / len(rev_yoy) * 2.0 - 1.0, -1.5, 2.0)
    # 资本配置能力: ROE水平
    roe = fins[0].get("roe")
    if roe is not None:
        score += clamp((roe - 10) / 5.0, -2.0, 2.0)
    # 治理审慎: 低负债
    debt = fins[0].get("debt_ratio")
    if debt is not None:
        score += clamp((45 - debt) / 20.0, -1.5, 1.5)
    return round(clamp(score), 1)


def score_3c(hist, fins):
    """3C 哲学: Cycle(周期) + Change(变化) + Certainty(确定性)"""
    # C1 Cycle: 估值分位判断周期位置
    pct = hist.get("pe_pct_3y") if hist.get("pe_pct_3y") is not None else hist.get("pb_pct_3y")
    c_cycle = 5.0
    if pct is not None:
        c_cycle += clamp((50 - pct) / 15.0, -3.0, 3.0)
    # C2 Change: 基本面变化(增速+毛利率)
    c_change = 5.0
    if fins:
        np_yoy = fins[0].get("np_yoy")
        rev_yoy = fins[0].get("rev_yoy")
        if np_yoy is not None and np_yoy > 0:
            c_change += 1.5
        elif np_yoy is not None and np_yoy < 0:
            c_change -= 1.0
        if rev_yoy is not None and rev_yoy > 0:
            c_change += 1.0
        gm = [f["gross_margin"] for f in fins[:4] if f.get("gross_margin") is not None]
        if len(gm) >= 2:
            c_change += clamp((gm[0] - gm[-1]) / 5.0, -1.0, 1.0)
    # C3 Certainty: ROE稳定性 + 盈利确定性 + 低负债
    c_certainty = 5.0
    if fins:
        roes = [f["roe"] for f in fins[:8] if f.get("roe") is not None]
        if roes:
            if roes[0] >= 15:
                c_certainty += 2.0
            elif roes[0] >= 10:
                c_certainty += 1.0
            elif roes[0] < 5:
                c_certainty -= 1.5
            if len(roes) >= 4:
                sd = statistics.pstdev(roes[:4])
                if sd < 3:
                    c_certainty += 1.5
                elif sd < 6:
                    c_certainty += 0.5
                else:
                    c_certainty -= 0.5
        debt = fins[0].get("debt_ratio")
        if debt is not None:
            if debt < 40:
                c_certainty += 1.5
            elif debt < 60:
                c_certainty += 0.5
            else:
                c_certainty -= 0.5
    return round(clamp(0.34 * c_cycle + 0.33 * c_change + 0.33 * c_certainty), 1)


def structural_opportunity(fins, hist):
    """结构性机会识别: 风停了还能自己飞"""
    checks = []
    if fins:
        rev = fins[0].get("rev_yoy")
        np_ = fins[0].get("np_yoy")
        checks.append(rev is not None and np_ is not None and rev > 0 and np_ > 0)
        gm = [f["gross_margin"] for f in fins[:4] if f.get("gross_margin") is not None]
        checks.append(len(gm) >= 2 and gm[0] >= gm[-1])
        roe = fins[0].get("roe")
        checks.append(roe is not None and roe >= 8)
    else:
        checks = [False, False, False]
    pct = hist.get("pe_pct_3y") if hist.get("pe_pct_3y") is not None else hist.get("pb_pct_3y")
    checks.append(pct is not None and pct < 50)
    return sum(checks) >= 3


def signal_from_score(total):
    if total >= 6.5:
        return "买入", "综合评分较高，可分批建仓，仓位不超过建议上限"
    if total >= 5.0:
        return "持有偏多", "评分中性偏多，持有为主，回调可关注"
    if total >= 3.5:
        return "持有", "评分中性，建议观望，等待更清晰信号"
    return "卖出", "评分偏低，建议减仓或回避"


def confidence(fins, hist):
    """数据完整度 -> 置信度(0-100)"""
    score = 50
    if len(fins) >= 4:
        score += 20
    elif fins:
        score += 10
    if hist.get("pe_pct_3y") is not None or hist.get("pb_pct_3y") is not None:
        score += 20
    if hist.get("pe_ttm") is not None or hist.get("pb") is not None:
        score += 10
    return min(score, 100)


def max_position(total, pct):
    """3C风控: 建议最大单票仓位(%)"""
    pos = total / 10 * 20
    if pct is not None and pct > 70:
        pos = pos / 2
    return round(min(pos, 20), 1)


def analyze(code):
    """主分析流程"""
    market, code = detect_market(code)
    hist = get_history(code, market)
    fins = get_financials(code, market)
    name = hist.get("name") or _name_cache.get(code) or code

    # 5M
    m1 = score_market(fins)
    m2 = score_share(fins)
    m3 = score_opm(fins)
    m4 = score_business(fins)
    m5 = score_management(fins)
    score_5m = 0.20 * m1 + 0.20 * m2 + 0.20 * m3 + 0.20 * m4 + 0.20 * m5

    # 3D
    d1, roe_now = score_roe(fins)
    d2, growth_signals = score_growth(fins)
    d3 = score_sentiment(hist)
    score_3d = 0.40 * d1 + 0.35 * d2 + 0.25 * d3

    # 3T
    t1, t2, t3 = score_time(fins, hist)
    score_3t = 0.25 * t1 + 0.35 * t2 + 0.40 * t3

    # 3C
    c_score = score_3c(hist, fins)

    total = round(clamp(0.15 * c_score + 0.30 * score_3d + 0.40 * score_5m + 0.15 * score_3t), 1)
    signal, advice = signal_from_score(total)

    pct = hist.get("pe_pct_3y") if hist.get("pe_pct_3y") is not None else hist.get("pb_pct_3y")
    if pct is not None and pct > 85 and signal == "买入":
        signal, advice = "持有", "估值分位过高(>85%)，风控降档，暂不升格买入"

    conf = confidence(fins, hist)
    structural = structural_opportunity(fins, hist)
    max_pos = max_position(total, pct)

    return {
        "code": code, "market": market, "name": name,
        "signal": signal, "advice": advice,
        "score_total": total, "confidence": conf,
        "scores": {
            "3c": c_score, "3d": round(score_3d, 1),
            "5m": round(score_5m, 1), "3t": round(score_3t, 1),
            "d1": d1, "d2": d2, "d3": d3,
            "m1": m1, "m2": m2, "m3": m3, "m4": m4, "m5": m5,
            "t1": t1, "t2": t2, "t3": t3,
        },
        "hist": hist,
        "financials": fins[:12],
        "structural": structural,
        "max_position": max_pos,
        "growth_signals": growth_signals,
    }


# ---------- HTTP ----------

class Handler(BaseHTTPRequestHandler):
    def log_message(self, *a):
        pass

    def _send_json(self, obj, status=200):
        body = json.dumps(obj, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        url = urlparse(self.path)
        qs = parse_qs(url.query)
        if url.path in ("/", "/index.html"):
            self._serve_index()
        elif url.path == "/api/analyze":
            code = (qs.get("code") or [""])[0].strip()
            if not code:
                self._send_json({"ok": False, "reason": "缺少 code 参数"}, 400)
                return
            try:
                result = get_cached(code)
                self._send_json({"ok": True, "result": result})
            except Exception as e:
                self._send_json({"ok": False, "error": str(e)}, 500)
        elif url.path == "/api/health":
            self._send_json({"ok": True, "service": "fenghe-3d3c3t5m"})
        else:
            self._send_json({"ok": False, "error": "not found"}, 404)

    def _serve_index(self):
        idx = BASE_DIR / "index.html"
        if not idx.exists():
            self._send_json({"ok": False, "error": "index.html 不存在"}, 404)
            return
        body = idx.read_bytes()
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


def get_cached(code):
    market, ncode = detect_market(code)
    key = f"{market}:{ncode}"
    with _cache_lock:
        ent = _cache.get(key)
        if ent and time.time() - ent[0] < CACHE_TTL:
            return ent[1]
    result = analyze(ncode)
    with _cache_lock:
        _cache[key] = (time.time(), result)
    return result


def main():
    server = ThreadingHTTPServer(("0.0.0.0", PORT), Handler)
    print(f"FengHe 3D3C3T5M 分析系统启动: http://localhost:{PORT}")
    server.serve_forever()


if __name__ == "__main__":
    main()