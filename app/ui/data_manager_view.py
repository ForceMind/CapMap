from datetime import datetime

import streamlit as st

from core.data_access import (
    DEFAULT_MIN_PERIOD,
    _get_cached_codes_for_date,
    _get_daily_codes,
    _scan_cached_dates,
    _start_manual_prefetch,
)


def render_data_manager(origin_df):
    st.subheader("📦 本地分时数据管理")
    st.caption("说明：只显示 1分钟分时缓存，可手动补齐缺失。")
    if origin_df is None or origin_df.empty:
        st.warning("暂无历史数据，请先初始化。")
    else:
        trading_dates = sorted(origin_df['日期'].dt.date.unique())
        date_strs = [d.strftime("%Y-%m-%d") for d in trading_dates]
        selected_date_str = st.selectbox("选择交易日期", date_strs, index=len(date_strs) - 1)
        selected_date = datetime.strptime(selected_date_str, "%Y-%m-%d").date()
        date_key = selected_date_str.replace("-", "")
        codes, name_map = _get_daily_codes(origin_df, selected_date)
        st.write(f"当日成分股数量: {len(codes)}")

        cached_codes = _get_cached_codes_for_date(date_key, codes, period=DEFAULT_MIN_PERIOD, is_index=False)
        missing_codes = [c for c in codes if c not in cached_codes]
        st.write(f"股票分时缓存: {len(cached_codes)}/{len(codes)}")
        if missing_codes:
            missing_lines = [f"{c} {name_map.get(c, c)}" for c in missing_codes]
            st.text_area("缺失股票", "\n".join(missing_lines), height=160)
        else:
            st.success("股票分时已完整。")

        index_codes = ["000300", "000001", "399001"]
        cached_idx = _get_cached_codes_for_date(date_key, index_codes, period=DEFAULT_MIN_PERIOD, is_index=True)
        missing_idx = [c for c in index_codes if c not in cached_idx]
        st.write(f"指数分时缓存: {len(cached_idx)}/{len(index_codes)}")
        if missing_idx:
            st.warning("缺失指数: " + ", ".join(missing_idx))
        else:
            st.success("指数分时已完整。")

        st.markdown("### 手动补齐")
        include_indices = st.checkbox("包含指数", value=True)
        default_codes = missing_codes[:20]
        select_codes = st.multiselect("选择要补齐的股票(缺失)", options=missing_codes, default=default_codes)
        if st.button("启动后台补齐"):
            started = _start_manual_prefetch(selected_date_str, select_codes, name_map, include_indices=include_indices)
            if started:
                st.info("已启动后台补齐，请查看 logs/app.log。")
            else:
                st.warning("没有可补齐的任务。")

        with st.expander("本地分时缓存日期"):
            cached_dates = _scan_cached_dates(period=DEFAULT_MIN_PERIOD, is_index=False)
            if cached_dates:
                readable = [d[:4] + '-' + d[4:6] + '-' + d[6:] for d in cached_dates]
                st.text_area("缓存日期", "\n".join(readable), height=120)
            else:
                st.write("暂无本地分时缓存。")
