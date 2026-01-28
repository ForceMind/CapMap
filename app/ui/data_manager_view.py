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
    st.subheader("📦 本地数据管理与概览")
    
    tab1, tab2 = st.tabs(["📅 日线缓存概览", "⏱️ 分时数据详情"])
    
    if origin_df is None or origin_df.empty:
        st.warning("暂无历史数据，请先初始化。")
        return

    all_dates = sorted(origin_df['日期'].dt.date.unique())

    with tab1:
        st.metric("总交易天数", len(all_dates))
        st.write(f"时间跨度: {all_dates[0]} 至 {all_dates[-1]}")
        
        # 简单的热力图或列表显示缺失情况 (假设 origin_df 是连续拉取的，
        # 如果中间有断层，可以通过 date_range 对比)
        # 这里主要展示是否有某天数据量异常 (比如只有几百只股票)
        
        # 统计每天的股票数量
        daily_counts = origin_df.groupby(origin_df['日期'].dt.date).size().reset_index(name='count')
        
        # 找出数量较少的天 (可能数据不全)
        threshold = 200 # 假设少于200只认为异常
        suspicious_days = daily_counts[daily_counts['count'] < threshold]
        
        if not suspicious_days.empty:
            st.error(f"发现 {len(suspicious_days)} 个交易日数据量异常偏低 (V2 repair enabled):")
            st.dataframe(suspicious_days)
        else:
            st.success("日线数据覆盖看起来正常 (每天 > 200 只股票).")

        st.line_chart(daily_counts.set_index('日期'))

    with tab2:
        st.caption("分时数据 (Minutes) 缓存覆盖率查询")
        
        col_d1, col_d2 = st.columns([1, 2])
        with col_d1:
            date_strs = [d.strftime("%Y-%m-%d") for d in all_dates]
            # 默认选最近一天
            selected_date_str = st.selectbox("选择交易日期", date_strs, index=len(date_strs) - 1)
        
        selected_date = datetime.strptime(selected_date_str, "%Y-%m-%d").date()
        date_key = selected_date_str.replace("-", "")
        
        # 获取当日应有的股票
        codes, name_map = _get_daily_codes(origin_df, selected_date)
        
        # 获取实际缓存
        cached_codes = _get_cached_codes_for_date(date_key, codes, period=DEFAULT_MIN_PERIOD, is_index=False)
        
        # 指数缓存
        indices = ["000300", "000001", "399001", "000905", "000852"]
        cached_indices = _get_cached_codes_for_date(date_key, indices, period=DEFAULT_MIN_PERIOD, is_index=True)
        
        with col_d2:
            st.write(f"### {selected_date_str}")
            c1, c2 = st.columns(2)
            c1.metric("指数覆盖", f"{len(cached_indices)} / {len(indices)}")
            c2.metric("个股覆盖", f"{len(cached_codes)} / {len(codes)}")
            
            if len(cached_codes) < len(codes):
                st.progress(len(cached_codes) / len(codes))
            else:
                st.progress(1.0)
        
        st.divider()
        
        miss_col, exist_col = st.columns(2)
        with miss_col:
            missing_codes = sorted(list(set(codes) - set(cached_codes)))
            st.warning(f"缺失股票 ({len(missing_codes)})")
            if missing_codes:
                st.text_area("缺失代码列表", ",".join(missing_codes), height=150)
                if st.button("🚀 仅补全缺失数据"):
                    _start_manual_prefetch([selected_date], origin_df)
        
        with exist_col:
             st.success(f"已缓存股票 ({len(cached_codes)})")
             st.text_area("已缓存代码预览", ",".join(list(cached_codes)[:500]) + ("..." if len(cached_codes)>500 else ""), height=150)

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
