import streamlit as st
import pandas as pd
from datetime import datetime
import os
import threading

from modules.config import STOCK_POOLS
from modules.data_loader import fetch_history_data, fetch_intraday_data_v2, background_prefetch_task
from modules.analysis import calculate_deviation_data, filter_deviation_data
from modules.visualization import plot_market_heatmap, plot_deviation_scatter, plot_intraday_charts
from modules.utils import add_script_run_ctx

st.set_page_config(
    page_title="A?????????",
    page_icon="?",
    layout="wide"
)

with st.sidebar:
    st.header("?? ????")

    selected_pool = st.selectbox(
        "?? ?????",
        list(STOCK_POOLS.keys()),
        index=0,
        key="sb_selected_pool"
    )

    st.markdown("---")
    st.header("?? ????")

    with st.expander("???????", expanded=True):
        st.write(f"??????: **{selected_pool}**")

        if st.button("?? ?????? (??)"):
            try:
                p_cfg = STOCK_POOLS[selected_pool]
                c_path = p_cfg["cache"]

                if os.path.exists(c_path):
                    _df = pd.read_parquet(c_path)
                    if not _df.empty:
                        _df['??'] = pd.to_datetime(_df['??'])
                    _today = datetime.now().date()
                    _df_new = _df[_df['??'].dt.date < _today]
                    _df_new.to_parquet(c_path)
                    st.toast(f"??? [{selected_pool}] ???????????...")
                else:
                    st.toast(f"[{selected_pool}] ???????????...")

                st.cache_data.clear()
                st.rerun()
            except Exception as e:
                st.error(f"????: {e}")

        if st.button("?? ???????"):
            st.cache_data.clear()
            st.toast("? ???????????????????????")

        if st.button(f"?? ?? [{selected_pool}] ????"):
            p_cfg = STOCK_POOLS[selected_pool]
            c_path = p_cfg["cache"]
            if os.path.exists(c_path):
                os.remove(c_path)
                st.toast(f"??? [{selected_pool}] ???????")
            st.cache_data.clear()
            st.rerun()

        if st.checkbox("?????? (????)"):
            if st.button("?? ?????? (???????)"):
                for p_name, p_val in STOCK_POOLS.items():
                    if os.path.exists(p_val["cache"]):
                        os.remove(p_val["cache"])
                st.cache_data.clear()
                st.rerun()

    st.markdown("---")
    st.markdown("### ??? ????")
    filter_cyb = st.checkbox("????? (300??)", value=False)
    filter_kcb = st.checkbox("????? (688??)", value=False)

    st.markdown("---")
    nav_option = st.radio(
        "?? ????",
        ["?? ????", "?? ??????"],
        index=0
    )

with st.spinner(f"????? [{selected_pool}] ??????..."):
    origin_df = fetch_history_data(selected_pool)

# --- ????????? ---
bg_thread = None
for t in threading.enumerate():
    if t.name == "PrefetchWorker":
        bg_thread = t
        break

with st.sidebar:
    st.markdown("---")
    with st.expander("?? ??????", expanded=False):
        st.caption("???????? N ?????")
        prefetch_days = st.number_input("????", min_value=5, max_value=200, value=30, step=10)

        if bg_thread and bg_thread.is_alive():
            st.info("?? ???????...\n????????")
        else:
            if st.button("?? ??????"):
                if not origin_df.empty:
                    all_dates = sorted(origin_df['??'].dt.date.unique())
                    target_prefetch_dates = all_dates[-prefetch_days:]

                    t = threading.Thread(
                        target=background_prefetch_task,
                        args=(target_prefetch_dates, origin_df),
                        name="PrefetchWorker",
                        daemon=True
                    )
                    add_script_run_ctx(t)
                    t.start()
                    st.rerun()
                else:
                    st.error("????????")

if origin_df.empty:
    st.error("?????????????")
    st.stop()

# ????
filtered_df = origin_df.copy()
if filter_cyb:
    filtered_df = filtered_df[~filtered_df['??'].astype(str).str.startswith('300')]
if filter_kcb:
    filtered_df = filtered_df[~filtered_df['??'].astype(str).str.startswith('688')]

if filtered_df.empty:
    st.warning("??????????????????????")
    st.stop()

if nav_option == "?? ????":
    st.title(f"A??????? - {selected_pool} (Market Replay)")
    st.markdown(
        "> ??? **????**?\n"
        "> 1. ?????????????????? 2-3 ????\n"
        "> 2. ?????????????\n"
        "> 3. ???????????????"
    )

    available_dates = sorted(filtered_df['??'].dt.date.unique())

    if 'selected_date_idx' not in st.session_state:
        st.session_state.selected_date_idx = len(available_dates) - 1

    if st.session_state.selected_date_idx >= len(available_dates):
        st.session_state.selected_date_idx = len(available_dates) - 1
    if st.session_state.selected_date_idx < 0:
        st.session_state.selected_date_idx = 0

    st.markdown("### ?? ??????")

    mode_col1, mode_col2 = st.columns([1, 3])
    with mode_col1:
        playback_mode = st.radio("????", ["????", "??????"], horizontal=True)

    if playback_mode == "????":
        col_prev, col_slider, col_next = st.columns([1, 6, 1])

        with col_prev:
            st.write("")
            st.write("")
            if st.button("?? ???"):
                if st.session_state.selected_date_idx > 0:
                    st.session_state.selected_date_idx -= 1
                    st.rerun()

        with col_next:
            st.write("")
            st.write("")
            if st.button("??? ??"):
                if st.session_state.selected_date_idx < len(available_dates) - 1:
                    st.session_state.selected_date_idx += 1
                    st.rerun()

        with col_slider:
            current_date_val = available_dates[st.session_state.selected_date_idx]
            picked_date = st.date_input(
                "??",
                value=current_date_val,
                min_value=available_dates[0],
                max_value=available_dates[-1],
                label_visibility="collapsed"
            )

            if picked_date != current_date_val:
                if picked_date in available_dates:
                    st.session_state.selected_date_idx = available_dates.index(picked_date)
                else:
                    closest_date = min(available_dates, key=lambda d: abs(d - picked_date))
                    st.session_state.selected_date_idx = available_dates.index(closest_date)
                    st.toast(f"?? ???????????????: {closest_date}")
                st.rerun()

        target_dates = [available_dates[st.session_state.selected_date_idx]]
        selected_date = target_dates[0]

    else:
        with mode_col2:
            date_range = st.date_input(
                "?????? (?????5????????)",
                value=[available_dates[-5] if len(available_dates) > 5 else available_dates[0], available_dates[-1]],
                min_value=available_dates[0],
                max_value=available_dates[-1]
            )

        if len(date_range) == 2:
            start_d, end_d = date_range
            target_dates = [d for d in available_dates if start_d <= d <= end_d]
            if not target_dates:
                st.warning("?? ??????????????????????")
                target_dates = [available_dates[-1]]
            st.info(f"??? {len(target_dates)} ??????????")
            selected_date = target_dates[-1]
        else:
            st.warning("?????????????")
            target_dates = [available_dates[-1]]
            selected_date = available_dates[-1]

    daily_df = filtered_df[filtered_df['??'].dt.date == selected_date].copy()

    if daily_df.empty:
        st.warning(f"{selected_date} ??????????????????????")
    else:
        median_chg = daily_df['???'].median()
        total_turnover = daily_df['???'].sum() / 1e8
        top_gainer = daily_df.loc[daily_df['???'].idxmax()]

        col1, col2, col3, col4 = st.columns(4)
        col1.metric("??????", selected_date.strftime("%Y-%m-%d"))
        col2.metric("????????", f"{median_chg:.2f}%", delta=f"{median_chg:.2f}%", delta_color="normal")
        col3.metric("??????", f"{total_turnover:.1f} ?")
        col4.metric("????", f"{top_gainer['??']} ({top_gainer['???']:.2f}%)")

        st.markdown("---")
        st.subheader("?? ??????????")

        col_mode, col_num = st.columns([3, 1])
        with col_mode:
            chart_mode = st.radio("????", ["??? Top (???)", "???? Top (????)"], horizontal=True)
        with col_num:
            top_n = st.number_input(
                "????",
                min_value=5,
                max_value=50,
                value=20,
                step=5,
                help="?/??? N ???????? 2N?",
                key="top_n_stocks_input"
            )

        st.caption(f"?????????? **{selected_date}** ?????????????????????????????????")
        st.caption("?????? = ??? ? ??(??????/????)????????????????")

        show_intraday = st.checkbox("?????? (????????)", value=False)

        if show_intraday:
            progress_area = st.empty()

            if "???" in chart_mode:
                sort_col = '???'
            else:
                daily_df['abs_impact'] = (daily_df['???'] * daily_df['???']).abs()
                sort_col = 'abs_impact'

            sh_pool = daily_df[daily_df['??'].astype(str).str.startswith('6')].copy()
            sz_pool = daily_df[~daily_df['??'].astype(str).str.startswith('6')].copy()

            sh_top = sh_pool.sort_values(sort_col, ascending=False).head(top_n)
            sz_top = sz_pool.sort_values(sort_col, ascending=False).head(top_n)

            top_stocks_df = pd.concat([sh_top, sz_top], ignore_index=True)

            target_stocks_list = []
            for _, row in top_stocks_df.iterrows():
                target_stocks_list.append((row['??'], row['??'], row['???']))

            all_intraday_data = []
            period_to_use = '1'

            if len(target_dates) > 5 and playback_mode == "??????":
                if len(target_dates) > 30:
                    period_to_use = '15'
                    st.info(f"?? ???? {len(target_dates)} ??????????15???????")
                else:
                    period_to_use = '5'
                    st.info(f"?? ???? {len(target_dates)} ??????????5???????")
            elif len(target_dates) > 10:
                st.toast(f"?? ???? {len(target_dates)} ?????????????????...")

            target_dates_to_fetch = target_dates
            total_steps = len(target_dates_to_fetch)

            status_text = st.empty()
            fetch_progress = st.progress(0)

            for i, d_date in enumerate(target_dates_to_fetch):
                status_text.text(f"?? ????: {d_date.strftime('%Y-%m-%d')} ({i+1}/{total_steps})...")
                fetch_progress.progress((i + 1) / total_steps)

                d_str = d_date.strftime("%Y-%m-%d")
                day_results = fetch_intraday_data_v2(target_stocks_list, d_str, period=period_to_use)

                for res in day_results:
                    res['data']['date_col'] = d_str
                    res['real_date'] = d_date

                all_intraday_data.extend(day_results)

            status_text.empty()
            fetch_progress.empty()
            progress_area.empty()

            if not all_intraday_data:
                st.warning("?????????")
            else:
                valid_dates = set()
                for item in all_intraday_data:
                    if 'real_date' in item:
                        valid_dates.add(item['real_date'].strftime("%Y-%m-%d"))

                days_list = sorted(list(valid_dates))
                if not days_list:
                    days_list = sorted(list(set([x.strftime("%Y-%m-%d") for x in target_dates_to_fetch])))

                fig_sh, fig_sz = plot_intraday_charts(all_intraday_data, days_list, daily_df, chart_mode)

                tab1, tab2 = st.tabs(["?? (SH)", "?? (SZ)"])
                with tab1:
                    if fig_sh:
                        st.plotly_chart(fig_sh, use_container_width=True)
                with tab2:
                    if fig_sz:
                        st.plotly_chart(fig_sz, use_container_width=True)

        st.subheader(f"?? {selected_date.strftime('%Y?%m?%d?')} ???????")
        fig = plot_market_heatmap(daily_df)
        st.plotly_chart(fig, use_container_width=True)

        with st.expander("????????"):
            st.dataframe(
                daily_df[['??', '??', '??', '???', '???']].style.format({
                    '??': '{:.2f}',
                    '???': '{:.2f}%',
                    '???': '{:,.0f}'
                }),
                hide_index=True
            )

elif nav_option == "?? ??????":
    st.subheader("?? ??????? (Alpha Divergence)")
    st.info("?? **????**??????????????????????????????????\n\n?????? **?????** ? **??????**?????????????????????????")

    available_dates = sorted(filtered_df['??'].dt.date.unique())
    col_d1, col_d2 = st.columns(2)
    with col_d1:
        date_range_div = st.date_input(
            "????",
            value=[available_dates[-5] if len(available_dates) > 5 else available_dates[0], available_dates[-1]],
            min_value=available_dates[0],
            max_value=available_dates[-1],
            key="divergence_date_input"
        )

    target_dates_div = []
    if len(date_range_div) == 2:
        s_d, e_d = date_range_div
        target_dates_div = [d for d in available_dates if s_d <= d <= e_d]

    if not target_dates_div:
        st.warning("??????????")
        st.stop()

    st.caption(f"??? {target_dates_div[0]} ? {target_dates_div[-1]}?? {len(target_dates_div)} ?????")

    div_df, market_median_chg = calculate_deviation_data(filtered_df, target_dates_div)

    if div_df.empty:
        st.stop()

    st.markdown("### ?? ????")
    strategy_mode = st.radio(
        "??????",
        ["?? (????)", "??? ??/?? (????)", "?? ??/?? (???/??)", "?? ??/?? (????)"],
        horizontal=True
    )

    filtered_div = filter_deviation_data(div_df, strategy_mode=strategy_mode)

    col_m1, col_m2 = st.columns(2)
    col_m1.metric("??(???)???", f"{market_median_chg:.2f}%")
    col_m2.metric("????????", f"{len(filtered_div)} ?")

    st.divider()

    if not filtered_div.empty:
        fig_scatter = plot_deviation_scatter(filtered_div, strategy_mode)
        if fig_scatter:
            st.plotly_chart(fig_scatter, use_container_width=True)
        else:
            st.warning("??????????????")
    else:
        st.warning("??????????????")

    col_list1, col_list2 = st.columns(2)

    with col_list1:
        st.subheader("?? ???? (??????)")
        buy_df = filtered_div[filtered_div['???'] > 0].sort_values('?????', ascending=False).head(20)
        st.dataframe(
            buy_df[['??', '??', '???', '???(?)', '?????']].style.format({
                '???': '+{:.2f}%',
                '???(?)': '{:.1f}',
                '?????': '{:.2f}%'
            }),
            hide_index=True
        )

    with col_list2:
        st.subheader("?? ???? (??????)")
        sell_df = filtered_div[filtered_div['???'] < 0].sort_values('?????', ascending=False).head(20)
        st.dataframe(
            sell_df[['??', '??', '???', '???(?)', '?????']].style.format({
                '???': '{:.2f}%',
                '???(?)': '{:.1f}',
                '?????': '{:.2f}%'
            }),
            hide_index=True
        )
