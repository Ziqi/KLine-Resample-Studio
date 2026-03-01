#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import time
import glob
import datetime
import threading
import pandas as pd
import tkinter as tk
from tkinter import messagebox, filedialog
import ttkbootstrap as ttk
from ttkbootstrap.constants import *
from pathlib import Path
from concurrent.futures import ProcessPoolExecutor

# ================= Engine Node =================
def process_single_file(input_file, out_dir, start_dt, end_dt):
    try:
        df = pd.read_csv(input_file)
        if df.empty: return (False, os.path.basename(input_file), "晶体碎裂: 实体为空")
        
        df['dt'] = pd.to_datetime(df['dt'])
        
        # 切片截断
        if start_dt and end_dt:
            df = df[(df['dt'] >= start_dt) & (df['dt'] <= end_dt)]
            if df.empty: return (False, os.path.basename(input_file), "过滤抛弃: 指定时间视野内无数据")
            
        df.set_index('dt', inplace=True)
        
        # 五分钟合成法则
        agg_dict = {
            'open': 'first',
            'high': 'max',
            'low': 'min',
            'close': 'last',
            'volume': 'sum',
            'amount': 'sum'
        }
        
        # a-share standard logic
        df_5m = df.resample('5T', closed='right', label='right').agg(agg_dict)
        df_5m.dropna(subset=['open'], inplace=True)
        
        df_5m.reset_index(inplace=True)
        df_5m.rename(columns={'dt': 'timestamps'}, inplace=True)
        
        cols = ['timestamps', 'open', 'high', 'low', 'close', 'volume', 'amount']
        df_5m = df_5m[cols]
        
        base_name = os.path.basename(input_file).replace("_1m_", "_5m_")
        out_path = os.path.join(out_dir, base_name)
        df_5m.to_csv(out_path, index=False)
        
        return (True, base_name, f"降维成功 [{len(df_5m)} 条]")
        
    except Exception as e:
        return (False, os.path.basename(input_file), f"解构崩溃: {str(e)}")

# ================= UI Components =================

class DashFrame(tk.Frame):
    def __init__(self, master, title, bg_color, fg_color, dash_color, font, *args, **kwargs):
        super().__init__(master, bg=bg_color, *args, **kwargs)
        self.bg_color = bg_color
        self.dash_color = dash_color
        
        self.canvas = tk.Canvas(self, bg=bg_color, highlightthickness=0)
        self.canvas.place(relx=0, rely=0, relwidth=1, relheight=1)
        
        self.content = tk.Frame(self, bg=bg_color)
        self.content.pack(fill=BOTH, expand=True, padx=12, pady=(25, 12)) 
        
        self.bind("<Configure>", self._draw)
        
        self.title_text = title
        self.fg_color = fg_color
        self.font = font
        
    def _draw(self, event=None):
        self.canvas.delete("all")
        w, h = self.winfo_width(), self.winfo_height()
        if w < 10 or h < 10: return
        
        self.canvas.create_rectangle(2, 10, w-2, h-2, outline=self.dash_color, dash=(5, 5))
        self.canvas.create_rectangle(15, 0, 15 + len(self.title_text)*10, 20, fill=self.bg_color, outline="")
        self.canvas.create_text(20, 10, text=self.title_text, anchor="w", font=self.font, fill=self.fg_color)

def bind_auto_scrollbar(container, scrollbar, side, fill):
    def check_mouse_leave(e):
        x, y = container.winfo_pointerxy()
        cx, cy = container.winfo_rootx(), container.winfo_rooty()
        cw, ch = container.winfo_width(), container.winfo_height()
        if not (cx <= x <= cx + cw and cy <= y <= cy + ch):
            scrollbar.pack_forget()

    def on_enter(e):
        scrollbar.pack(side=side, fill=fill, before=container.winfo_children()[0] if side in [tk.BOTTOM, tk.TOP] else None)
        
    def on_leave(e):
        container.after(100, check_mouse_leave, e)
        
    container.bind("<Enter>", on_enter)
    container.bind("<Leave>", on_leave)


# ================= Main Window =================

class ResampleMatrixGUI(ttk.Window):
    def __init__(self):
        super().__init__(themename="cyborg")
        self.title("全市场 K线数据转换器 (1分钟 转 5分钟)")
        self.geometry("1100x860")
        self.minsize(1050, 800)
        
        # Theme Colors (Flat Dark Gold)
        self.c_bg = "#080808"
        self.c_panel = "#101010"
        self.c_gold = "#F0B90B"
        self.c_gold_dim = "#715A2B"
        self.c_fg = "#E1C699"
        self.c_green = "#00D47C"
        self.c_red = "#FF3B30"
        
        self.font_title = ("Menlo", 36, "bold")
        self.font_base = ("Menlo", 14)
        self.font_base_lg = ("Menlo", 16)
        self.font_log = ("Menlo", 13)
        
        self._setup_styles()
        
        # Cross-directory absolute path resolution
        base_path = Path(__file__).resolve().parent.parent
        self.source_dir = base_path / "1-KLine-Extract" / "gui_downloads"
        self.target_dir = base_path / "2-KLine-Resample" / "gui_out_5m"
        self.target_dir.mkdir(parents=True, exist_ok=True)
        
        # Runtime tracking
        self.process_thread = None
        self.stop_requested = False
        self._target_mapping = {}
        
        self.setup_ui()
        self.poll_source_dir()
        self.poll_target_dir()

    def _setup_styles(self):
        style = ttk.Style()
        style.configure(".", font=self.font_base, background=self.c_bg, foreground=self.c_fg)
        
        style.configure("TLabelframe", background=self.c_bg, bordercolor=self.c_gold_dim, borderwidth=1, relief="solid")
        style.configure("TLabelframe.Label", font=("Menlo", 15, "bold"), foreground=self.c_gold, background=self.c_bg)
        
        style.configure("FlatGold.TButton", font=self.font_base_lg, background=self.c_bg, foreground=self.c_gold, bordercolor=self.c_gold, borderwidth=1)
        style.map("FlatGold.TButton", background=[("active", "#1A140B")], foreground=[("active", "#FFD700")])
        style.configure("FlatRed.TButton", font=self.font_base_lg, background=self.c_bg, foreground=self.c_red, bordercolor=self.c_red, borderwidth=1)
        style.map("FlatRed.TButton", background=[("active", "#1A0505")])
        
        style.layout('Hidden.Vertical.TScrollbar', [('Vertical.Scrollbar.trough', {'children': [('Vertical.Scrollbar.thumb', {'expand': '1', 'sticky': 'nswe'})], 'sticky': 'ns'})])
        style.configure("Hidden.Vertical.TScrollbar", background=self.c_gold_dim, troughcolor=self.c_bg, bordercolor=self.c_bg, relief="flat")
        style.map("Hidden.Vertical.TScrollbar", background=[("active", self.c_gold)])
        
        style.layout('Hidden.Horizontal.TScrollbar', [('Horizontal.Scrollbar.trough', {'children': [('Horizontal.Scrollbar.thumb', {'expand': '1', 'sticky': 'nswe'})], 'sticky': 'we'})])
        style.configure("Hidden.Horizontal.TScrollbar", background=self.c_gold_dim, troughcolor=self.c_bg, bordercolor=self.c_bg, relief="flat")
        style.map("Hidden.Horizontal.TScrollbar", background=[("active", self.c_gold)])
        
        style.configure("Treeview", background=self.c_panel, foreground=self.c_fg, fieldbackground=self.c_panel, borderwidth=0, font=self.font_base, rowheight=32)
        style.map("Treeview", background=[("selected", "#2A2111")], foreground=[("selected", self.c_gold)])
        style.configure("Treeview.Heading", font=("Menlo", 13, "bold"), background=self.c_bg, foreground=self.c_gold, borderwidth=1)

    def setup_ui(self):
        self.configure(bg=self.c_bg)
        
        self.lift()
        self.attributes('-topmost', True)
        self.after(1000, lambda: self.attributes('-topmost', False))
        os.system('''osascript -e 'tell application "System Events" to set frontmost of the first process whose unix id is %d to true' ''' % os.getpid())
        
        # --- HEADER ---
        header_frame = tk.Frame(self, bg=self.c_bg, pady=15)
        header_frame.pack(fill=X, padx=20)
        tk.Label(header_frame, text="全市场 K线数据转换器 (1分钟 ▷ 5分钟)", font=self.font_title, fg=self.c_gold, bg=self.c_bg).pack(side=LEFT)
        self.status_sign = tk.Label(header_frame, text="系统就绪", font=("Menlo", 16, "bold"), fg=self.c_gold_dim, bg=self.c_bg)
        self.status_sign.pack(side=RIGHT, anchor=S)

        # --- BODY ---
        body_frame = tk.Frame(self, bg=self.c_bg)
        body_frame.pack(fill=BOTH, expand=True, padx=20, pady=(0, 20))
        
        # --- LEFT PANEL: CONTROLS ---
        left_panel = tk.Frame(body_frame, width=400, bg=self.c_bg)
        left_panel.pack(side=LEFT, fill=Y, padx=(0, 20))
        left_panel.pack_propagate(False)
        
        # 1. Directory Config
        dir_lf = DashFrame(left_panel, title=" 文件夹路径设置 ", bg_color=self.c_bg, fg_color=self.c_gold, dash_color=self.c_gold_dim, font=("Menlo", 15, "bold"))
        dir_lf.pack(fill=X, pady=(0, 15))
        
        tk.Label(dir_lf.content, text="1分钟K线来源夹:", font=self.font_base, fg=self.c_fg, bg=self.c_bg).pack(anchor=W)
        self.src_var = tk.StringVar(value=str(self.source_dir))
        src_fr = tk.Frame(dir_lf.content, bg=self.c_bg)
        src_fr.pack(fill=X, pady=(2, 10))
        tk.Entry(src_fr, textvariable=self.src_var, font=self.font_log, bg=self.c_panel, fg=self.c_gold, relief="flat", highlightthickness=1, highlightbackground=self.c_gold_dim).pack(side=LEFT, fill=X, expand=True)
        ttk.Button(src_fr, text="打开", style="FlatGold.TButton", command=self.on_browse_src).pack(side=RIGHT, padx=(5,0))
        
        tk.Label(dir_lf.content, text="5分钟K线输出夹:", font=self.font_base, fg=self.c_fg, bg=self.c_bg).pack(anchor=W)
        self.tgt_var = tk.StringVar(value=str(self.target_dir))
        tgt_fr = tk.Frame(dir_lf.content, bg=self.c_bg)
        tgt_fr.pack(fill=X, pady=(2, 5))
        tk.Entry(tgt_fr, textvariable=self.tgt_var, font=self.font_log, bg=self.c_panel, fg=self.c_green, relief="flat", highlightthickness=1, highlightbackground=self.c_gold_dim).pack(side=LEFT, fill=X, expand=True)
        ttk.Button(tgt_fr, text="打开", style="FlatGold.TButton", command=self.on_browse_tgt).pack(side=RIGHT, padx=(5,0))

        # 2. Date Range
        date_lf = DashFrame(left_panel, title=" 时间范围过滤 ", bg_color=self.c_bg, fg_color=self.c_gold, dash_color=self.c_gold_dim, font=("Menlo", 15, "bold"))
        date_lf.pack(fill=X, pady=(0, 15))
        
        self.all_history_var = tk.BooleanVar(value=True)
        self.all_history_cb = tk.Checkbutton(date_lf.content, text="全量转换 (忽略时间范围)", variable=self.all_history_var, command=self.toggle_dates, font=self.font_base, fg=self.c_gold, bg=self.c_bg, selectcolor=self.c_panel, activebackground=self.c_bg, activeforeground=self.c_gold)
        self.all_history_cb.pack(fill=X, anchor=W, pady=(0, 15))
        
        self.date_pickers_frame = tk.Frame(date_lf.content, bg=self.c_bg)
        self.date_pickers_frame.pack(fill=X)
        
        s_frame = tk.Frame(self.date_pickers_frame, bg=self.c_bg)
        s_frame.pack(fill=X, pady=(0, 10))
        tk.Label(s_frame, text="开始", width=4, font=self.font_base, bg=self.c_bg, fg=self.c_fg).pack(side=LEFT)
        start_date = datetime.datetime.now() - datetime.timedelta(days=365)
        self.start_y = ttk.Combobox(s_frame, values=[str(y) for y in range(1990, 2030)], width=5, font=self.font_base)
        self.start_y.set(start_date.strftime("%Y"))
        self.start_y.pack(side=LEFT, padx=2)
        self.start_m = ttk.Combobox(s_frame, values=[f"{m:02d}" for m in range(1, 13)], width=3, font=self.font_base)
        self.start_m.set(start_date.strftime("%m"))
        self.start_m.pack(side=LEFT, padx=2)
        self.start_d = ttk.Combobox(s_frame, values=[f"{d:02d}" for d in range(1, 32)], width=3, font=self.font_base)
        self.start_d.set(start_date.strftime("%d"))
        self.start_d.pack(side=LEFT, padx=2)
        
        e_frame = tk.Frame(self.date_pickers_frame, bg=self.c_bg)
        e_frame.pack(fill=X)
        tk.Label(e_frame, text="结束", width=4, font=self.font_base, bg=self.c_bg, fg=self.c_fg).pack(side=LEFT)
        end_date = datetime.datetime.now()
        self.end_y = ttk.Combobox(e_frame, values=[str(y) for y in range(1990, 2030)], width=5, font=self.font_base)
        self.end_y.set(end_date.strftime("%Y"))
        self.end_y.pack(side=LEFT, padx=2)
        self.end_m = ttk.Combobox(e_frame, values=[f"{m:02d}" for m in range(1, 13)], width=3, font=self.font_base)
        self.end_m.set(end_date.strftime("%m"))
        self.end_m.pack(side=LEFT, padx=2)
        self.end_d = ttk.Combobox(e_frame, values=[f"{d:02d}" for d in range(1, 32)], width=3, font=self.font_base)
        self.end_d.set(end_date.strftime("%d"))
        self.end_d.pack(side=LEFT, padx=2)
        
        self.toggle_dates()

        # 3. Action Controls
        ctrl_lf = DashFrame(left_panel, title=" 操作面板 ", bg_color=self.c_bg, fg_color=self.c_gold, dash_color=self.c_gold_dim, font=("Menlo", 15, "bold"))
        ctrl_lf.pack(fill=X, pady=(0, 15))

        self.start_btn = ttk.Button(ctrl_lf.content, text="开始转换 (1m -> 5m)", style="FlatGold.TButton", command=self.on_start_click)
        self.start_btn.pack(fill=X, pady=(15, 10), ipady=5)
        
        self.stop_btn = ttk.Button(ctrl_lf.content, text="停止转换", style="FlatRed.TButton", command=self.on_stop_click, state=DISABLED)
        self.stop_btn.pack(fill=X, pady=(0, 5), ipady=5)

        # 4. Console Stream (moved to Left Panel)
        console_lf = DashFrame(left_panel, title=" 运行日志 ", bg_color=self.c_bg, fg_color=self.c_gold, dash_color=self.c_gold_dim, font=("Menlo", 15, "bold"))
        console_lf.pack(fill=BOTH, expand=True)
        
        prog_frame = tk.Frame(console_lf.content, bg=self.c_bg)
        prog_frame.pack(fill=X, pady=(0, 5))
        
        self.prog_lbl = tk.Label(prog_frame, text="待命：准备转换", font=self.font_base_lg, fg=self.c_gold, bg=self.c_bg)
        self.prog_lbl.pack(anchor=W)
        
        prog_bar_border = tk.Frame(console_lf.content, bg=self.c_gold_dim, height=4)
        prog_bar_border.pack(fill=X, expand=False, padx=2)
        self.prog_bar_fill = tk.Frame(prog_bar_border, bg=self.c_gold, width=0, height=4)
        self.prog_bar_fill.pack(side=LEFT)
        self.prog_bar_border = prog_bar_border
        
        txt_frame = tk.Frame(console_lf.content, bg=self.c_panel)
        txt_frame.pack(fill=BOTH, expand=True, pady=(5, 0), padx=5)
        
        self.log_widget = tk.Text(txt_frame, font=self.font_log, bg=self.c_panel, fg=self.c_fg, insertbackground=self.c_fg, wrap=tk.WORD, borderwidth=0, highlightthickness=0, spacing1=4, spacing3=4)
        txt_scroll = ttk.Scrollbar(txt_frame, orient=tk.VERTICAL, command=self.log_widget.yview, style="Hidden.Vertical.TScrollbar")
        self.log_widget.configure(yscrollcommand=txt_scroll.set)
        
        self.log_widget.pack(side=LEFT, fill=BOTH, expand=True)
        bind_auto_scrollbar(txt_frame, txt_scroll, tk.RIGHT, tk.Y)
        
        self.log_widget.tag_config("info", foreground=self.c_fg)
        self.log_widget.tag_config("warn", foreground="#FF9800", font=("Menlo", 13, "bold"))
        self.log_widget.tag_config("err", foreground=self.c_red, font=("Menlo", 13, "bold"))
        self.log_widget.tag_config("succ", foreground=self.c_green, font=("Menlo", 13, "bold"))
        self.log_widget.tag_config("sys", foreground=self.c_gold, font=("Menlo", 13, "bold"))
        self.log_widget.configure(state='disabled')

        # --- RIGHT PANEL ---
        right_panel = tk.Frame(body_frame, bg=self.c_bg)
        right_panel.pack(side=LEFT, fill=BOTH, expand=True)
        
        # === 1. TOP: Source files list ===
        assets_lf = DashFrame(right_panel, title=" 1分钟K线源文件列表 (请勾选需要转换的文件) ", bg_color=self.c_bg, fg_color=self.c_gold, dash_color=self.c_gold_dim, font=("Menlo", 15, "bold"))
        assets_lf.pack(side=TOP, fill=BOTH, expand=True, pady=(0, 10))
        
        # Select All / Unselect All ToolBar
        tb = tk.Frame(assets_lf.content, bg=self.c_bg)
        tb.pack(fill=X, pady=(0, 5))
        ttk.Button(tb, text="[ 全选 ]", style="FlatGold.TButton", command=self.on_select_all).pack(side=LEFT, padx=(0,5))
        ttk.Button(tb, text="[ 取消 ]", style="FlatGold.TButton", command=self.on_unselect_all).pack(side=LEFT)
        
        columns = ("name", "code", "start", "end", "size", "delete")
        tree_container = tk.Frame(assets_lf.content, bg=self.c_bg)
        tree_container.pack(fill=BOTH, expand=True)
        
        self.tree = ttk.Treeview(tree_container, columns=columns, show="headings", selectmode="extended")
        self.tree.heading("name", text="股票名称")
        self.tree.heading("code", text="代码")
        self.tree.heading("start", text="开始日期")
        self.tree.heading("end", text="结束日期")
        self.tree.heading("size", text="文件大小")
        self.tree.heading("delete", text="[ 删除 ]")
        self.tree.column("name", width=120, anchor=tk.W)
        self.tree.column("code", width=100, anchor=tk.W)
        self.tree.column("start", width=110, anchor=tk.CENTER)
        self.tree.column("end", width=110, anchor=tk.CENTER)
        self.tree.column("size", width=100, anchor=tk.E)
        self.tree.column("delete", width=70, anchor=tk.CENTER)
        
        tree_yscroll = ttk.Scrollbar(tree_container, orient=tk.VERTICAL, command=self.tree.yview, style="Hidden.Vertical.TScrollbar")
        self.tree.configure(yscrollcommand=tree_yscroll.set)
        
        self.tree.pack(side=LEFT, fill=BOTH, expand=True)
        bind_auto_scrollbar(tree_container, tree_yscroll, tk.RIGHT, tk.Y)
        
        self.tree.bind('<ButtonRelease-1>', self.on_src_tree_click)
        self.tree.bind('<Delete>', self.on_delete_src)
        self.tree.bind('<BackSpace>', self.on_delete_src)
        
        # === 2. BOTTOM: Target files list (The output archive) ===
        target_lf = DashFrame(right_panel, title=" 5分钟K线输出结果 ", bg_color=self.c_bg, fg_color=self.c_green, dash_color=self.c_gold_dim, font=("Menlo", 15, "bold"))
        target_lf.pack(side=TOP, fill=BOTH, expand=True)
        
        tgt_columns = ("name", "code", "start", "end", "size", "open", "delete")
        tgt_container = tk.Frame(target_lf.content, bg=self.c_bg)
        tgt_container.pack(fill=BOTH, expand=True)
        
        self.tgt_tree = ttk.Treeview(tgt_container, columns=tgt_columns, show="headings", selectmode="browse")
        self.tgt_tree.heading("name", text="股票名称")
        self.tgt_tree.heading("code", text="代码")
        self.tgt_tree.heading("start", text="开始日期")
        self.tgt_tree.heading("end", text="结束日期")
        self.tgt_tree.heading("size", text="文件大小")
        self.tgt_tree.heading("open", text="[ 打开 ]")
        self.tgt_tree.heading("delete", text="[ 删除 ]")
        self.tgt_tree.column("name", width=120, anchor=tk.W)
        self.tgt_tree.column("code", width=100, anchor=tk.W)
        self.tgt_tree.column("start", width=100, anchor=tk.CENTER)
        self.tgt_tree.column("end", width=100, anchor=tk.CENTER)
        self.tgt_tree.column("size", width=100, anchor=tk.E)
        self.tgt_tree.column("open", width=70, anchor=tk.CENTER)
        self.tgt_tree.column("delete", width=70, anchor=tk.CENTER)
        
        tgt_yscroll = ttk.Scrollbar(tgt_container, orient=tk.VERTICAL, command=self.tgt_tree.yview, style="Hidden.Vertical.TScrollbar")
        self.tgt_tree.configure(yscrollcommand=tgt_yscroll.set)
        
        self.tgt_tree.pack(side=LEFT, fill=BOTH, expand=True)
        bind_auto_scrollbar(tgt_container, tgt_yscroll, tk.RIGHT, tk.Y)
        
        # Bindings for opening output files
        self.tgt_tree.bind('<ButtonRelease-1>', self.on_tgt_tree_click)
        self.tgt_tree.bind('<Delete>', self.on_delete_tgt)
        self.tgt_tree.bind('<BackSpace>', self.on_delete_tgt)

    def toggle_dates(self):
        st = tk.DISABLED if self.all_history_var.get() else tk.NORMAL
        self.start_y.config(state=st)
        self.start_m.config(state=st)
        self.start_d.config(state=st)
        self.end_y.config(state=st)
        self.end_m.config(state=st)
        self.end_d.config(state=st)

    def log_msg(self, msg, level="info"):
        self.log_widget.configure(state='normal')
        ts = datetime.datetime.now().strftime("%H:%M:%S")
        self.log_widget.insert(END, f"[{ts}] ", "sys")
        self.log_widget.insert(END, msg + "\n", level)
        self.log_widget.see(END)
        self.log_widget.configure(state='disabled')

    def set_progress(self, percent):
        total_w = self.prog_bar_border.winfo_width()
        if total_w <= 1: total_w = 600
        w = int((percent / 100.0) * total_w)
        self.prog_bar_fill.config(width=w)

    def on_browse_src(self):
        d = filedialog.askdirectory(initialdir=self.src_var.get())
        if d:
            self.src_var.set(d)
            self.poll_source_dir()

    def on_browse_tgt(self):
        d = filedialog.askdirectory(initialdir=self.tgt_var.get())
        if d: 
            self.tgt_var.set(d)
            self.poll_target_dir()

    def on_select_all(self):
        self.tree.selection_set(self.tree.get_children())

    def on_unselect_all(self):
        self.tree.selection_remove(self.tree.get_children())

    def poll_source_dir(self):
        p = Path(self.src_var.get())
        if not p.exists() or not p.is_dir():
            return
            
        current_iids = self.tree.get_children()
        for iid in current_iids:
            self.tree.delete(iid)
            
        self._file_mapping = {}
        import re
        files = sorted(p.glob("*_1m_*.csv"), key=os.path.getmtime, reverse=True)
        for f in files:
            size_kb = f.stat().st_size / 1024.0
            size_str = f"{size_kb:.1f} KB"
            m = re.match(r"^(.*)_(.*)_1m_(\d{8})_to_(\d{8})\.csv$", f.name)
            if m:
                name, code, start_d, end_d = m.groups()
                s_fmt = f"{start_d[:4]}-{start_d[4:6]}-{start_d[6:]}"
                e_fmt = f"{end_d[:4]}-{end_d[4:6]}-{end_d[6:]}"
                iid = self.tree.insert("", END, values=(name, code, s_fmt, e_fmt, size_str, "[ 删除 ]"))
            else:
                iid = self.tree.insert("", END, values=(f.name, "N/A", "N/A", "N/A", size_str, "[ 删除 ]"))
            self._file_mapping[iid] = f.name
            
        self.on_select_all()

    def poll_target_dir(self):
        p = Path(self.tgt_var.get())
        if not p.exists() or not p.is_dir():
            return
            
        current_iids = self.tgt_tree.get_children()
        current_files = [self._target_mapping.get(iid) for iid in current_iids]
            
        import re
        files = sorted(p.glob("*_5m_*.csv"), key=os.path.getmtime, reverse=True)
        new_files = [f.name for f in files]
        
        if current_files != new_files:
            for iid in current_iids:
                self.tgt_tree.delete(iid)
                
            self._target_mapping.clear()
            for f in files:
                size_kb = f.stat().st_size / 1024.0
                size_str = f"{size_kb:.1f} KB"
                m = re.match(r"^(.*)_(.*)_5m_(\d{8})_to_(\d{8})\.csv$", f.name)
                if m:
                    name, code, start_d, end_d = m.groups()
                    s_fmt = f"{start_d[:4]}-{start_d[4:6]}-{start_d[6:]}"
                    e_fmt = f"{end_d[:4]}-{end_d[4:6]}-{end_d[6:]}"
                    iid = self.tgt_tree.insert("", END, values=(name, code, s_fmt, e_fmt, size_str, "[ 打开 ]", "[ 删除 ]"))
                else:
                    iid = self.tgt_tree.insert("", END, values=(f.name, "N/A", "N/A", "N/A", size_str, "[ 打开 ]", "[ 删除 ]"))
                self._target_mapping[iid] = f.name
                
        self.after(2000, self.poll_target_dir)

    def on_tgt_tree_click(self, event):
        region = self.tgt_tree.identify("region", event.x, event.y)
        if region == "cell":
            col = self.tgt_tree.identify_column(event.x)
            iid = self.tgt_tree.identify_row(event.y)
            if not iid: return
            
            filename = self._target_mapping.get(iid)
            if not filename: return
            filepath = os.path.join(self.tgt_var.get(), filename)
            
            if col == '#6': # open locator
                if os.path.exists(filepath):
                    os.system(f"open -R '{os.path.abspath(filepath)}'")
            elif col == '#7': # delete
                if messagebox.askyesno("清理确认", f"确定在本地磁盘彻底销毁以下资产吗？\n\n{filename}"):
                    try:
                        if os.path.exists(filepath):
                            os.remove(filepath)
                            self.log_msg(f"数据已物理销毁: {filename}", "succ")
                            self.tgt_tree.delete(iid)
                    except Exception as e:
                        messagebox.showerror("删除失败", str(e))

    def on_src_tree_click(self, event):
        region = self.tree.identify("region", event.x, event.y)
        if region == "cell":
            col = self.tree.identify_column(event.x)
            iid = self.tree.identify_row(event.y)
            if not iid: return
            
            filename = self._file_mapping.get(iid)
            if not filename: return
            filepath = os.path.join(self.src_var.get(), filename)
            
            if col == '#6': # delete
                if messagebox.askyesno("清理确认", f"确定在本地磁盘彻底销毁底层源文件吗？\n\n{filename}"):
                    try:
                        if os.path.exists(filepath):
                            os.remove(filepath)
                            self.log_msg(f"源数据已物理销毁: {filename}", "sys")
                            self.tree.delete(iid)
                    except Exception as e:
                        messagebox.showerror("删除失败", str(e))

    def on_delete_src(self, event):
        selection = self.tree.selection()
        if not selection: return
        
        if len(selection) == 1:
            iid = selection[0]
            filename = getattr(self, '_file_mapping', {}).get(iid)
            if filename:
                filepath = os.path.join(self.src_var.get(), filename)
                if messagebox.askyesno("清理确认", f"确定删除源文件吗？\n\n{filename}"):
                    try:
                        if os.path.exists(filepath):
                            os.remove(filepath)
                            self.log_msg(f"源数据已物理销毁: {filename}", "sys")
                            self.tree.delete(iid)
                    except Exception as e:
                        messagebox.showerror("删除失败", str(e))
        else:
            if messagebox.askyesno("批量清理确认", f"确定批量删除选中的 {len(selection)} 个源文件吗？"):
                for iid in selection:
                    filename = getattr(self, '_file_mapping', {}).get(iid)
                    if filename:
                        filepath = os.path.join(self.src_var.get(), filename)
                        try:
                            if os.path.exists(filepath):
                                os.remove(filepath)
                                self.tree.delete(iid)
                        except Exception as e:
                            self.log_msg(f"删除 {filename} 时失败: {str(e)}", "err")
                self.log_msg(f"源文件批量物理销毁完成", "sys")

    def on_delete_tgt(self, event):
        selection = self.tgt_tree.selection()
        if not selection: return
        
        if len(selection) == 1:
            iid = selection[0]
            filename = getattr(self, '_target_mapping', {}).get(iid)
            if filename:
                filepath = os.path.join(self.tgt_var.get(), filename)
                if messagebox.askyesno("清理确认", f"确定删除输出结果文件吗？\n\n{filename}"):
                    try:
                        if os.path.exists(filepath):
                            os.remove(filepath)
                            self.log_msg(f"输出数据已物理销毁: {filename}", "succ")
                            self.tgt_tree.delete(iid)
                    except Exception as e:
                        messagebox.showerror("删除失败", str(e))
        else:
            if messagebox.askyesno("批量清理确认", f"确定批量删除选中的 {len(selection)} 个输出结果文件吗？"):
                for iid in selection:
                    filename = getattr(self, '_target_mapping', {}).get(iid)
                    if filename:
                        filepath = os.path.join(self.tgt_var.get(), filename)
                        try:
                            if os.path.exists(filepath):
                                os.remove(filepath)
                                self.tgt_tree.delete(iid)
                        except Exception as e:
                            self.log_msg(f"删除 {filename} 时失败: {str(e)}", "err")
                self.log_msg(f"输出结果批量物理销毁完成", "succ")

    def on_start_click(self):
        if self.process_thread and self.process_thread.is_alive():
            return
            
        selected_items = self.tree.selection()
        if not selected_items:
            messagebox.showwarning("空载启动弹回", "没有在源晶体缓存库中勾选任何需要降维的文件。")
            return
            
        target_path = Path(self.tgt_var.get())
        if not target_path.exists():
            try:
                target_path.mkdir(parents=True)
            except Exception as e:
                messagebox.showerror("挂载失败", f"无法创建输出目录: {e}")
                return
                
        files_to_process = []
        for iid in selected_items:
            filename = self._file_mapping.get(iid)
            if not filename: continue
            filepath = os.path.join(self.src_var.get(), filename)
            files_to_process.append(filepath)
            
        start_dt = None
        end_dt = None
        if not self.all_history_var.get():
            try:
                sy, sm, sd = int(self.start_y.get()), int(self.start_m.get()), int(self.start_d.get())
                ey, em, ed = int(self.end_y.get()), int(self.end_m.get()), int(self.end_d.get())
                start_dt = datetime.datetime(sy, sm, sd, 0, 0, 0)
                end_dt = datetime.datetime(ey, em, ed, 23, 59, 59)
                self.log_msg(f"[*] 时间切割域已锁定: {start_dt.date()} -> {end_dt.date()}")
            except ValueError:
                messagebox.showerror("坐标解析错误", "提取边界的时间选择器中存在不合法日期。")
                return
        else:
            self.log_msg("[*] 忽略时间阀门，进入全量无差别降维模式")
            
        self.stop_requested = False
        self.start_btn.config(state=DISABLED)
        self.stop_btn.config(state=NORMAL)
        self.prog_lbl.config(text="引擎轰鸣中：正在并列裂解降维阵列...", fg=self.c_gold)
        self.set_progress(0)
        self.log_widget.configure(state='normal')
        self.log_widget.delete("1.0", END)
        self.log_widget.configure(state='disabled')
        
        self.log_msg(f"[>] 指令下达: {len(files_to_process)} 份物理切片准备压入反应堆", "sys")
        
        self.process_thread = threading.Thread(target=self._run_process_pool, args=(files_to_process, str(target_path), start_dt, end_dt))
        self.process_thread.daemon = True
        self.process_thread.start()

    def _run_process_pool(self, files, out_dir, start_dt, end_dt):
        total = len(files)
        success_ct = 0
        
        try:
            with ProcessPoolExecutor(max_workers=os.cpu_count() or 4) as executor:
                futures = []
                for f in files:
                    futures.append(executor.submit(process_single_file, f, out_dir, start_dt, end_dt))
                
                for idx, future in enumerate(futures, 1):
                    if self.stop_requested:
                        self.after(0, lambda: self.log_msg("[!] 收到强制熔断命令，切断后续通道。", "warn"))
                        executor.shutdown(wait=False, cancel_futures=True)
                        break
                        
                    is_succ, title, msg = future.result()
                    if is_succ:
                        success_ct += 1
                        self.after(0, lambda t=title, m=msg: self.log_msg(f"[+] {t} | {m}", "succ"))
                    else:
                        self.after(0, lambda t=title, m=msg: self.log_msg(f"[-] {t} | {m}", "err"))
                        
                    pct = int((idx / total) * 100)
                    self.after(0, lambda p=pct: self.set_progress(p))
                    
        except Exception as e:
            self.after(0, lambda e=e: self.log_msg(f"[x] 处理池遭遇系统级毁灭打击: {str(e)}", "err"))
            
        self.after(0, lambda t=total, s=success_ct: self._on_finish(t, s))

    def _on_finish(self, total, success):
        self.start_btn.config(state=NORMAL)
        self.stop_btn.config(state=DISABLED)
        if self.stop_requested:
            self.prog_lbl.config(text="系统迫降：人工强制干预脱离", fg=self.c_red)
        else:
            self.prog_lbl.config(text=f"降维矩阵执行完毕: {success}/{total} 条流完成转化", fg=self.c_green)
            self.log_msg(f"[>] 列队脱离，产物已全部释放至: {self.tgt_var.get()}", "sys")

    def on_stop_click(self):
        self.stop_requested = True
        self.stop_btn.config(state=DISABLED)

if __name__ == "__main__":
    app = ResampleMatrixGUI()
    app.mainloop()
