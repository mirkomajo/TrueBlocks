#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from __future__ import annotations

"""
DEXTRACK — LP Farm P&L Studio + Desktop Button Runner with Interactive Terminal (Tkinter)

This build:
- Added a fully-functional CSV search bar next to "CSV IMPORT".
  • Case-insensitive search across all columns.
  • "Search" applies filter; "Clear" resets; hit counter shown.
  • Sorting & export operate on the visible (filtered) rows.
- Themes: Amber (built-in), Retro Green (80s), Sun Valley (nerd - green text), Sun Valley (evee - purple text).
- Matrix-style hex swap header animation (symmetrical entry/exit).
- CSV loader renders NaN/None as blanks.
- TXS DETAIL formatted like your screenshot; Received amounts show '+' for positive numbers.
- Layout: CSV left, right column stacks TXS (top) and TERMINAL (bottom).
"""

import os
import sys
import csv
import json
import random
import queue
import shlex
import signal
import subprocess
import threading
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from datetime import datetime

try:
    import pandas as pd  # Optional (for faster CSV)
except Exception:
    pd = None  # type: ignore

import tkinter as tk
from tkinter import filedialog, messagebox
from tkinter import font as tkfont
from tkinter import ttk
from tkinter.scrolledtext import ScrolledText

# ====================== PARAMETER CHANGE (edit here) ======================
APP_BRAND              = "DEXTRACK"
APP_NAME               = f"{APP_BRAND} — KNOW YOUR P&L"

# Brand header behavior (matrix-like HEX swap)
BRAND_SWAP_PERIOD_MS   = 20000   # how often to start a swap cycle (20s)
GLITCH_DURATION_MS     = 960     # how long one direction of the glitch runs
GLITCH_STEP_MS         = 120     # speed of glitch frames
GLITCH_HOLD_MS         = 650     # hold time at each end state
HEX_LEN                = 8       # length of the hex-ish string (after "0x")
HEX_ALPHABET           = "0123456789ABCDEFJXv"

# Window sizing
INIT_SIZE              = (1860, 1020)
FIT_TO_SCREEN          = True
WINDOW_OFFSET          = (8, 8)
MIN_SIZE               = (1280, 820)

# Panel widths/heights
SIDEBAR_W              = 260
DEFAULT_TERM_RATIO     = 0.58      # fraction of right column height reserved for Terminal
DEFAULT_CSV_RATIO      = 0.68      # fraction of total width reserved for CSV (left)
MIN_RIGHT_W_RESTORE    = 360
MIN_TXS_H_RESTORE      = 180
MIN_TERMINAL_H_RESTORE = 200

# Table density / autosize
DEFAULT_FONT_SIZE      = 13
DEFAULT_FONT_SIZES     = {"csv": 13, "txs": 15, "terminal": 19}
ROW_HEIGHT_FACTOR      = 1.6
MAX_TREE_ROWS          = 1000
ROW_SAMPLE_FOR_WIDTH   = 500
AUTOSIZE_ALL_ON_LOAD   = True
COL_CHAR_PX            = 7.2
MIN_COL_W              = 140
MAX_COL_W              = 380

# CSV & hotkeys
SYNC_SYMBOL            = "⟳"
SYNC_HOTKEY            = "<Control-r>"
PREFER_PANDAS          = True
STARTUP_CSV            = os.environ.get(
    "RICHMAN_STARTUP_CSV",
    r"C:\Users\MKM\Desktop\lptool\app\data\simple_txs_overview.csv"
)
CONFIG_FILENAME        = "config.json"

# Sessions (auto-load / quick-save)
DEFAULT_SESSION_NAME   = "last_session.ambersession"
MAX_TERMINAL_SAVE_CHARS= 300_000

# Console header/footers around button runs
SHOW_BUTTON_HEADER     = True
SHOW_RUN_CMD           = False
SHOW_EXIT_NOTICE       = False
# ========================================================================

# ----------------- Base color definitions -----------------
# Amber theme
AMBER_FG           = "#FFBF00"
INK                = "#0A0A0A"
BG                 = "#000000"
CARD               = "#0F0F0F"
ACCENT             = "#FFD966"
SELECT_BG          = "#262626"
ROW_ALT            = "#111111"

# Retro Green theme
RETRO_BG           = "#000000"
RETRO_CARD         = "#001800"
RETRO_INK          = "#000A00"
RETRO_FG           = "#00FF66"
RETRO_ACCENT       = "#33FF99"
RETRO_SELECT       = "#003300"
RETRO_ROW_ALT      = "#001500"

# Sun Valley dark base (we reuse sv_ttk if available, and tint our custom palette)
SV_BG              = "#1B1B1B"
SV_CARD            = "#232323"
SV_INK             = "#141414"
SV_ROW_ALT         = "#1E1E1E"
SV_SELECT          = "#2A2A2A"
NERD_FG            = RETRO_FG         # text color for "Sun Valley (nerd)"
EVEE_FG            = "#C084FC"        # purple for "Sun Valley (evee)"

# ====================== UTILITIES ======================
def best_mono() -> str:
    root = tk._get_default_root() or tk.Tk()
    families = set(tkfont.families(root))
    for fam in (
        "JetBrains Mono",
        "Cascadia Mono",
        "Consolas",
        "Menlo",
        "DejaVu Sans Mono",
        "SF Mono",
        "Fira Code",
        "Courier New",
    ):
        if fam in families:
            return fam
    return "Courier"


def safe_float(v: object) -> float:
    try:
        return float(str(v).replace(",", "").strip())
    except Exception:
        return 0.0


def app_dir() -> Path:
    try:
        return Path(__file__).resolve().parent
    except Exception:
        return Path(os.getcwd())


def clean_value(v: object) -> str:
    """Normalize any 'nan'/'NaN'/'None' to a blank string for display."""
    s = "" if v is None else str(v)
    return "" if s.strip().lower() in {"nan", "none"} else s


APP_DIR = app_dir()
CONFIG_DIR = APP_DIR / "config"
CONFIG_PATH = CONFIG_DIR / CONFIG_FILENAME
DEFAULT_SESSION_PATH = APP_DIR / DEFAULT_SESSION_NAME

DEFAULT_CONFIG = {
    "wallet_address": "0x42fd11266e2b05e7a86576774049f6faab6582e9",
    "buttons": {
        str(i): {
            "label": f"Button {i}",
            "path": "",
            "args": "",
            "prefer_module": True,
            "force_module": False,
            "cwd": "",
            "pass_csv": True
        } for i in range(1, 10 + 1)
    },
    "ui": {
        "font_sizes": DEFAULT_FONT_SIZES.copy()
    }
}


def save_config(cfg: dict) -> None:
    try:
        CONFIG_DIR.mkdir(parents=True, exist_ok=True)
        CONFIG_PATH.write_text(json.dumps(cfg, indent=2), encoding="utf-8")
    except Exception as e:
        try:
            messagebox.showwarning("Config", f"Couldn't save {CONFIG_FILENAME}:\n{e}")
        except Exception:
            print(f"[WARN] Couldn't save {CONFIG_FILENAME}: {e}", file=sys.stderr)


def load_config() -> dict:
    dirty = False
    if CONFIG_PATH.exists():
        try:
            cfg = json.loads(CONFIG_PATH.read_text(encoding="utf-8"))
        except Exception as e:
            try:
                messagebox.showwarning("Config", f"Couldn't read {CONFIG_FILENAME}, regenerating defaults.\n{e}")
            except Exception:
                print(f"[WARN] Couldn't read {CONFIG_FILENAME}, regenerating defaults: {e}", file=sys.stderr)
            cfg = json.loads(json.dumps(DEFAULT_CONFIG))
            dirty = True
    else:
        cfg = json.loads(json.dumps(DEFAULT_CONFIG))
        dirty = True

    if not isinstance(cfg.get("wallet_address"), str):
        cfg["wallet_address"] = DEFAULT_CONFIG["wallet_address"]
        dirty = True

    buttons = cfg.setdefault("buttons", {})
    for i in range(1, 10 + 1):
        key = str(i)
        entry = buttons.get(key)
        if not isinstance(entry, dict):
            buttons[key] = json.loads(json.dumps(DEFAULT_CONFIG["buttons"][key]))
            dirty = True
            continue
        for fld, default_val in DEFAULT_CONFIG["buttons"][key].items():
            if fld not in entry:
                entry[fld] = default_val
                dirty = True
        if not isinstance(entry["label"], str) or not entry["label"]:
            entry["label"] = f"Button {i}"
            dirty = True
        for bf in ("prefer_module", "force_module", "pass_csv"):
            if not isinstance(entry.get(bf), bool):
                entry[bf] = bool(entry.get(bf, DEFAULT_CONFIG["buttons"][key][bf]))
                dirty = True
        for sf in ("path", "args", "cwd"):
            if not isinstance(entry.get(sf), str):
                entry[sf] = str(entry.get(sf, ""))
                dirty = True

    ui = cfg.setdefault("ui", {})
    sizes = ui.get("font_sizes")
    if not isinstance(sizes, dict):
        sizes = DEFAULT_FONT_SIZES.copy()
        ui["font_sizes"] = sizes
        dirty = True
    for key, default in DEFAULT_FONT_SIZES.items():
        if not isinstance(sizes.get(key), int):
            sizes[key] = default
            dirty = True
    ui["font_size"] = sizes.get("csv", DEFAULT_FONT_SIZES["csv"])

    if dirty:
        save_config(cfg)
    return cfg


def detect_project_root(script_path: Path) -> Path:
    try:
        cand = script_path.parents[2]
        if (cand / "app" / "__init__.py").exists():
            return cand
    except Exception:
        pass
    return script_path.parent


def build_run_command(mapping: dict, loaded_csv: str | None):
    py = sys.executable or "python"
    base = [py, "-u"]
    args_from_cfg = shlex.split(mapping.get("args", "") or "")
    csv_arg = [loaded_csv] if loaded_csv and mapping.get("pass_csv", True) else []

    force_module = bool(mapping.get("force_module"))
    prefer_module = bool(mapping.get("prefer_module", True))
    path_str = mapping.get("path", "") or ""
    if not path_str:
        raise FileNotFoundError("No script configured for this button.")

    if force_module and "." in path_str and not Path(path_str).exists():
        module_name = path_str
        cwd = mapping.get("cwd") or str(APP_DIR)
        cmd = base + ["-m", module_name] + csv_arg + args_from_cfg
        return cmd, cwd

    sp = Path(path_str)
    if sp.exists():
        if prefer_module:
            project_root = detect_project_root(sp)
            try:
                rel = sp.relative_to(project_root)
                module_name = ".".join(rel.with_suffix("").parts)
                if (project_root / "app" / "__init__.py").exists() and module_name.startswith("app."):
                    cwd = mapping.get("cwd") or str(project_root)
                    cmd = base + ["-m", module_name] + csv_arg + args_from_cfg
                    return cmd, cwd
            except Exception:
                pass
        cwd = mapping.get("cwd") or str(sp.parent)
        cmd = base + [str(sp)] + csv_arg + args_from_cfg
        return cmd, cwd

    if "." in path_str:
        cwd = mapping.get("cwd") or str(APP_DIR)
        cmd = base + ["-m", path_str] + csv_arg + args_from_cfg
        return cmd, cwd

    raise FileNotFoundError(f"Script not found: {path_str}")


# ====================== MAIN APP ======================
class AmberRunner(tk.Tk):
    def __init__(self) -> None:
        super().__init__()

        # ---------- Window ----------
        self.title(APP_NAME)
        w, h = INIT_SIZE
        if FIT_TO_SCREEN:
            sw = max(640, self.winfo_screenwidth())
            sh = max(480, self.winfo_screenheight())
            w = min(w, sw - (WINDOW_OFFSET[0] * 2))
            h = min(h, sh - (WINDOW_OFFSET[1] * 2))
        self.geometry(f"{w}x{h}+{WINDOW_OFFSET[0]}+{WINDOW_OFFSET[1]}")
        self.minsize(*MIN_SIZE)

        # ---------- Fonts ----------
        fam = best_mono()
        self.font_base = tkfont.Font(family=fam, size=DEFAULT_FONT_SIZE)
        self.font_title = tkfont.Font(family=fam, size=14, weight="bold")
        self.font_huge = tkfont.Font(family=fam, size=20, weight="bold")
        self.font_brand = tkfont.Font(family=fam, size=24, weight="bold")

        # ---------- Config / State ----------
        self.config_data = load_config()
        ui_cfg = self.config_data.get("ui", {})
        self.font_sizes = {
            "csv": int(ui_cfg.get("font_sizes", {}).get("csv", DEFAULT_FONT_SIZES["csv"])),
            "txs": int(ui_cfg.get("font_sizes", {}).get("txs", DEFAULT_FONT_SIZES["txs"])),
            "terminal": int(ui_cfg.get("font_sizes", {}).get("terminal", DEFAULT_FONT_SIZES["terminal"])),
        }
        self.font_size = self.font_sizes["csv"]

        # Data sets: full and current view (filtered/sorted)
        self.data_full: Optional[List[Dict[str, str]]] = None
        self.data_view: Optional[List[Dict[str, str]]] = None
        self.columns: List[str] = []
        self.current_csv_path: Optional[str] = None

        self.proc: subprocess.Popen | None = None
        self.reader_thread: threading.Thread | None = None
        self.q = queue.Queue()
        self._sort_state = {}
        self._session_loaded = False
        self._last_started_button: Optional[int] = None

        # Brand timers
        self._brand_glitch_job: Optional[str] = None
        self._brand_swap_job: Optional[str] = None

        # ---------- Theme/palette ----------
        self._theme_current = "Amber"
        self.P: Dict[str, str] = {}
        self._apply_pro_theme_if_available()
        self._set_palette("amber")
        self._init_styles()

        # ---------- Menubar ----------
        self._build_menubar()

        # ---------- Main split ----------
        self.main_pane = ttk.Panedwindow(self, orient="horizontal", style="App.TPanedwindow")
        self.main_pane.pack(fill="both", expand=True)

        # Sidebar
        self.sidebar = ttk.Frame(self.main_pane, style="Sidebar.TFrame", width=SIDEBAR_W)
        self._build_sidebar(self.sidebar)

        # Workspace
        self.workspace = ttk.Frame(self.main_pane, style="TFrame")
        self._build_workspace(self.workspace)

        self.main_pane.add(self.sidebar, weight=0)
        self.main_pane.add(self.workspace, weight=1)

        # Status
        self.status = tk.StringVar(value="Ready.")
        st = ttk.Frame(self, style="TFrame")
        st.pack(fill="x")
        self.status_label = tk.Label(
            st,
            textvariable=self.status,
            anchor="w",
            bg=self.P["INK"],
            fg=self.P["FG"],
            font=self.font_base,
            padx=10,
            pady=6,
        )
        self.status_label.pack(fill="x")

        # Hotkeys
        self.bind_all(SYNC_HOTKEY, lambda e: self.sync_csv())

        # Layout & auto-load
        self.after(120, self._snap_initial_layout)
        self.after(160, self._load_last_session_if_any)
        self.after(240, self._load_startup_csv_if_any)

        # Brand swap loop
        self.after(800, self._schedule_next_hex_cycle)

        # Apply root bg
        self.configure(bg=self.P["BG"])

    # ===================== Theme support =====================
    def _apply_pro_theme_if_available(self) -> None:
        """Optionally enable Sun Valley look (sv_ttk) if present; do NOT auto-switch theme."""
        self._sv_available = False
        try:
            import sv_ttk  # type: ignore
            sv_ttk.set_theme("dark")  # ensure styles exist if user chooses a Sun Valley variant later
            self._sv_available = True
        except Exception:
            self._sv_available = False
        try:
            ttk.Style().theme_use("clam")
        except Exception:
            pass

    def _set_palette(self, which: str) -> None:
        w = which.lower()
        if w.startswith("retro"):
            self.P = {
                "BG": RETRO_BG,
                "CARD": RETRO_CARD,
                "INK": RETRO_INK,
                "FG": RETRO_FG,
                "ACCENT": RETRO_ACCENT,
                "SELECT": RETRO_SELECT,
                "ROW_ALT": RETRO_ROW_ALT,
                "MENU_BG": RETRO_INK,
                "MENU_FG": RETRO_FG,
                "MENU_ACTIVE_BG": "#001F00",
                "MENU_ACTIVE_FG": RETRO_ACCENT,
            }
        elif w.startswith("sv_nerd"):
            self.P = {
                "BG": SV_BG,
                "CARD": SV_CARD,
                "INK": SV_INK,
                "FG": NERD_FG,
                "ACCENT": "#4ADE80",
                "SELECT": SV_SELECT,
                "ROW_ALT": SV_ROW_ALT,
                "MENU_BG": SV_INK,
                "MENU_FG": NERD_FG,
                "MENU_ACTIVE_BG": "#222833",
                "MENU_ACTIVE_FG": "#8AFFC1",
            }
        elif w.startswith("sv_evee"):
            self.P = {
                "BG": SV_BG,
                "CARD": SV_CARD,
                "INK": SV_INK,
                "FG": EVEE_FG,
                "ACCENT": "#A78BFA",
                "SELECT": SV_SELECT,
                "ROW_ALT": SV_ROW_ALT,
                "MENU_BG": SV_INK,
                "MENU_FG": EVEE_FG,
                "MENU_ACTIVE_BG": "#2D1B3D",
                "MENU_ACTIVE_FG": "#D6BCFA",
            }
        else:  # amber
            self.P = {
                "BG": BG,
                "CARD": CARD,
                "INK": INK,
                "FG": AMBER_FG,
                "ACCENT": ACCENT,
                "SELECT": SELECT_BG,
                "ROW_ALT": ROW_ALT,
                "MENU_BG": INK,
                "MENU_FG": AMBER_FG,
                "MENU_ACTIVE_BG": "#1a1a1a",
                "MENU_ACTIVE_FG": ACCENT,
            }

    def _apply_theme_choice(self, choice: str) -> None:
        if choice in ("Sun Valley (nerd)", "Sun Valley (evee)"):
            if self._sv_available:
                try:
                    import sv_ttk  # type: ignore
                    sv_ttk.set_theme("dark")
                except Exception:
                    pass

        if choice == "Amber":
            try:
                ttk.Style().theme_use("clam")
            except Exception:
                pass
            self._theme_current = "Amber"
            self._set_palette("amber")
        elif choice == "Retro Green (80s)":
            self._theme_current = "Retro Green (80s)"
            self._set_palette("retro")
        elif choice == "Sun Valley (nerd)":
            self._theme_current = "Sun Valley (nerd)"
            self._set_palette("sv_nerd")
        elif choice == "Sun Valley (evee)":
            self._theme_current = "Sun Valley (evee)"
            self._set_palette("sv_evee")

        self._init_styles()
        self.configure(bg=self.P["BG"])
        if hasattr(self, "tree"):
            self.tree.configure(style="App.Treeview")
        if hasattr(self, "txs_text"):
            self.txs_text.configure(
                bg=self.P["INK"], fg=self.P["FG"], insertbackground=self.P["FG"],
                highlightbackground=self.P["FG"], highlightcolor=self.P["FG"]
            )
        if hasattr(self, "term_text"):
            self.term_text.configure(bg=self.P["INK"], fg=self.P["FG"], insertbackground=self.P["FG"])
        if hasattr(self, "term_entry"):
            try:
                self.term_entry.configure(bg=self.P["INK"], fg=self.P["FG"], insertbackground=self.P["FG"])
            except Exception:
                pass
        if hasattr(self, "status_label"):
            self.status_label.configure(bg=self.P["INK"], fg=self.P["FG"])
        if hasattr(self, "brand_label"):
            self.brand_label.configure(bg=self.P["INK"], fg=self.P["FG"])
        self._build_menubar()
        self._schedule_next_hex_cycle()

    def _init_styles(self) -> None:
        s = ttk.Style(self)
        s.configure("TFrame", background=self.P["BG"])
        s.configure("Card.TFrame", background=self.P["CARD"])
        s.configure("Sidebar.TFrame", background=self.P["INK"])
        s.configure("App.TPanedwindow", background=self.P["BG"])

        s.configure("App.TLabel", background=self.P["CARD"], foreground=self.P["FG"], font=self.font_base)
        s.configure("AppTitle.TLabel", background=self.P["CARD"], foreground=self.P["FG"], font=self.font_title)
        s.configure("AppHuge.TLabel", background=self.P["BG"], foreground=self.P["FG"], font=self.font_huge)

        s.configure("App.TButton", background=self.P["CARD"], foreground=self.P["FG"], padding=6, font=self.font_base)
        s.map("App.TButton", background=[("active", "#151515")], foreground=[("active", self.P["ACCENT"])])

        s.configure("Sidebar.TButton", background=self.P["INK"], foreground=self.P["FG"], padding=10, font=self.font_base)
        s.map("Sidebar.TButton", background=[("active", "#151515")], foreground=[("active", self.P["ACCENT"])])

        s.layout("App.Treeview", [("Treeview.treearea", {"sticky": "nswe"})])
        s.configure(
            "App.Treeview",
            background=self.P["INK"],
            fieldbackground=self.P["INK"],
            foreground=self.P["FG"],
            rowheight=max(22, int(self.font_sizes["csv"] * ROW_HEIGHT_FACTOR)),
            font=(best_mono(), self.font_sizes["csv"]),
            bordercolor=self.P["INK"],
            lightcolor=self.P["INK"],
            darkcolor=self.P["INK"],
            troughcolor=self.P["INK"],
        )
        s.map("App.Treeview", background=[("selected", self.P["SELECT"])], foreground=[("selected", self.P["ACCENT"])])
        s.configure("App.Treeview.Heading", background=self.P["CARD"], foreground=self.P["ACCENT"], font=self.font_title)

        s.configure("App.Vertical.TScrollbar", background=self.P["INK"], troughcolor=self.P["BG"], arrowcolor=self.P["FG"])
        s.configure("App.Horizontal.TScrollbar", background=self.P["INK"], troughcolor=self.P["BG"], arrowcolor=self.P["FG"])

        s.configure("App.TCombobox", fieldbackground=self.P["INK"], background=self.P["INK"], foreground=self.P["FG"], arrowcolor=self.P["FG"])
        s.map("App.TCombobox", fieldbackground=[("readonly", self.P["INK"])], foreground=[("readonly", self.P["FG"])], arrowcolor=[("active", self.P["ACCENT"])])

    # ===================== Menubar =====================
    def _build_menubar(self) -> None:
        m = tk.Menu(self, tearoff=False, bg=self.P["MENU_BG"], fg=self.P["MENU_FG"],
                    activebackground=self.P["MENU_ACTIVE_BG"], activeforeground=self.P["MENU_ACTIVE_FG"])

        filem = tk.Menu(m, tearoff=False, bg=self.P["MENU_BG"], fg=self.P["MENU_FG"],
                        activebackground=self.P["MENU_ACTIVE_BG"], activeforeground=self.P["MENU_ACTIVE_FG"])
        filem.add_command(label="Open CSV…", command=self.load_csv_dialog, accelerator="Ctrl+O")
        filem.add_command(label="Export Table as CSV…", command=self.export_table_csv, accelerator="Ctrl+E")
        filem.add_separator()
        filem.add_command(label="Save Session As…", command=self.save_session_as, accelerator="Ctrl+S")
        filem.add_command(label="Load Session…", command=self.load_session, accelerator="Ctrl+L")
        filem.add_separator()
        filem.add_command(label="Set Wallet Address…", command=self.set_wallet_address)
        filem.add_separator()
        filem.add_command(label="Clear", command=self.clear_csv, accelerator="Ctrl+N")
        filem.add_separator()
        filem.add_command(label="Exit", command=self.destroy)
        m.add_cascade(label="File", menu=filem)

        viewm = tk.Menu(m, tearoff=False, bg=self.P["MENU_BG"], fg=self.P["MENU_FG"],
                        activebackground=self.P["MENU_ACTIVE_BG"], activeforeground=self.P["MENU_ACTIVE_FG"])
        themem = tk.Menu(viewm, tearoff=False, bg=self.P["MENU_BG"], fg=self.P["MENU_FG"],
                         activebackground=self.P["MENU_ACTIVE_BG"], activeforeground=self.P["MENU_ACTIVE_FG"])
        themem.add_command(label="Amber (built-in)", command=lambda: self._apply_theme_choice("Amber"))
        themem.add_command(label="Retro Green (80s)", command=lambda: self._apply_theme_choice("Retro Green (80s)"))
        themem.add_command(label="Sun Valley (nerd)", command=lambda: self._apply_theme_choice("Sun Valley (nerd)"))
        themem.add_command(label="Sun Valley (evee)", command=lambda: self._apply_theme_choice("Sun Valley (evee)"))
        viewm.add_cascade(label=f"Theme ({self._theme_current})", menu=themem)
        m.add_cascade(label="View", menu=viewm)

        self.config(menu=m)

        self.bind_all("<Control-o>", lambda e: self.load_csv_dialog())
        self.bind_all("<Control-e>", lambda e: self.export_table_csv())
        self.bind_all("<Control-s>", lambda e: self.save_session_as())
        self.bind_all("<Control-l>", lambda e: self.load_session())
        self.bind_all("<Control-n>", lambda e: self.clear_csv())

    # ===================== Sidebar =====================
    def set_wallet_address(self) -> None:
        cur = self.config_data.get("wallet_address", "")
        val = self.simple_prompt(self, "Set Wallet Address", cur)
        if val is None:
            return
        self.config_data["wallet_address"] = val.strip()
        save_config(self.config_data)
        self.status.set("Wallet address updated.")
        self._update_overview_info()

    def _build_sidebar(self, parent: tk.Misc) -> None:
        self.brand_label = tk.Label(
            parent,
            text=APP_BRAND,
            bg=self.P["INK"],
            fg=self.P["FG"],
            font=self.font_brand,
            padx=14,
            pady=16,
            anchor="w",
        )
        self.brand_label.pack(fill="x")

        self.menu_buttons: Dict[int, ttk.Button] = {}
        for i in range(1, 10 + 1):
            label = self.config_data["buttons"][str(i)].get("label", f"Button {i}")
            b = ttk.Button(parent, text=label, style="Sidebar.TButton", command=lambda n=i: self.on_button_clicked(n))
            b.pack(fill="x", padx=10, pady=6)
            self.menu_buttons[i] = b

        ttk.Separator(parent).pack(fill="x", padx=10, pady=(8, 8))
        ttk.Button(parent, text="SETTINGS", style="Sidebar.TButton", command=self.configure_buttons).pack(fill="x", padx=10, pady=(0, 6))

        tk.Label(parent, text="Built by testnetmirko", bg=self.P["INK"], fg=self.P["FG"], font=self.font_base).pack(
            side="bottom", pady=10
        )

    # ---------- Brand animation (symmetrical) ----------
    def _schedule_next_hex_cycle(self) -> None:
        if self._brand_swap_job is not None:
            try:
                self.after_cancel(self._brand_swap_job)
            except Exception:
                pass
        self._brand_swap_job = self.after(BRAND_SWAP_PERIOD_MS, self._perform_hex_swap_cycle)

    def _perform_hex_swap_cycle(self) -> None:
        steps = max(1, GLITCH_DURATION_MS // GLITCH_STEP_MS)
        self._run_glitch(steps_remaining=steps, target="hex")

    def _rand_hex(self) -> str:
        return "0x" + "".join(random.choice(HEX_ALPHABET) for _ in range(HEX_LEN))

    def _run_glitch(self, steps_remaining: int, target: str) -> None:
        if self._brand_glitch_job is not None:
            try:
                self.after_cancel(self._brand_glitch_job)
            except Exception:
                pass
            self._brand_glitch_job = None

        if steps_remaining <= 0:
            if target == "hex":
                self.brand_label.configure(text=self._rand_hex())
                self._brand_glitch_job = self.after(
                    GLITCH_HOLD_MS, lambda: self._run_glitch(
                        steps_remaining=max(1, GLITCH_DURATION_MS // GLITCH_STEP_MS),
                        target="brand"
                    )
                )
            else:
                self.brand_label.configure(text=APP_BRAND)
                self._brand_glitch_job = self.after(GLITCH_HOLD_MS, self._schedule_next_hex_cycle)
            return

        self.brand_label.configure(text=self._rand_hex())
        self._brand_glitch_job = self.after(
            GLITCH_STEP_MS, lambda: self._run_glitch(steps_remaining - 1, target)
        )

    def _refresh_sidebar_labels(self) -> None:
        for i in range(1, 10 + 1):
            btn = self.menu_buttons.get(i)
            if not btn:
                continue
            label = self.config_data["buttons"][str(i)].get("label", f"Button {i}")
            try:
                btn.configure(text=label)
            except Exception:
                pass

    # ===================== Workspace =====================
    def _build_workspace(self, parent: tk.Misc) -> None:
        # Toolbar
        toolbar = ttk.Frame(parent, style="Card.TFrame")
        toolbar.pack(fill="x", padx=10, pady=(10, 6))

        ttk.Button(toolbar, text="💾 Save Session", style="App.TButton", command=self.quick_save_session).pack(
            side="left", padx=(8, 6), pady=6
        )
        ttk.Button(toolbar, text="📂 Load Session", style="App.TButton", command=self.load_session).pack(
            side="left", padx=6, pady=6
        )
        ttk.Button(toolbar, text="Load CSV", style="App.TButton", command=self.load_csv_dialog).pack(
            side="left", padx=6, pady=6
        )
        ttk.Button(toolbar, text=f"{SYNC_SYMBOL} Sync", style="App.TButton", command=self.sync_csv).pack(
            side="left", padx=6, pady=6
        )
        ttk.Button(toolbar, text="Export Table CSV", style="App.TButton", command=self.export_table_csv).pack(
            side="left", padx=6, pady=6
        )

        # KPIs
        header = ttk.Frame(parent, style="Card.TFrame")
        header.pack(fill="x", padx=10, pady=(0, 6))
        ttk.Label(header, text="OVERVIEW", style="AppTitle.TLabel").pack(side="left", padx=12, pady=8)
        self.kpi_overview = ttk.Label(
            header,
            text=self.config_data.get("wallet_address", ""),
            style="App.TLabel",
        )
        self.kpi_overview.pack(side="left", padx=10)
        right = ttk.Frame(header, style="Card.TFrame")
        right.pack(side="right", padx=8)
        self.kpi_file = ttk.Label(right, text="No file loaded", style="App.TLabel")
        self.kpi_file.pack(side="left", padx=10)

        # Bottom: CSV (left) | RIGHT_COL (TXS above TERMINAL)
        self.bottom_split = ttk.Panedwindow(parent, orient="horizontal", style="App.TPanedwindow")
        self.bottom_split.pack(fill="both", expand=True, padx=10, pady=10)

        # Left: CSV
        self.csv_frame = ttk.Frame(self.bottom_split, style="Card.TFrame")
        self._build_csv(self.csv_frame)

        # Right column: vertical split
        self.right_split = ttk.Panedwindow(self.bottom_split, orient="vertical", style="App.TPanedwindow")
        self.txs_frame = ttk.Frame(self.right_split, style="Card.TFrame")
        self.term_frame = ttk.Frame(self.right_split, style="Card.TFrame")

        self._build_txs(self.txs_frame)
        self._build_terminal(self.term_frame)

        self.right_split.add(self.txs_frame, weight=1)
        self.right_split.add(self.term_frame, weight=1)

        self.bottom_split.add(self.csv_frame, weight=1)
        self.bottom_split.add(self.right_split, weight=0)

    def _build_txs(self, parent: tk.Misc) -> None:
        for w in parent.winfo_children():
            w.destroy()
        ttk.Label(parent, text="TXS DETAIL", style="AppTitle.TLabel").pack(anchor="w", padx=12, pady=8)

        wrap = ttk.Frame(parent, style="Card.TFrame")
        wrap.pack(fill="both", expand=True, padx=10, pady=10)

        xsb = ttk.Scrollbar(wrap, orient="horizontal", style="App.Horizontal.TScrollbar")
        ysb = ttk.Scrollbar(wrap, orient="vertical", style="App.Vertical.TScrollbar")

        self.txs_text = tk.Text(
            wrap,
            bg=self.P["INK"],
            fg=self.P["FG"],
            insertbackground=self.P["FG"],
            font=(best_mono(), self.font_sizes["txs"]),
            wrap="word",
            relief="flat",
            xscrollcommand=xsb.set,
            yscrollcommand=ysb.set,
            highlightthickness=1,
            highlightbackground=self.P["FG"],
            highlightcolor=self.P["FG"],
        )
        self.txs_text.pack(side="left", fill="both", expand=True)
        self.txs_text.bind("<Control-MouseWheel>", lambda e: self._on_zoom("txs", e))
        ysb.config(command=self.txs_text.yview)
        ysb.pack(side="right", fill="y")
        xsb.config(command=self.txs_text.xview)
        xsb.pack(side="bottom", fill="x")

        self.txs_text.insert("end", "Load a CSV to see row details here.\n")

    # ---------------- CSV AREA (with Search) ----------------
    def _build_csv(self, parent: tk.Misc) -> None:
        for w in parent.winfo_children():
            w.destroy()

        # Header row with "CSV IMPORT" (left) and Search controls (right)
        hdr = ttk.Frame(parent, style="Card.TFrame")
        hdr.pack(fill="x", padx=12, pady=(8, 0))
        ttk.Label(hdr, text="CSV IMPORT", style="AppTitle.TLabel").pack(side="left")

        sr = ttk.Frame(hdr, style="Card.TFrame")
        sr.pack(side="right")

        self.search_var = tk.StringVar(value="")
        self.search_entry = ttk.Entry(sr, width=36, textvariable=self.search_var)
        self.search_entry.pack(side="left", padx=(0, 6))
        self.search_entry.bind("<Return>", lambda e: self.apply_search())

        ttk.Button(sr, text="Search", style="App.TButton", command=self.apply_search).pack(side="left", padx=(0, 6))
        ttk.Button(sr, text="Clear", style="App.TButton", command=self.clear_search).pack(side="left", padx=(0, 8))
        self.search_hits = ttk.Label(sr, text="", style="App.TLabel")
        self.search_hits.pack(side="left")

        # Table area
        content = ttk.Frame(parent, style="Card.TFrame")
        content.pack(fill="both", expand=True, padx=10, pady=10)

        content.columnconfigure(0, weight=1)
        content.rowconfigure(0, weight=1)

        self.tree = ttk.Treeview(content, columns=(), show="headings", style="App.Treeview")
        self.tree.grid(row=0, column=0, sticky="nsew")

        vsb = ttk.Scrollbar(content, orient="vertical", style="App.Vertical.TScrollbar", command=self.tree.yview)
        hsb = ttk.Scrollbar(content, orient="horizontal", style="App.Horizontal.TScrollbar", command=self.tree.xview)
        self.tree.configure(yscrollcommand=vsb.set, xscrollcommand=hsb.set)

        vsb.grid(row=0, column=1, sticky="ns")
        hsb.grid(row=1, column=0, sticky="ew")

        # Bindings
        self.tree.bind("<Button-1>", self._on_tree_heading_click, add="+")
        self.tree.bind("<Button-3>", self._on_tree_right_click)
        self.tree.bind("<<TreeviewSelect>>", self._on_tree_select)
        self.tree.bind("<Control-MouseWheel>", lambda e: self._on_zoom("csv", e))

        self._build_tree_menu()

    # ---- Search logic ----
    def apply_search(self) -> None:
        """Filter the current CSV view by the entered search text (case-insensitive, any column)."""
        if self.data_full is None:
            return
        q = self.search_var.get().strip().lower()
        if not q:
            self.data_view = list(self.data_full)
            self._populate_tree()
            self.search_hits.config(text="")
            self.status.set("Search cleared.")
            return

        def row_matches(row: Dict[str, str]) -> bool:
            for v in row.values():
                try:
                    if q in str(v).lower():
                        return True
                except Exception:
                    continue
            return False

        self.data_view = [r for r in self.data_full if row_matches(r)]
        self._populate_tree()
        self.search_hits.config(text=f"({len(self.data_view)} matches)")
        self.status.set(f"Filtered by '{q}' → {len(self.data_view)} rows")

    def clear_search(self) -> None:
        self.search_var.set("")
        if self.data_full is None:
            self.search_hits.config(text="")
            return
        self.data_view = list(self.data_full)
        self._populate_tree()
        self.search_hits.config(text="")
        self.status.set("Search cleared.")

    # ---- Context menu ----
    def _build_tree_menu(self) -> None:
        self.tree_menu = tk.Menu(self, tearoff=False, bg=self.P["MENU_BG"], fg=self.P["MENU_FG"],
                                 activebackground=self.P["MENU_ACTIVE_BG"], activeforeground=self.P["MENU_ACTIVE_FG"])
        self.tree_menu.add_command(label="Copy Cell", command=lambda: self._copy_selection(kind="cell"))
        self.tree_menu.add_command(label="Copy Row", command=lambda: self._copy_selection(kind="row"))
        self.tree_menu.add_separator()
        self.tree_menu.add_command(label="Autosize Column", command=self._autosize_current_column)

    def _on_tree_right_click(self, event: tk.Event) -> None:
        try:
            rowid = self.tree.identify_row(event.y)
            if rowid:
                self.tree.selection_set(rowid)
            self._tree_rc_col = self.tree.identify_column(event.x)
            self.tree_menu.tk_popup(event.x_root, event.y_root)
        finally:
            self.tree_menu.grab_release()

    def _copy_selection(self, kind: str = "cell") -> None:
        sel = self.tree.selection()
        if not sel:
            return
        item = sel[0]
        vals = self.tree.item(item, "values")
        if kind == "row":
            text = ",".join(map(str, vals))
        else:
            try:
                idx = max(int(getattr(self, "_tree_rc_col", "#1").replace("#", "")) - 1, 0)
            except Exception:
                idx = 0
            text = str(vals[idx]) if idx < len(vals) else ""
        self.clipboard_clear()
        self.clipboard_append(text)
        self.status.set("Copied to clipboard.")

    def _autosize_current_column(self) -> None:
        try:
            idx = max(int(getattr(self, "_tree_rc_col", "#1").replace("#", "")) - 1, 0)
        except Exception:
            idx = 0
        if not (0 <= idx < len(self.columns)):
            return
        col = self.columns[idx]
        dataset = self.data_view or []
        sample = [col] + [clean_value(r.get(col, "")) for r in dataset[:ROW_SAMPLE_FOR_WIDTH]]
        width = int(max(MIN_COL_W, min(MAX_COL_W, max(len(s) for s in sample) * COL_CHAR_PX)))
        self.tree.column(col, width=width)

    def _autosize_all_columns(self) -> None:
        if not (self.columns and self.data_view):
            return
        for col in self.columns:
            sample = [col] + [clean_value(r.get(col, "")) for r in self.data_view[:ROW_SAMPLE_FOR_WIDTH]]
            width = int(max(MIN_COL_W, min(MAX_COL_W, max(len(s) for s in sample) * COL_CHAR_PX)))
            self.tree.column(col, width=width, anchor="w")

    def _on_tree_heading_click(self, event: tk.Event) -> None:
        region = self.tree.identify("region", event.x, event.y)
        if region != "heading":
            return
        colid = self.tree.identify_column(event.x)
        try:
            idx = int(colid.replace("#", "")) - 1
        except Exception:
            return
        if not (0 <= idx < len(self.columns)):
            return
        column = self.columns[idx]
        self._sort_by_column(column)

    def _sort_by_column(self, column: str) -> None:
        if not self.data_view:
            return
        asc = self._sort_state.get(column, True)
        self._sort_state[column] = not asc

        def key(row: Dict[str, str]):
            val = row.get(column)
            try:
                return float(str(val).replace(",", ""))
            except Exception:
                return str(val)

        self.data_view.sort(key=key, reverse=not asc)
        self._populate_tree()
        self.status.set(f"Sorted by {column} ({'↑' if asc else '↓'}).")

    # ===================== TERMINAL =====================
    def _build_terminal(self, parent: tk.Misc) -> None:
        for w in parent.winfo_children():
            w.destroy()

        header = ttk.Frame(parent, style="Card.TFrame")
        header.pack(fill="x", padx=12, pady=(10, 6))
        ttk.Label(header, text="TERMINAL", style="AppTitle.TLabel").pack(side="left")

        self.start_var = tk.StringVar(value="")
        self.start_combo = ttk.Combobox(header, state="readonly", width=28, textvariable=self.start_var, style="App.TCombobox")
        self.start_combo.pack(side="left", padx=(12, 6))
        ttk.Button(header, text="Start", style="App.TButton", command=self._terminal_start_selected).pack(side="left")
        ttk.Button(header, text="Stop", style="App.TButton", command=self.stop_running).pack(side="left", padx=(6, 0))

        self.term_text = ScrolledText(
            parent,
            height=10,
            wrap="word",
            font=(best_mono(), self.font_sizes["terminal"]),
            bg=self.P["INK"],
            fg=self.P["FG"],
            insertbackground=self.P["FG"],
            relief="flat",
            padx=8, pady=8
        )
        self.term_text.pack(fill="both", expand=True, padx=12, pady=(0, 8))
        self.term_text.bind("<Control-MouseWheel>", lambda e: self._on_zoom("terminal", e))
        self.term_text.insert("end", "Press a Button (left) or pick one above and click Start.\n")

        input_row = ttk.Frame(parent, style="Card.TFrame")
        input_row.pack(fill="x", padx=12, pady=(0, 12))
        ttk.Label(input_row, text="›", style="AppTitle.TLabel").pack(side="left", padx=(0, 8))
        self.term_entry = tk.Entry(input_row, bg=self.P["INK"], fg=self.P["FG"], insertbackground=self.P["FG"], relief="flat")
        self.term_entry.pack(side="left", fill="x", expand=True)
        ttk.Button(input_row, text="Send", style="App.TButton", command=self._send_terminal_input).pack(side="left", padx=(8, 0))
        self.term_entry.bind("<Return>", lambda e: self._send_terminal_input())

    def _terminal_start_selected(self) -> None:
        sel = self.start_var.get().strip()
        if not sel:
            return
        try:
            n = int(sel.split(":")[0])
        except Exception:
            return
        self.on_button_clicked(n)

    # ===================== Layout snap =====================
    def _snap_initial_layout(self) -> None:
        total_w = self.bottom_split.winfo_width()
        total_h = self.bottom_split.winfo_height()
        if total_w <= 0 or total_h <= 0:
            self.after(80, self._snap_initial_layout)
            return
        # Left vs Right width
        csv_w = int(total_w * DEFAULT_CSV_RATIO)
        right_w = max(MIN_RIGHT_W_RESTORE, total_w - csv_w)
        try:
            self.bottom_split.sashpos(0, total_w - right_w)
        except Exception:
            pass
        # Right column: TXS top, Terminal bottom
        rh = self.right_split.winfo_height()
        if rh <= 0:
            rh = total_h
        txs_h = int(rh * (1.0 - DEFAULT_TERM_RATIO))
        txs_h = max(MIN_TXS_H_RESTORE, txs_h)
        try:
            self.right_split.sashpos(0, txs_h)
        except Exception:
            pass

    # ===================== CSV / Stats =====================
    def load_csv_dialog(self) -> None:
        path = filedialog.askopenfilename(title="Select CSV", filetypes=[("CSV files", "*.csv"), ("All files", "*.*")])
        if not path:
            return
        self._load_csv_from_path(path)
        self.current_csv_path = path
        self.status.set(f"Loaded {os.path.basename(path)}")

    def _load_startup_csv_if_any(self) -> None:
        if self._session_loaded:
            return
        try:
            if STARTUP_CSV and os.path.exists(STARTUP_CSV):
                self._load_csv_from_path(STARTUP_CSV)
                self.current_csv_path = STARTUP_CSV
                self.status.set(f"Loaded {os.path.basename(STARTUP_CSV)}")
        except Exception:
            pass

    def _read_csv_records(self, path: str) -> tuple[list[str], list[dict[str, str]]]:
        if PREFER_PANDAS and (pd is not None):
            df = pd.read_csv(path, dtype=str, keep_default_na=False, na_filter=False)
            df = df.fillna("")
            cols = [str(c) for c in df.columns]
            recs = [{c: clean_value(v) for c, v in row.items()} for row in df.to_dict(orient="records")]
            return cols, recs
        with open(path, "r", newline="", encoding="utf-8") as f:
            try:
                sample = f.read(4096)
                f.seek(0)
                dialect = csv.Sniffer().sniff(sample)
            except Exception:
                dialect = csv.excel
            r = csv.DictReader(f, dialect=dialect)
            cols = r.fieldnames or []
            recs = [{k: clean_value(v) for k, v in row.items()} for row in r]
            return cols, recs

    def _load_csv_from_path(self, path: str) -> None:
        try:
            cols, recs = self._read_csv_records(path)
            self.columns = [str(c) for c in cols]
            self.data_full = recs
            self.data_view = list(recs)
        except Exception as e:
            messagebox.showerror("CSV Error", f"Failed to load CSV:\n{e}")
            return
        self.kpi_file.config(text=os.path.basename(path))
        self._populate_tree()
        self._update_txs_initial()
        self.refresh_stats()
        # reset search
        if hasattr(self, "search_var"):
            self.search_var.set("")
            self.search_hits.config(text="")

    def sync_csv(self) -> None:
        path = self.current_csv_path or STARTUP_CSV
        if not path:
            messagebox.showinfo("Sync", "No CSV path available to sync.")
            return
        if not os.path.exists(path):
            messagebox.showerror("Sync", f"CSV not found:\n{path}")
            return
        self._load_csv_from_path(path)
        self.current_csv_path = path
        self.status.set(f"Synchronized from {os.path.basename(path)}")

    def clear_csv(self) -> None:
        self.data_full = None
        self.data_view = None
        self.columns = []
        if hasattr(self, "tree"):
            for i in self.tree.get_children():
                self.tree.delete(i)
            self.tree["columns"] = ()
        self.txs_text.delete("1.0", "end")
        self.txs_text.insert("end", "Load a CSV to see row details here.\n")
        self.term_text.delete("1.0", "end")
        self.term_text.insert("end", "Press a Button (left) or pick one above and click Start.\n")
        self.kpi_file.config(text="No file loaded")
        self._update_overview_info()
        if hasattr(self, "search_hits"):
            self.search_hits.config(text="")
        if hasattr(self, "search_var"):
            self.search_var.set("")
        self.status.set("Cleared.")

    def export_table_csv(self) -> None:
        dataset = self.data_view or []
        if not dataset:
            messagebox.showinfo("Export", "No data to export. Load a CSV first.")
            return
        path = filedialog.asksaveasfilename(
            defaultextension=".csv", filetypes=[("CSV", "*.csv")], title="Export Table as CSV"
        )
        if not path:
            return
        try:
            with open(path, "w", newline="", encoding="utf-8") as f:
                w = csv.DictWriter(f, fieldnames=self.columns)
                w.writeheader()
                for row in dataset:
                    w.writerow({k: clean_value(row.get(k, "")) for k in self.columns})
            self.status.set(f"Exported table to {os.path.basename(path)}")
        except Exception as e:
            messagebox.showerror("Export Error", str(e))

    def _populate_tree(self) -> None:
        dataset = self.data_view or []
        self.tree["columns"] = self.columns

        for c in self.columns:
            self.tree.heading(c, text=c)
            self.tree.column(c, width=max(MIN_COL_W, min(MAX_COL_W, int(len(c) * COL_CHAR_PX))), anchor="w")

        for i in self.tree.get_children():
            self.tree.delete(i)

        for idx, row in enumerate(dataset[:MAX_TREE_ROWS]):
            tag = "odd" if idx % 2 else "even"
            values = [clean_value(row.get(c, "")) for c in self.columns]
            self.tree.insert("", "end", values=values, tags=(tag,))

        self.tree.tag_configure("odd", background=self.P["INK"])
        self.tree.tag_configure("even", background=self.P["ROW_ALT"])

        if AUTOSIZE_ALL_ON_LOAD:
            self._autosize_all_columns()

    # ---------- TXS DETAIL formatting ----------
    def _format_amounts(self, text: str) -> List[str]:
        out: List[str] = []
        if not text or str(text).strip().lower() == "nan":
            return out
        raw = str(text).replace(",", " ").replace(";", " ; ").split(";")
        for chunk in raw:
            s = chunk.strip()
            if not s:
                continue
            out.append(s)
        return out

    def _format_txs_detail(self, row: Dict[str, str]) -> str:
        def short_hash(h: str) -> str:
            if not h:
                return ""
            s = str(h)
            return s if len(s) <= 12 else s[:6] + "…" + s[-4:]

        def normalize_sent(chunk: str) -> str:
            t = chunk.strip()
            if t and t[0] in "+-" and (len(t) > 1 and (t[1].isdigit() or t[1] == ".")):
                return t[1:].lstrip()
            return t

        def normalize_received(chunk: str) -> str:
            t = chunk.strip()
            if not t:
                return t
            if t[0] in "+-" and (len(t) > 1 and (t[1].isdigit() or t[1] == ".")):
                sign = "+" if t[0] == "+" else "-"
                return f"{sign} {t[1:].lstrip()}"
            if t[0].isdigit() or t[0] == ".":
                return f"+ {t}"
            return t

        typ = clean_value(row.get("type") or row.get("tx_type") or "")
        when = clean_value(row.get("tx_timestamp") or row.get("timestamp") or row.get("time") or "")
        hsh = clean_value(row.get("short_tx_hash") or row.get("tx_hash") or row.get("hash") or "")
        sent = self._format_amounts(clean_value(row.get("amount_sent") or row.get("out") or ""))
        recv = self._format_amounts(clean_value(row.get("amount_received") or row.get("in") or ""))
        nft  = clean_value(row.get("nft_transfere") or row.get("nft_transfer") or "")

        lines: List[str] = []
        header_bits = [b for b in (when, short_hash(hsh)) if b]
        if header_bits:
            lines.append("  ".join(header_bits))
            lines.append("")

        type_line = (typ or "TX").replace("_", " ").upper()
        lines.append(type_line)
        lines.append("")

        if sent:
            lines.append("Sent:")
            for s in sent:
                lines.append(f"  – {normalize_sent(s)}")
        if recv:
            if sent:
                lines.append("")
            lines.append("Received:")
            for s in recv:
                lines.append(f"  – {normalize_received(s)}")

        if nft:
            if not sent and not recv:
                lines.append("Details:")
            lines.append(f"  – NFT: {nft}")

        return "\n".join(lines).rstrip() or "(no details)"

    def _update_txs_initial(self) -> None:
        self.txs_text.delete("1.0", "end")
        if not self.data_view:
            self.txs_text.insert("end", "No data loaded.\n")
            return
        first = self.data_view[0]
        self.txs_text.insert("end", self._format_txs_detail(first))

    def _on_tree_select(self, _event=None):
        sel = self.tree.selection()
        if not sel:
            return
        item = sel[0]
        vals = self.tree.item(item, "values")
        row = {k: clean_value(vals[i] if i < len(vals) else "") for i, k in enumerate(self.columns)}
        self.txs_text.delete("1.0", "end")
        self.txs_text.insert("end", self._format_txs_detail(row))

    # ---------- Stats ----------
    def refresh_stats(self) -> None:
        self._update_overview_info()

    def _update_overview_info(self) -> None:
        wallet = self.config_data.get("wallet_address", "")
        dataset = self.data_view or []
        last = first = None
        time_col: Optional[str] = None
        for c in self.columns:
            lc = c.lower()
            if "time" in lc:
                time_col = c
                break
        if dataset and time_col:
            times = []
            for row in dataset:
                t = row.get(time_col)
                if not t:
                    continue
                try:
                    if pd:
                        dt = pd.to_datetime(t)
                        if pd.isna(dt):
                            continue
                        times.append(dt.to_pydatetime() if hasattr(dt, "to_pydatetime") else dt)
                    else:
                        times.append(datetime.fromisoformat(str(t)))
                except Exception:
                    continue
            if times:
                last = max(times)
                first = min(times)
        if last and first:
            self.kpi_overview.config(
                text=f"{wallet} last {last.strftime('%Y-%m-%d %H:%M')} first {first.strftime('%Y-%m-%d %H:%M')}"
            )
        else:
            self.kpi_overview.config(text=wallet)

    # ===================== Buttons / Terminal (process) =====================
    def on_button_clicked(self, number: int):
        mapping = self.config_data["buttons"][str(number)]
        path = mapping.get("path") or ""
        if not path:
            if not messagebox.askyesno("Configure", f"No script set for {mapping.get('label', f'Button {number}')}. Set it now?"):
                return
            self.assign_script_to_button(number)
            mapping = self.config_data["buttons"][str(number)]

        try:
            cmd, cwd = build_run_command(mapping, self.current_csv_path)
        except Exception as e:
            messagebox.showerror("Run", str(e))
            return

        label = mapping.get("label", f"Button {number}")
        hdr = f"\n=== {label} ===\n" if SHOW_BUTTON_HEADER else ""
        self._last_started_button = number
        self.start_process(cmd, cwd, header=hdr)

    def assign_script_to_button(self, number: int):
        f = filedialog.askopenfilename(
            title=f"Choose script for {self.config_data['buttons'][str(number)]['label']}",
            filetypes=[("Python", "*.py"), ("All files", "*.*")]
        )
        if not f:
            return
        mapping = self.config_data["buttons"][str(number)]
        mapping["path"] = f
        save_config(self.config_data)
        self._refresh_sidebar_labels()
        messagebox.showinfo("Configured", f"{mapping.get('label', f'Button {number}')} set to:\n{f}")

    def configure_buttons(self):
        win = tk.Toplevel(self)
        win.title("Configure Buttons")
        win.geometry("1120x640")
        win.grab_set()
        s = ttk.Style(win)
        s.configure("Cfg.TFrame", background=self.P["CARD"])

        wrap = ttk.Frame(win, style="Cfg.TFrame")
        wrap.pack(fill="both", expand=True)

        cols = ("btn", "label", "path", "args", "pass_csv", "prefer_module", "force_module", "cwd")
        tv = ttk.Treeview(wrap, columns=cols, show="headings", height=16, style="App.Treeview")
        widths = (60, 180, 380, 240, 110, 130, 130, 260)
        for c, w in zip(cols, widths):
            tv.heading(c, text=c.upper())
            tv.column(c, width=w, anchor="w", stretch=True)
        tv.pack(fill="both", expand=True, padx=8, pady=8)

        def refresh_tv():
            tv.delete(*tv.get_children())
            for i in range(1, 10 + 1):
                m = self.config_data["buttons"][str(i)]
                tv.insert(
                    "",
                    "end",
                    iid=str(i),
                    values=(i, m.get("label", f"Button {i}"), m["path"], m["args"], m["pass_csv"], m["prefer_module"], m["force_module"], m["cwd"])
                )
        refresh_tv()

        btns = ttk.Frame(wrap, style="Cfg.TFrame")
        btns.pack(fill="x", padx=8, pady=(0, 8))

        def ensure_sel() -> Optional[int]:
            sel = tv.selection()
            if not sel:
                return None
            return int(sel[0])

        def browse():
            idx = ensure_sel()
            if not idx:
                return
            p = filedialog.askopenfilename(title=f"Script for Button {idx}", filetypes=[("Python", "*.py"), ("All", "*.*")])
            if not p:
                return
            self.config_data["buttons"][str(idx)]["path"] = p
            refresh_tv()

        def edit_field(field):
            idx = ensure_sel()
            if not idx:
                return
            cur = self.config_data["buttons"][str(idx)][field]
            val = self.simple_prompt(win, f"Set {field} for Button {idx}", cur)
            if val is None:
                return
            if field in ("pass_csv", "prefer_module", "force_module"):
                v = str(val).strip().lower()
                val = v in ("1", "true", "yes", "y", "on")
            self.config_data["buttons"][str(idx)][field] = val
            refresh_tv()
            if field in ("label", "path"):
                self._refresh_sidebar_labels()
            save_config(self.config_data)

        ttk.Button(btns, text="Browse Script", style="App.TButton", command=browse).pack(side="left")
        ttk.Button(btns, text="Edit Label", style="App.TButton", command=lambda: edit_field("label")).pack(side="left", padx=6)
        ttk.Button(btns, text="Edit Args", style="App.TButton", command=lambda: edit_field("args")).pack(side="left")
        ttk.Button(btns, text="Toggle pass_csv", style="App.TButton", command=lambda: edit_field("pass_csv")).pack(side="left", padx=6)
        ttk.Button(btns, text="Toggle prefer_module", style="App.TButton", command=lambda: edit_field("prefer_module")).pack(side="left")
        ttk.Button(btns, text="Toggle force_module", style="App.TButton", command=lambda: edit_field("force_module")).pack(side="left", padx=6)
        ttk.Button(btns, text="Edit CWD", style="App.TButton", command=lambda: edit_field("cwd")).pack(side="left")

        ttk.Button(btns, text="Save", style="App.TButton", command=lambda: (save_config(self.config_data), self._refresh_sidebar_labels(), win.destroy())).pack(side="right")

    def start_process(self, cmd, cwd, header=""):
        if self.proc and self.proc.poll() is not None:
            self.proc = None

        if self.proc and self.proc.poll() is None:
            if not messagebox.askyesno("Running", "A process is still running. Stop it and start the new one?"):
                return
            self.stop_running()

        if header:
            self.term_text.insert("end", header)
        if SHOW_RUN_CMD:
            self.term_text.insert("end", f"[cwd] {cwd}\n[cmd] {' '.join(cmd)}\n\n")
        self.term_text.see("end")

        try:
            creation = subprocess.CREATE_NO_WINDOW if os.name == "nt" else 0
        except Exception:
            creation = 0

        try:
            self.proc = subprocess.Popen(
                cmd,
                cwd=cwd,
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                bufsize=1,
                universal_newlines=True,
                creationflags=creation
            )
        except Exception as e:
            messagebox.showerror("Launch error", str(e))
            return

        def reader():
            try:
                assert self.proc is not None and self.proc.stdout is not None
                for ch in iter(lambda: self.proc.stdout.read(1), ""):
                    self.q.put(ch)
            except Exception as e:
                self.q.put(f"\n[reader error] {e}\n")
            finally:
                try:
                    rc = self.proc.wait()  # type: ignore
                except Exception:
                    rc = -1
                if SHOW_EXIT_NOTICE:
                    self.q.put(f"\n[exit] code {rc}\n")

        self.reader_thread = threading.Thread(target=reader, daemon=True)
        self.reader_thread.start()
        self.after(20, self._drain_terminal_queue)

        try:
            self.term_entry.focus_set()
        except Exception:
            pass

    def _drain_terminal_queue(self):
        try:
            while True:
                ch = self.q.get_nowait()
                self.term_text.insert("end", ch)
                self.term_text.see("end")
        except queue.Empty:
            pass
        if self.proc and self.proc.poll() is None:
            self.after(20, self._drain_terminal_queue)

    def _send_terminal_input(self):
        text = self.term_entry.get()
        if text is None:
            text = ""
        self.term_entry.delete(0, "end")
        if self.proc and self.proc.poll() is None and self.proc.stdin:
            try:
                self.proc.stdin.write(text + "\n")
                self.proc.stdin.flush()
            except Exception as e:
                self.term_text.insert("end", f"\n[stdin error] {e}\n")
        self.term_text.insert("end", f"\n› {text}\n")
        self.term_text.see("end")

    def stop_running(self):
        if not self.proc or self.proc.poll() is not None:
            return
        self.term_text.insert("end", "\n[stop] stopping…\n")
        try:
            if os.name == "nt":
                try:
                    self.proc.send_signal(signal.CTRL_BREAK_EVENT)  # type: ignore
                except Exception:
                    pass
                self.proc.terminate()
                try:
                    self.proc.kill()
                except Exception:
                    pass
            else:
                self.proc.terminate()
        except Exception:
            pass

    # ===================== Session Save/Load =====================
    def _build_session_payload(self) -> dict:
        try:
            main_pos = self.main_pane.sashpos(0)
        except Exception:
            main_pos = SIDEBAR_W
        try:
            bottom_main = self.bottom_split.sashpos(0)  # CSV | RIGHT
        except Exception:
            bottom_main = 900
        try:
            right_div = self.right_split.sashpos(0)     # TXS over TERMINAL
        except Exception:
            right_div = 300

        tree_widths = {}
        if hasattr(self, "tree") and self.columns:
            for c in self.columns:
                try:
                    tree_widths[c] = self.tree.column(c, "width")
                except Exception:
                    pass

        tree_y = tree_x = 0.0
        selected_index = None
        try:
            tree_y = float(self.tree.yview()[0])
            tree_x = float(self.tree.xview()[0])
            sel = self.tree.selection()
            if sel:
                selected_index = int(self.tree.index(sel[0]))
        except Exception:
            pass

        txs_text = self.txs_text.get("1.0", "end-1c")
        terminal_text = self.term_text.get("1.0", "end-1c")
        if len(terminal_text) > MAX_TERMINAL_SAVE_CHARS:
            terminal_text = terminal_text[-MAX_TERMINAL_SAVE_CHARS:]
        try:
            txs_y = float(self.txs_text.yview()[0])
        except Exception:
            txs_y = 0.0
        try:
            term_y = float(self.term_text.yview()[0])
        except Exception:
            term_y = 0.0

        sess = {
            "theme": self._theme_current,
            "geometry": self.geometry(),
            "window_state": self.state(),
            "csv_file": self.current_csv_path,
            "sashes": {"main": main_pos, "bottom_main": bottom_main, "right_div": right_div},
            "tree_widths": tree_widths,
            "tree_scroll": {"x": tree_x, "y": tree_y},
            "tree_selection_index": selected_index,
            "sort_state": self._sort_state,
            "ui": {"font_sizes": self.font_sizes},
            "texts": {"txs": txs_text, "txs_y": txs_y, "terminal": terminal_text, "terminal_y": term_y},
        }
        return sess

    def quick_save_session(self) -> None:
        try:
            sess = self._build_session_payload()
            with open(DEFAULT_SESSION_PATH, "w", encoding="utf-8") as f:
                json.dump(sess, f, indent=2)
            self.status.set(f"Session saved → {DEFAULT_SESSION_PATH.name}")
        except Exception as e:
            messagebox.showerror("Save Session Error", str(e))

    def save_session_as(self) -> None:
        path = filedialog.asksaveasfilename(
            defaultextension=".ambersession",
            filetypes=[("Amber Session", "*.ambersession"), ("JSON", "*.json"), ("All files", "*.*")],
            initialfile=DEFAULT_SESSION_NAME,
            initialdir=str(APP_DIR),
            title="Save Session As",
        )
        if not path:
            return
        try:
            sess = self._build_session_payload()
            with open(path, "w", encoding="utf-8") as f:
                json.dump(sess, f, indent=2)
            self.status.set(f"Saved session → {os.path.basename(path)}")
        except Exception as e:
            messagebox.showerror("Save Session Error", str(e))

    def _restore_sashes_with_retries(self, sashes: dict, tries: int = 8, delay_ms: int = 120) -> None:
        def attempt(left: int) -> None:
            total_w = self.bottom_split.winfo_width()
            total_h = self.bottom_split.winfo_height()
            if total_w <= 0 or total_h <= 0:
                if left > 0:
                    self.after(delay_ms, lambda: attempt(left - 1))
                return

            main = int(sashes.get("main", SIDEBAR_W))
            main = max(180, min(main, self.main_pane.winfo_width() - 240))
            try:
                self.main_pane.sashpos(0, main)
            except Exception:
                pass

            bm = int(sashes.get("bottom_main", int(total_w * DEFAULT_CSV_RATIO)))
            bm = max(400, min(bm, total_w - MIN_RIGHT_W_RESTORE))
            try:
                self.bottom_split.sashpos(0, bm)
            except Exception:
                pass

            rh = self.right_split.winfo_height()
            if rh <= 0:
                rh = total_h
            rv = int(sashes.get("right_div", int(rh * (1.0 - DEFAULT_TERM_RATIO))))
            rv = max(MIN_TXS_H_RESTORE, min(rv, rh - MIN_TERMINAL_H_RESTORE))
            try:
                self.right_split.sashpos(0, rv)
            except Exception:
                pass

            if left > 0:
                self.after(delay_ms, lambda: attempt(left - 1))

        attempt(tries)

    def _apply_session(self, sess: dict) -> None:
        geo = sess.get("geometry")
        if isinstance(geo, str):
            self.geometry(geo)
        st = sess.get("window_state")
        try:
            if isinstance(st, str):
                self.state(st)
        except Exception:
            pass

        th = sess.get("theme") or "Amber"
        self._apply_theme_choice(th)

        fs = sess.get("ui", {}).get("font_sizes", {})
        for key in ("csv", "txs", "terminal"):
            try:
                self.font_sizes[key] = max(8, min(28, int(fs.get(key, self.font_sizes[key]))))
            except Exception:
                pass
        self.font_size = self.font_sizes["csv"]
        self._init_styles()
        self.txs_text.configure(font=(best_mono(), self.font_sizes["txs"]))
        self.term_text.configure(font=(best_mono(), self.font_sizes["terminal"]))

        csv_path = sess.get("csv_file")
        if csv_path and os.path.exists(csv_path):
            self._load_csv_from_path(csv_path)
            self.current_csv_path = csv_path

        sashes = sess.get("sashes", {})
        self._restore_sashes_with_retries(sashes)

        widths = sess.get("tree_widths") or {}
        for c, tw in widths.items():
            try:
                self.tree.column(c, width=int(tw))
            except Exception:
                pass

        st_map = sess.get("sort_state")
        if isinstance(st_map, dict):
            self._sort_state = st_map

        def _restore_tree_scrolling():
            ts = sess.get("tree_scroll", {})
            try:
                self.tree.xview_moveto(float(ts.get("x", 0.0)))
                self.tree.yview_moveto(float(ts.get("y", 0.0)))
            except Exception:
                pass
            idx = sess.get("tree_selection_index")
            if idx is not None and isinstance(idx, int):
                try:
                    iid = self.tree.get_children()[idx]
                    self.tree.selection_set(iid)
                    self.tree.see(iid)
                    self._on_tree_select()
                except Exception:
                    pass
        self.after(220, _restore_tree_scrolling)

        texts = sess.get("texts", {})
        term_txt = texts.get("terminal", texts.get("calc"))
        if isinstance(self.txs_text, tk.Text) and isinstance(texts.get("txs"), str):
            self.txs_text.delete("1.0", "end")
            self.txs_text.insert("end", texts["txs"])
        if isinstance(self.term_text, tk.Text) and isinstance(term_txt, str):
            self.term_text.delete("1.0", "end")
            self.term_text.insert("end", term_txt)

        def _restore_text_scroll():
            try:
                self.txs_text.yview_moveto(float(texts.get("txs_y", 0.0)))
            except Exception:
                pass
            try:
                self.term_text.yview_moveto(float(texts.get("terminal_y", texts.get("calc_y", 0.0))))
            except Exception:
                pass
        self.after(260, _restore_text_scroll)

        self.status.set("Loaded session")

    def load_session(self) -> None:
        path = filedialog.askopenfilename(
            title="Load Session",
            filetypes=[("Amber Session", "*.ambersession"), ("JSON", "*.json"), ("All files", "*.*")],
            initialdir=str(APP_DIR),
        )
        if not path:
            return
        try:
            with open(path, "r", encoding="utf-8") as f:
                sess = json.load(f)
            self._apply_session(sess)
        except Exception as e:
            messagebox.showerror("Load Session Error", str(e))

    def _load_last_session_if_any(self) -> None:
        if DEFAULT_SESSION_PATH.exists():
            try:
                with open(DEFAULT_SESSION_PATH, "r", encoding="utf-8") as f:
                    sess = json.load(f)
                self._apply_session(sess)
                self._session_loaded = True
            except Exception:
                self._session_loaded = False

    def _on_zoom(self, target: str, event) -> str:
        delta = 0
        if getattr(event, "delta", 0) != 0:
            delta = 1 if event.delta > 0 else -1
        elif getattr(event, "num", None) in (4, 5):
            delta = 1 if event.num == 4 else -1
        if delta:
            self.adjust_font(target, delta)
        return "break"

    def adjust_font(self, target: str, delta: int):
        size = max(8, min(28, self.font_sizes[target] + delta))
        self.font_sizes[target] = size
        ui_cfg = self.config_data.setdefault("ui", {})
        ui_cfg.setdefault("font_sizes", {})[target] = size
        ui_cfg["font_size"] = self.font_sizes["csv"]
        save_config(self.config_data)
        if target == "csv":
            self.font_size = size
            self._init_styles()
            if hasattr(self, "tree"):
                self.tree.configure(style="App.Treeview")
        elif target == "txs" and hasattr(self, "txs_text"):
            self.txs_text.configure(font=(best_mono(), size))
        elif target == "terminal" and hasattr(self, "term_text"):
            self.term_text.configure(font=(best_mono(), size))
        

    @staticmethod
    def simple_prompt(parent, title, initial_value):
        top = tk.Toplevel(parent)
        top.title(title)
        top.geometry("520x140")
        top.grab_set()
        ttk.Label(top, text=title, style="App.TLabel").pack(anchor="w", padx=8, pady=(8, 0))
        var = tk.StringVar(value=str(initial_value))
        ent = ttk.Entry(top, textvariable=var)
        ent.pack(fill="x", padx=8, pady=8)
        ent.focus_set()

        result = {"val": None}

        def ok():
            result["val"] = var.get()
            top.destroy()

        def cancel():
            result["val"] = None
            top.destroy()

        row = ttk.Frame(top)
        row.pack(fill="x", padx=8, pady=(0, 8))
        ttk.Button(row, text="OK", style="App.TButton", command=ok).pack(side="right")
        ttk.Button(row, text="Cancel", style="App.TButton", command=cancel).pack(side="right", padx=6)
        top.wait_window()
        return result["val"]


# ===================== MAIN =====================
if __name__ == "__main__":
    if sys.platform.startswith("win"):
        try:
            from ctypes import windll  # type: ignore
            windll.shcore.SetProcessDpiAwareness(1)
        except Exception:
            pass
    try:
        app = AmberRunner()
        app.mainloop()
    except Exception as e:
        try:
            messagebox.showerror("Fatal error", str(e))
        except Exception:
            print(f"Fatal error: {e}", file=sys.stderr)
