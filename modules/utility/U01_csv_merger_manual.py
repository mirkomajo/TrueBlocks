import tkinter as tk
from tkinter import filedialog, messagebox
from tkinter import ttk
import pandas as pd
from typing import List

class DragDropListbox(tk.Listbox):
    def __init__(self, master, **kw):
        kw['selectmode'] = tk.SINGLE
        super().__init__(master, **kw)
        self.bind('<Button-1>', self.set_current)
        self.bind('<B1-Motion>', self.shift_selection)
        self.cur_index = None

    def set_current(self, event):
        self.cur_index = self.nearest(event.y)

    def shift_selection(self, event):
        i = self.nearest(event.y)
        if self.cur_index is None:
            return
        if i < 0:
            i = 0
        if i != self.cur_index:
            self.move(self.cur_index, i)
            self.cur_index = i

    def items(self) -> List[str]:
        return [self.get(i) for i in range(self.size())]

    def move(self, from_index, to_index):
        if from_index == to_index:
            return
        item = self.get(from_index)
        self.delete(from_index)
        self.insert(to_index, item)
        self.selection_clear(0, tk.END)
        self.selection_set(to_index)

class CheckList(tk.Frame):
    def __init__(self, master, options: List[str], command=None):
        super().__init__(master)
        self.command = command
        self.vars = {}

        canvas = tk.Canvas(self, borderwidth=0, highlightthickness=0)
        self.frame = tk.Frame(canvas)
        scrollbar = ttk.Scrollbar(self, orient="vertical", command=canvas.yview)
        canvas.configure(yscrollcommand=scrollbar.set)

        scrollbar.pack(side="right", fill="y")
        canvas.pack(side="left", fill="both", expand=True)
        canvas.create_window((0, 0), window=self.frame, anchor="nw")
        self.frame.bind("<Configure>", lambda e: canvas.configure(scrollregion=canvas.bbox("all")))

        for opt in options:
            var = tk.BooleanVar(value=True)
            chk = ttk.Checkbutton(self.frame, text=opt, variable=var, command=lambda o=opt, v=var: self._on_toggle(o, v))
            chk.pack(anchor='w')
            self.vars[opt] = var

    def _on_toggle(self, option, var):
        if self.command:
            self.command(option, var.get())

    def get_selected(self) -> List[str]:
        return [k for k, v in self.vars.items() if v.get()]

    def select_all(self):
        for v in self.vars.values():
            v.set(True)
        if self.command:
            for k in self.vars.keys():
                self.command(k, True)

    def deselect_all(self):
        for v in self.vars.values():
            v.set(False)
        if self.command:
            for k in self.vars.keys():
                self.command(k, False)

class CSVSheetMerger(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("CSV Merger")
        self.geometry("1200x700")

        self.df = pd.DataFrame()
        self.log_entries = []  # store in-app log

        # Top controls
        top = ttk.Frame(self)
        top.pack(fill='x', padx=10, pady=10)

        self.import_btn = ttk.Button(top, text="Import CSV files", command=self.import_csvs)
        self.import_btn.pack(side='left')

        ttk.Separator(top, orient='vertical').pack(side='left', fill='y', padx=8)

        self.select_all_btn = ttk.Button(top, text="Select all columns", command=self._select_all_cols)
        self.select_all_btn.pack(side='left')
        self.deselect_all_btn = ttk.Button(top, text="Deselect all", command=self._deselect_all_cols)
        self.deselect_all_btn.pack(side='left', padx=(6,0))

        ttk.Separator(top, orient='vertical').pack(side='left', fill='y', padx=8)

        self.export_btn = ttk.Button(top, text="Export merged CSV", command=self.export_csv)
        self.export_btn.pack(side='left')

        # Middle layout
        middle = ttk.Frame(self)
        middle.pack(fill='both', expand=True, padx=10, pady=(0,10))

        # Column selection
        left_frame = ttk.LabelFrame(middle, text="Columns (check to keep)")
        left_frame.pack(side='left', fill='y', padx=(0,10))
        self.checklist_container = left_frame
        self.checklist: CheckList | None = None

        # Column order
        center_frame = ttk.LabelFrame(middle, text="Column order (drag to rearrange)")
        center_frame.pack(side='left', fill='y', padx=(0,10))
        self.order_box = DragDropListbox(center_frame, width=35, height=25)
        self.order_box.pack(fill='both', expand=True)

        # Preview
        right_frame = ttk.LabelFrame(middle, text="Preview (first 200 rows)")
        right_frame.pack(side='left', fill='both', expand=True)
        self.preview = ttk.Treeview(right_frame, show='headings', height=25)
        self.preview.pack(fill='both', expand=True)
        self.preview_scroll_x = ttk.Scrollbar(right_frame, orient='horizontal', command=self.preview.xview)
        self.preview_scroll_x.pack(side='bottom', fill='x')
        self.preview_scroll_y = ttk.Scrollbar(right_frame, orient='vertical', command=self.preview.yview)
        self.preview_scroll_y.pack(side='right', fill='y')
        self.preview.configure(xscrollcommand=self.preview_scroll_x.set, yscrollcommand=self.preview_scroll_y.set)

        # Log panel
        log_frame = ttk.LabelFrame(self, text="Action Log (copy to AI agent)")
        log_frame.pack(fill='both', expand=False, padx=10, pady=(0,10))
        self.log_text = tk.Text(log_frame, height=8, wrap='word')
        self.log_text.pack(fill='both', expand=True)

        self.status = tk.StringVar(value='Import CSV files to begin.')
        statusbar = ttk.Label(self, textvariable=self.status, anchor='w')
        statusbar.pack(fill='x', padx=10, pady=(0,10))

    def _log(self, message: str):
        self.log_entries.append(message)
        self.log_text.insert(tk.END, message + "\n")
        self.log_text.see(tk.END)

    def import_csvs(self):
        paths = filedialog.askopenfilenames(title='Select CSV files', filetypes=[('CSV Files', '*.csv'), ('All Files', '*.*')])
        if not paths:
            return
        dfs = []
        for p in paths:
            try:
                dfp = pd.read_csv(p, dtype=str, keep_default_na=False, na_values=[])
                dfs.append(dfp)
                self._log(f"Imported file: {p}")
            except Exception as e:
                self._log(f"Failed to import {p}: {e}")
        if not dfs:
            return
        self.df = pd.concat(dfs, ignore_index=True, sort=False)
        self.df.columns = [str(c) for c in self.df.columns]
        self._build_column_controls()
        self._refresh_preview()
        self.status.set(f"Loaded {len(paths)} file(s). Rows: {len(self.df):,}. Columns: {len(self.df.columns)}.")

    def _build_column_controls(self):
        for child in self.checklist_container.winfo_children():
            child.destroy()
        self.checklist = CheckList(self.checklist_container, list(self.df.columns), command=self._on_checkbox)
        self.checklist.pack(fill='both', expand=True)
        self.order_box.delete(0, tk.END)
        for col in self.df.columns:
            self.order_box.insert(tk.END, col)

    def _on_checkbox(self, col: str, keep: bool):
        if keep:
            self._log(f"Column kept: {col}")
        else:
            self._log(f"Column removed: {col}")
        current = self.order_box.items()
        if keep and col not in current:
            self.order_box.insert(tk.END, col)
        elif not keep and col in current:
            idx = current.index(col)
            self.order_box.delete(idx)
        self._refresh_preview()

    def _select_all_cols(self):
        if self.checklist:
            self.checklist.select_all()
        self._log("All columns selected")
        self._refresh_preview()

    def _deselect_all_cols(self):
        if self.checklist:
            self.checklist.deselect_all()
        self.order_box.delete(0, tk.END)
        self._log("All columns deselected")
        self._refresh_preview()

    def _ordered_selected_columns(self) -> List[str]:
        return self.order_box.items()

    def _refresh_preview(self):
        if self.df.empty:
            return
        cols = self._ordered_selected_columns()
        preview_df = self.df[cols].head(200) if cols else self.df.head(200)[[]]
        self.preview.delete(*self.preview.get_children())
        self.preview['columns'] = list(preview_df.columns)
        for c in preview_df.columns:
            self.preview.heading(c, text=c)
            self.preview.column(c, width=120)
        for _, row in preview_df.iterrows():
            self.preview.insert('', 'end', values=[row[c] for c in preview_df.columns])

    def export_csv(self):
        if self.df.empty:
            messagebox.showinfo("Nothing to export", "Please import at least one CSV file first.")
            return
        cols = self._ordered_selected_columns()
        if not cols:
            if not messagebox.askyesno("No columns selected", "Export empty CSV?"):
                return
        export_df = self.df[cols] if cols else self.df.head(0)
        save_path = filedialog.asksaveasfilename(defaultextension='.csv', filetypes=[('CSV Files', '*.csv')], title='Save merged CSV as...')
        if not save_path:
            return
        try:
            export_df.to_csv(save_path, index=False)
            self._log(f"Exported CSV to: {save_path} with columns {cols}")
        except Exception as e:
            messagebox.showerror("Export failed", str(e))
            self._log(f"Export failed: {e}")
            return
        messagebox.showinfo("Export complete", f"Saved to:\n{save_path}")

if __name__ == '__main__':
    app = CSVSheetMerger()
    app.mainloop()
