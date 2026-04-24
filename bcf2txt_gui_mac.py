# -*- coding: utf-8 -*-
"""
VCF/BCF → 纯文本工具  v2.2-mac（MR 目标列版）
输入：.bcf / .vcf.gz / .vcf
输出：.vcf.gz / .txt / .txt.gz / .csv
MR 目标列：chr / pos / SNP / other_allele / effect_allele / beta / se / pval / n / id / trait

Mac 适配版：
  - bcftools 无后缀（Unix 二进制）
  - 字体使用 PingFang SC（macOS 内置中文字体）
  - 无需 DLL，无需 CREATE_NO_WINDOW
"""

import sys
import os
import gzip
import subprocess
import threading
import tkinter as tk
from tkinter import ttk, filedialog, messagebox
from concurrent.futures import ThreadPoolExecutor, as_completed


# ─────────────────────────────────────────────
#  资源路径（PyInstaller 打包后兼容）
# ─────────────────────────────────────────────
def get_resource_path(name: str) -> str:
    if hasattr(sys, "_MEIPASS"):
        return os.path.join(sys._MEIPASS, name)
    return os.path.join(os.path.dirname(os.path.abspath(__file__)), name)


# Mac 版：bcftools 无后缀
BCFTOOLS_EXE = get_resource_path("bcftools")
BINS_DIR = os.path.dirname(get_resource_path("bcftools"))


# ─────────────────────────────────────────────
#  MR 目标列（11列，对应 IEUGWASQ / TwoSampleMR 需求）
# ─────────────────────────────────────────────
MR_COLUMNS = [
    ("chr",           "%CHROM",             "."),
    ("pos",           "%POS",               "."),
    ("SNP",           "%ID",                "."),
    ("other_allele",  "%REF",               "."),
    ("effect_allele", "%ALT",               "."),
    ("beta",          "%INFO_beta",         "."),
    ("se",            "%INFO_se",           "."),
    ("pval",          "%INFO_pval",         "."),
    ("n",             "%INFO_n",            "."),
    ("id",            "%INFO_id",           "."),
    ("trait",         "%INFO_trait",        "."),
]


def build_query_format(delimiter: str) -> str:
    parts = [col[1] for col in MR_COLUMNS]
    return delimiter.join(parts) + "\\n"


def build_tsv_header() -> str:
    cols = [col[0] for col in MR_COLUMNS]
    return "\t".join(cols) + "\n"


# ─────────────────────────────────────────────
#  字体适配：macOS 使用 PingFang SC
# ─────────────────────────────────────────────
FONT_MAIN  = "PingFang SC"
FONT_MONO  = "Menlo"       # macOS 内置等宽字体


# ─────────────────────────────────────────────
#  主窗口
# ─────────────────────────────────────────────
class VCF2TXTApp(tk.Tk):
    BG      = "#F0F2F8"
    CARD    = "#FFFFFF"
    ACCENT  = "#4A7CF7"
    DANGER  = "#E74C3C"
    SECOND  = "#6C757D"

    def __init__(self):
        super().__init__()
        self._selected_files: list[str] = []
        self._converting      = False
        self._cancel_event: threading.Event | None = None

        self._setup_window()
        self._build_ui()

    def _setup_window(self):
        self.title("VCF/BCF → 文本工具  v2.2-mac")
        self.resizable(False, False)
        self.configure(bg=self.BG)

        w, h = 660, 740
        sw = self.winfo_screenwidth()
        sh = self.winfo_screenheight()
        self.geometry(f"{w}x{h}+{(sw-w)//2}+{(sh-h)//2}")

    def _build_ui(self):
        tk.Label(
            self, text="VCF / BCF 二进制 → MR 目标文本",
            font=(FONT_MAIN, 15, "bold"), bg=self.BG, fg="#1A1A2E"
        ).pack(pady=(18, 2))
        tk.Label(
            self,
            text="输入：.bcf / .vcf.gz / .vcf　｜　输出：.vcf.gz / .txt / .txt.gz / .csv",
            font=(FONT_MAIN, 9), bg=self.BG, fg="#888"
        ).pack(pady=(0, 10))

        card_file = self._make_card(padx=22, pady=(0, 4))
        self._build_file_section(card_file)

        card_fmt = self._make_card(padx=22, pady=4)
        self._build_format_section(card_fmt)

        card_thr = self._make_card(padx=22, pady=4)
        self._build_thread_section(card_thr)

        self._convert_btn = tk.Button(
            self, text="  ⚡  开始转换  ",
            font=(FONT_MAIN, 13, "bold"),
            bg=self.ACCENT, fg="white",
            activebackground="#2D62E0", activeforeground="white",
            relief="flat", cursor="hand2", bd=0,
            padx=28, pady=10,
            command=self._start_conversion
        )
        self._convert_btn.pack(pady=(10, 4))

        self._progress = ttk.Progressbar(
            self, mode="determinate", style="Hybrid.Horizontal.TProgressbar"
        )
        self._progress.pack(fill="x", padx=22, pady=(4, 0))

        log_frame = tk.Frame(self, bg=self.BG)
        log_frame.pack(fill="both", expand=True, padx=22, pady=(6, 14))

        tk.Label(log_frame, text="运行日志", font=(FONT_MAIN, 9),
                 bg=self.BG, fg="#888", anchor="w").pack(anchor="w")

        self._log_text = tk.Text(
            log_frame, height=9,
            font=(FONT_MONO, 9), bg="#1E1E2E", fg="#A8FF78",
            insertbackground="white", relief="flat",
            state="disabled", wrap="word", padx=8, pady=6
        )
        self._log_text.pack(fill="both", expand=True)
        self._log("就绪 — 请添加 VCF/BCF 文件后点击「开始转换」。")

    def _make_card(self, padx=22, pady=4):
        f = tk.Frame(self, bg=self.CARD, bd=0, relief="flat",
                     highlightbackground="#D8DCF0", highlightthickness=1)
        f.pack(fill="x", padx=padx, pady=pady)
        return f

    def _build_file_section(self, parent):
        hdr = tk.Frame(parent, bg=self.CARD)
        hdr.pack(fill="x", padx=14, pady=(12, 6))
        tk.Label(hdr, text="文件列表", font=(FONT_MAIN, 10, "bold"),
                 bg=self.CARD, fg="#1A1A2E").pack(side="left")
        self._file_count_lbl = tk.Label(hdr, text="0 个文件",
                 font=(FONT_MAIN, 9), bg=self.CARD, fg="#888")
        self._file_count_lbl.pack(side="right")

        btn_row = tk.Frame(parent, bg=self.CARD)
        btn_row.pack(fill="x", padx=14, pady=(0, 6))

        tk.Button(btn_row, text="📂 选择文件",
                  font=(FONT_MAIN, 9), bg=self.ACCENT, fg="white",
                  activebackground="#2D62E0", activeforeground="white",
                  relief="flat", cursor="hand2", bd=0, padx=10, pady=4,
                  command=self._select_files).pack(side="left", padx=(0, 8))

        tk.Button(btn_row, text="📁 选择文件夹",
                  font=(FONT_MAIN, 9), bg=self.SECOND, fg="white",
                  activebackground="#4A555E", activeforeground="white",
                  relief="flat", cursor="hand2", bd=0, padx=10, pady=4,
                  command=self._select_folder).pack(side="left", padx=(0, 8))

        tk.Button(btn_row, text="🗑️ 清除全部",
                  font=(FONT_MAIN, 9), bg="#E9ECEF", fg="#495057",
                  activebackground="#CED4DA", activeforeground="#495057",
                  relief="flat", cursor="hand2", bd=0, padx=10, pady=4,
                  command=self._clear_files).pack(side="left")

        list_frame = tk.Frame(parent, bg=self.CARD)
        list_frame.pack(fill="x", padx=14, pady=(0, 10))

        scroll_y = tk.Scrollbar(list_frame, orient="vertical")
        scroll_y.pack(side="right", fill="y")

        self._file_list = tk.Listbox(
            list_frame, height=6,
            font=(FONT_MONO, 9), bg="#F8F9FC", fg="#333",
            selectbackground="#D6E4FF", selectforeground="#1A1A2E",
            relief="flat", bd=0, highlightthickness=0,
            yscrollcommand=scroll_y.set, activestyle="none"
        )
        self._file_list.pack(side="left", fill="x", expand=True)
        scroll_y.config(command=self._file_list.yview)

        # Mac：右键菜单用 Button-2 或 Control-Button-1
        self._ctx_menu = tk.Menu(self._file_list, tearoff=0, font=(FONT_MAIN, 9))
        self._ctx_menu.add_command(label="🗑️ 移除此项", command=self._remove_selected)
        self._file_list.bind("<Button-2>", lambda e: self._ctx_menu.post(e.x_root, e.y_root))
        self._file_list.bind("<Control-Button-1>", lambda e: self._ctx_menu.post(e.x_root, e.y_root))

        ttk.Separator(parent, orient="horizontal").pack(fill="x", padx=14)

    def _build_format_section(self, parent):
        tk.Label(parent, text="输出格式", font=(FONT_MAIN, 10, "bold"),
                 bg=self.CARD, fg="#1A1A2E"
        ).pack(anchor="w", padx=14, pady=(12, 4))

        self._fmt_var = tk.StringVar(value="txt")

        formats = [
            ("txt",   "TXT 制表符格式（.txt）      ← Excel/WPS 直接打开，推荐"),
            ("csv",   "CSV 逗号格式（.csv）        ← Excel 直接导入"),
            ("txtgz", "TXT 压缩格式（.txt.gz）     ← 体积小、兼容 Excel"),
            ("vcfgz", "VCF 压缩格式（.vcf.gz）     ← 标准 VCF，体积最小"),
        ]
        for val, text in formats:
            tk.Radiobutton(
                parent, text=text,
                variable=self._fmt_var, value=val,
                font=(FONT_MAIN, 9), bg=self.CARD, fg="#333",
                selectcolor=self.CARD, activebackground=self.CARD,
                cursor="hand2"
            ).pack(anchor="w", padx=14)

        tk.Label(
            parent,
            text="输出列：chr  pos  SNP  other_allele  effect_allele  beta  se  pval  n  id  trait",
            font=(FONT_MONO, 8), bg=self.CARD, fg="#AAA"
        ).pack(anchor="w", padx=14, pady=(2, 10))

    def _build_thread_section(self, parent):
        row = tk.Frame(parent, bg=self.CARD)
        row.pack(fill="x", padx=14, pady=(10, 10))
        tk.Label(row, text="并发线程：", font=(FONT_MAIN, 10),
                 bg=self.CARD, fg="#333").pack(side="left")
        self._threads_var = tk.IntVar(value=4)
        for n in [1, 2, 4, 8]:
            tk.Radiobutton(
                row, text=str(n),
                variable=self._threads_var, value=n,
                font=(FONT_MAIN, 9), bg=self.CARD, fg="#333",
                selectcolor=self.CARD, activebackground=self.CARD,
                cursor="hand2"
            ).pack(side="left", padx=(4, 0))

    def _log(self, msg: str, append: bool = False):
        def _write():
            self._log_text.configure(state="normal")
            if append:
                self._log_text.insert("end", "\n" + msg)
            else:
                self._log_text.delete("1.0", "end")
                self._log_text.insert("end", msg)
            self._log_text.see("end")
            self._log_text.configure(state="disabled")
        self.after(0, _write)

    def _select_files(self):
        paths = filedialog.askopenfilenames(
            title="选择 VCF/BCF 文件（可多选）",
            filetypes=[
                ("VCF/BCF 文件", "*.vcf.gz *.bcf *.vcf"),
                ("压缩 VCF", "*.vcf.gz"),
                ("BCF 二进制", "*.bcf"),
                ("所有文件", "*.*"),
            ]
        )
        if paths:
            self._add_files(paths)

    def _select_folder(self):
        folder = filedialog.askdirectory(
            title="选择文件夹（递归扫描 .vcf.gz / .bcf / .vcf）"
        )
        if folder:
            self._log(f"正在扫描文件夹：{folder}")
            threading.Thread(target=self._scan_folder, args=(folder,), daemon=True).start()

    def _scan_folder(self, folder: str):
        found = []
        for root, _, files in os.walk(folder):
            for f in files:
                if f.lower().endswith((".vcf.gz", ".bcf", ".vcf")):
                    found.append(os.path.join(root, f))
        found = [f for f in found if os.path.isfile(f)]
        self.after(0, lambda: self._add_files(found))
        self.after(0, lambda: self._log(
            f"扫描完成，找到 {len(found)} 个文件。" if found else "未找到任何 VCF/BCF 文件。"
        ))

    def _add_files(self, paths: list[str]):
        for p in paths:
            if p not in self._selected_files and os.path.isfile(p):
                self._selected_files.append(p)
                self._file_list.insert("end", os.path.abspath(p))
        self._file_count_lbl.config(text=f"{len(self._selected_files)} 个文件")

    def _remove_selected(self):
        sel = self._file_list.curselection()
        if sel:
            self._file_list.delete(sel[0])
            self._selected_files.pop(sel[0])
            self._file_count_lbl.config(text=f"{len(self._selected_files)} 个文件")

    def _clear_files(self):
        self._selected_files.clear()
        self._file_list.delete(0, "end")
        self._file_count_lbl.config(text="0 个文件")

    def _validate(self) -> bool:
        if not self._selected_files:
            messagebox.showwarning("未添加文件", "请先添加 VCF/BCF 文件！")
            return False
        valid = [f for f in self._selected_files if os.path.isfile(f)]
        if not valid:
            messagebox.showerror("文件不存在", "所有文件均不存在，请重新添加。")
            return False
        missing = [f for f in self._selected_files if not os.path.isfile(f)]
        if missing:
            names = "\n".join(f"  {os.path.basename(f)}" for f in missing[:5])
            extra = "\n  ..." if len(missing) > 5 else ""
            messagebox.showwarning("部分文件不存在",
                f"以下 {len(missing)} 个文件未找到，将跳过：\n{names}{extra}")
        if not os.path.isfile(BCFTOOLS_EXE):
            messagebox.showerror("内部错误", f"找不到 bcftools！\n\n请重新下载完整程序。")
            return False
        return True

    def _start_conversion(self):
        if self._converting:
            return
        if not self._validate():
            return
        self._converting = True
        self._convert_btn.config(
            state="disabled", text="  ⏳ 转换中…（可取消）  ",
            command=self._cancel_conversion
        )
        self._progress["value"] = 0
        self._cancel_event = threading.Event()
        files = [f for f in self._selected_files if os.path.isfile(f)]
        threading.Thread(target=self._do_convert, args=(files,), daemon=True).start()

    def _cancel_conversion(self):
        if self._cancel_event:
            self._cancel_event.set()
        self._log("⏹ 已发送取消信号，正在停止…")

    def _do_convert(self, files: list[str]):
        fmt_key = self._fmt_var.get()
        threads  = self._threads_var.get()
        total   = len(files)
        success = 0
        failed  = []

        # Mac/Linux 无需 CREATE_NO_WINDOW
        CREATE_NO_WINDOW = 0x08000000 if sys.platform == "win32" else 0

        def convert_one(src: str):
            if self._cancel_event and self._cancel_event.is_set():
                return src, False, "已取消"

            src_dir = os.path.dirname(src)
            stem    = os.path.splitext(os.path.basename(src))[0]
            if stem.lower().endswith(".vcf"):
                stem = stem[:-4]

            suffix_map = {
                "txt":   ".txt",
                "csv":   ".csv",
                "txtgz": ".txt.gz",
                "vcfgz": ".vcf.gz",
            }
            out_file = os.path.join(src_dir, stem + suffix_map.get(fmt_key, ".txt"))

            env = os.environ.copy()
            env["PATH"] = BINS_DIR + os.pathsep + env.get("PATH", "")

            try:
                if fmt_key == "vcfgz":
                    cmd = [BCFTOOLS_EXE, "view", "-Oz", "-o", out_file, src]
                    r = subprocess.run(cmd,
                                       stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                       env=env, creationflags=CREATE_NO_WINDOW)
                    if r.returncode != 0:
                        err = r.stderr.decode("utf-8", errors="replace").strip()
                        return src, False, err or "未知错误"

                else:
                    if fmt_key in ("txt", "txtgz"):
                        delimiter = "\t"
                    else:
                        delimiter = ","

                    q_fmt = build_query_format(delimiter)
                    header = build_tsv_header()
                    tmp_file = out_file + ".tmp_query"

                    q_cmd = [BCFTOOLS_EXE, "query", "-f", q_fmt, "-o", tmp_file, src]
                    r1 = subprocess.run(q_cmd,
                                        stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                        env=env, creationflags=CREATE_NO_WINDOW)
                    if r1.returncode != 0:
                        err = r1.stderr.decode("utf-8", errors="replace").strip()
                        return src, False, err or "bcftools query 失败"

                    if fmt_key == "txtgz":
                        with gzip.open(out_file, "wt", encoding="utf-8") as fout:
                            fout.write(header)
                            with open(tmp_file, "rb") as fin:
                                data = fin.read()
                            fout.write(data.decode("utf-8", errors="replace"))
                        os.remove(tmp_file)

                    else:  # txt / csv
                        with open(out_file, "w", encoding="utf-8", newline="") as fout:
                            fout.write(header)
                            with open(tmp_file, "rb") as fin:
                                data = fin.read()
                            fout.write(data.decode("utf-8", errors="replace"))
                        os.remove(tmp_file)

                return src, True, out_file

            except PermissionError:
                return src, False, "权限不足（尝试将文件移到其他目录后重试）"
            except Exception as e:
                return src, False, str(e)

        self._log(f"开始转换 {total} 个文件（{threads} 线程）…")
        done = 0

        with ThreadPoolExecutor(max_workers=threads) as pool:
            futures = {pool.submit(convert_one, f): f for f in files}
            for future in as_completed(futures):
                if self._cancel_event and self._cancel_event.is_set():
                    break
                src, ok, msg = future.result()
                done += 1
                if ok:
                    success += 1
                    self._log(f"[{done}/{total}] ✅ {os.path.basename(src)}\n   → {msg}", True)
                else:
                    failed.append((src, msg))
                    self._log(f"[{done}/{total}] ❌ {os.path.basename(src)}\n   错误: {msg}", True)
                self._progress.config(value=done / total * 100)

        self._reset_btn()
        cancelled = self._cancel_event and self._cancel_event.is_set()

        if cancelled:
            messagebox.showinfo("已取消",
                f"已取消转换。\n\n成功：{success} 个\n失败：{len(failed)} 个")
        elif failed:
            err_list = "\n".join(f"  {os.path.basename(s)}: {e}" for s, e in failed[:10])
            extra = "\n  ..." if len(failed) > 10 else ""
            messagebox.showwarning("部分失败",
                f"转换完成，{len(failed)} 个文件失败：\n{err_list}{extra}\n\n成功：{success} 个\n"
                f"失败原因：INFO 列缺少 beta/se/pval 等字段，或文件损坏/格式特殊。")
        else:
            messagebox.showinfo("✅ 全部完成",
                f"成功转换 {success} 个文件！\n\n输出目录：与原文件相同\n"
                f"输出格式：.{fmt_key}")

    def _reset_btn(self):
        self._converting = False
        self._cancel_event = None
        self._convert_btn.config(
            state="normal", text="  ⚡  开始转换  ",
            command=self._start_conversion
        )


# ─────────────────────────────────────────────
#  程序入口
# ─────────────────────────────────────────────
def main():
    app = VCF2TXTApp()
    app.mainloop()


if __name__ == "__main__":
    main()
