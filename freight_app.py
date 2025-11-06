import tkinter as tk
from tkinter import filedialog, messagebox, simpledialog, ttk
import ttkbootstrap as b
from ttkbootstrap.constants import *
from ttkbootstrap.tableview import Tableview
import pandas as pd
import json
from datetime import datetime
from pathlib import Path
import os
import sys
import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
class PriceEditDialog(b.Toplevel):
    """
    A professional-looking dialog for multi-editing prices for a single region.
    """
    def __init__(self, master, region_name, price_df, callback):
        super().__init__(master, title=f"Edit Prices for {region_name}", modal=True)
        self.region_name = region_name
        self.price_df = price_df
        self.callback = callback
        self.entries = {}
        self.transient(master)
        self.grab_set()
        self.focus_force()
        self.geometry("450x350")
        self.columnconfigure(1, weight=1)
        self._create_widgets()
    def _create_widgets(self):
        b.Label(self, text=f"Region: {self.region_name}", font=('TkDefaultFont', 12, 'bold')).grid(row=0, column=0, columnspan=2, padx=10, pady=(10, 15), sticky=W)
        row_num = 1
        for col_name in self.price_df.columns:
            b.Label(self, text=f"{col_name}:").grid(row=row_num, column=0, padx=10, pady=5, sticky=W)
            current_value = self.price_df.at[self.region_name, col_name]
            entry = b.Entry(self, bootstyle="info")
            entry.insert(0, f"{current_value:.2f}")
            entry.grid(row=row_num, column=1, padx=10, pady=5, sticky=EW)
            self.entries[col_name] = entry
            row_num += 1
        b.Button(self, text="Apply Changes", command=self._apply_changes, bootstyle="success").grid(row=row_num, column=0, columnspan=2, pady=15, padx=10, sticky=EW)
    def _apply_changes(self):
        changes_made = False
        new_prices = {}
        for col_name, entry in self.entries.items():
            new_value = entry.get()
            try:
                float_value = float(new_value)
                if float_value < 0:
                    raise ValueError("Price cannot be negative.")
                new_prices[col_name] = float_value
                if self.price_df.at[self.region_name, col_name] != float_value:
                    self.price_df.at[self.region_name, col_name] = float_value
                    changes_made = True
            except ValueError as e:
                messagebox.showerror("Invalid Input", f"Price for '{col_name}' must be a valid non-negative number.\nError: {e}", parent=self)
                return
        if changes_made:
            self.callback(self.region_name)
        self.destroy()
class FreightApp:
    def __init__(self, root):
        self.root = root
        self.root.title("Freight Matrix Pro v1.0")
        self.root.geometry("1200x800")
        self.root.protocol("WM_DELETE_WINDOW", self._on_closing)
        self.base_path = Path.home() / "FreightMatrixApp"
        self.output_path = self.base_path / "Output"
        self.postcodes_file = self.base_path / "postcodes.json"
        self.processed_sku_data = None
        self.price_df = None
        self.region_postcodes = {}
        self.current_state_filter = tk.StringVar(value="All States")
        self.current_pagesize = tk.IntVar(value=50)
        self.changes_saved = tk.BooleanVar(value=True)
        self.paned_window = b.PanedWindow(self.root, orient=HORIZONTAL)
        self.paned_window.pack(fill=BOTH, expand=True, padx=10, pady=10)
        self.control_frame = b.Frame(self.paned_window, width=320)
        self.paned_window.add(self.control_frame, weight=1)
        self.main_frame = b.Frame(self.paned_window)
        self.paned_window.add(self.main_frame, weight=3)
        self._create_control_widgets()
        self._create_main_widgets()
        self.load_postcodes()
        self._initialize_price_data()
        self._populate_price_table()
        self._update_stats()
        self.log("Welcome! 🚀 App initialized. Please load SKU data and set prices.")
        self._check_save_state()
    def _create_control_widgets(self):
        """Creates all widgets for the left-side control panel (Workflow & Stats)."""
        workflow_frame = b.LabelFrame(self.control_frame, text="Workflow Steps", padding=10)
        workflow_frame.pack(fill=X, padx=5, pady=5)
        b.Label(workflow_frame, text="1. Load Product Data").pack(anchor=W)
        b.Button(workflow_frame, text="Upload SKU/Pack-Size File", command=self.upload_sku_file, bootstyle="primary").pack(fill=X, pady=(5, 15))
        b.Label(workflow_frame, text="2. Save Price Matrix Changes").pack(anchor=W)
        self.save_button = b.Button(workflow_frame, text="Confirm & Save Price Data", command=self._confirm_and_save_prices, bootstyle="warning")
        self.save_button.pack(fill=X, pady=5)
        b.Label(workflow_frame, text="3. Choose Output Format(s)").pack(anchor=W)
        self.generate_csv = tk.BooleanVar(value=True)
        self.generate_json = tk.BooleanVar(value=True)
        b.Checkbutton(workflow_frame, text="Generate CSV File", variable=self.generate_csv, bootstyle="primary").pack(anchor=W, pady=2)
        b.Checkbutton(workflow_frame, text="Generate JSON File", variable=self.generate_json, bootstyle="primary").pack(anchor=W, pady=(2, 15))
        b.Label(workflow_frame, text="4. Generate Final Files").pack(anchor=W)
        self.generate_button = b.Button(workflow_frame, text="Generate Files", command=self.generate_output_files, bootstyle="success", state=DISABLED)
        self.generate_button.pack(fill=X, pady=5)
        stats_frame = b.LabelFrame(self.control_frame, text="Statistics", padding=10)
        stats_frame.pack(fill=X, padx=5, pady=10)
        self.stats_regions_label = b.Label(stats_frame, text="Regions: 0")
        self.stats_regions_label.pack(anchor=W)
        self.stats_sizes_label = b.Label(stats_frame, text="Pack Sizes: 0")
        self.stats_sizes_label.pack(anchor=W)
        self.stats_skus_label = b.Label(stats_frame, text="SKUs Loaded: 0")
        self.stats_skus_label.pack(anchor=W)
        self.stats_avg_price_label = b.Label(stats_frame, text="Avg. Price: $0.00")
        self.stats_avg_price_label.pack(anchor=W)
    def _create_main_widgets(self):
        """Creates the tabbed interface (Price Input, Log, Settings)."""
        self.notebook = b.Notebook(self.main_frame)
        self.notebook.pack(fill=BOTH, expand=True)
        self._create_price_input_tab()
        self._create_log_tab()
        self._create_settings_tab()
        self.progress_bar = b.Progressbar(self.main_frame, orient=HORIZONTAL, mode='determinate', bootstyle="success-striped")
        self.progress_bar.pack(fill=X, padx=5, pady=(5,0))
    def _create_price_input_tab(self):
        """Creates the interactive price table tab with filters and controls."""
        price_tab = b.Frame(self.notebook, padding=5)
        self.notebook.add(price_tab, text='Price Input 💵')
        control_filter_frame = b.Frame(price_tab)
        control_filter_frame.pack(fill=X, pady=5)
        b.Button(control_filter_frame, text="Add Size", command=self._add_pack_size, bootstyle="primary-outline").pack(side=LEFT, padx=(0, 5))
        b.Button(control_filter_frame, text="Remove Size", command=self._remove_pack_size, bootstyle="danger-outline").pack(side=LEFT, padx=(0, 15))
        b.Button(control_filter_frame, text="Reset Prices", command=self._reset_price_data, bootstyle="secondary").pack(side=LEFT, padx=(0, 15))
        page_size_frame = b.Frame(control_filter_frame)
        page_size_frame.pack(side=RIGHT, padx=(15, 0))
        b.Label(page_size_frame, text="Page Size:").pack(side=LEFT, padx=5)
        self.current_pagesize.trace_add('write', self._update_pagesize)
        b.Combobox(page_size_frame, textvariable=self.current_pagesize, values=[10, 25, 50, 100, 200, 500], width=5, state='readonly').pack(side=LEFT)
        slicer_frame = b.Frame(control_filter_frame)
        slicer_frame.pack(side=RIGHT, padx=5)
        b.Label(slicer_frame, text="Filter by State:").pack(side=LEFT, padx=5)
        state_values = self._get_unique_states()
        self.current_state_filter.trace_add('write', self._filter_regions)
        b.Combobox(slicer_frame, textvariable=self.current_state_filter, values=state_values, width=15, state='readonly').pack(side=LEFT)
        coldata = [{"text": "Region", "stretch": False}]
        self.price_tree = Tableview(
            master=price_tab,
            coldata=coldata, 
            rowdata=[], 
            paginated=True, 
            pagesize=self.current_pagesize.get(), 
            autoalign=True, 
            bootstyle="primary"
        )
        self.price_tree.pack(fill=BOTH, expand=True, pady=5)
        self.price_tree.view.bind("<Double-1>", self._on_tree_double_click_or_row)
        self.price_tree.view.bind("<Return>", self._on_tree_double_click_or_row)
    def _create_log_tab(self):
        """Creates the log tab with better visuals."""
        log_tab = b.Frame(self.notebook, padding=5)
        self.notebook.add(log_tab, text='Log 📜')
        log_scroll = ttk.Scrollbar(log_tab)
        log_scroll.pack(side=RIGHT, fill=Y)
        self.log_text = tk.Text(log_tab, wrap='word', relief=FLAT, state=DISABLED, yscrollcommand=log_scroll.set, font=('Consolas', 9))
        self.log_text.pack(fill=BOTH, expand=True)
        log_scroll.config(command=self.log_text.yview)
    def _create_settings_tab(self):
        """Creates the new settings tab for configuration management."""
        settings_tab = b.Frame(self.notebook, padding=10)
        self.notebook.add(settings_tab, text='Settings & Config ⚙️')
        config_frame = b.LabelFrame(settings_tab, text="Configuration & Data Management", padding=10)
        config_frame.pack(fill=X, padx=5, pady=10, anchor=N)
        b.Button(config_frame, text="Save Configuration", command=self._save_configuration, bootstyle="info-outline").pack(fill=X, pady=2)
        b.Button(config_frame, text="Load Configuration", command=self._load_configuration, bootstyle="info-outline").pack(fill=X, pady=2)
        b.Separator(config_frame).pack(fill=X, pady=5)
        b.Button(config_frame, text="Set Output Directory", command=self.set_output_directory, bootstyle="secondary-outline").pack(fill=X, pady=(10, 2))
        b.Button(config_frame, text="Edit Postcode File (JSON)", command=self.edit_postcodes, bootstyle="secondary-outline").pack(fill=X, pady=2)
        b.Button(config_frame, text="Reload Postcode File", command=self.load_postcodes, bootstyle="secondary-outline").pack(fill=X, pady=2)
        theme_frame = b.Frame(config_frame)
        theme_frame.pack(fill=X, pady=(10,0))
        b.Label(theme_frame, text="Theme:").pack(side=LEFT, padx=(0,5))
        self.theme_selector = b.Combobox(theme_frame, values=self.root.style.theme_names(), state='readonly')
        self.theme_selector.pack(side=LEFT, fill=X, expand=True)
        self.theme_selector.set(self.root.style.theme_use())
        self.theme_selector.bind("<<ComboboxSelected>>", self._change_theme)
    def _initialize_price_data(self):
        """Initializes the price DataFrame based on loaded regions."""
        regions = list(self.region_postcodes.keys())
        default_sizes = ["Small", "Medium", "Large", "Bulky", "Extra_Bulky"]
        self.price_df = pd.DataFrame(0.0, index=regions, columns=default_sizes, dtype=float)
        self.price_df.index.name = "Region"
    def _populate_price_table(self):
        """Clears and re-populates the price Tableview, applying the state filter."""
        current_filter = self.current_state_filter.get()
        display_df = self.price_df
        if current_filter != "All States":
            filtered_regions = [
                region for region in self.price_df.index
                if self._get_state_from_region(region) == current_filter
            ]
            display_df = self.price_df.loc[filtered_regions]
        coldata = [{"text": "Region", "stretch": False}] + [{"text": col, "stretch": True} for col in display_df.columns]
        rowdata = []
        for region, row in display_df.iterrows():
            rowdata.append([region] + [f"{val:.2f}" for val in row])
        self.price_tree.build_table_data(coldata=coldata, rowdata=rowdata)
        self._update_stats()
        self.root.update_idletasks()
    def _filter_regions(self, *args):
        """Callback for state filter change."""
        self._populate_price_table()
    def _update_pagesize(self, *args):
        """Callback for page size combobox change."""
        try:
            new_size = self.current_pagesize.get()
            self.price_tree.pagesize = new_size
            self._populate_price_table() 
            self.log(f"Table page size set to {new_size}.")
        except Exception as e:
            self.log(f"ERROR: Could not update page size: {e}")
            logging.error(f"Page size error: {e}")
    def _on_tree_double_click_or_row(self, event):
        """Handle double-click for cell editing or launch multi-edit on Region cell."""
        region_id = self.price_tree.view.identify_row(event.y)
        column_id = self.price_tree.view.identify_column(event.x)
        if not region_id: return 
        row_iid = self.price_tree.view.focus()
        if not row_iid: return
        region_name = self.price_tree.view.item(row_iid, "values")[0]
        is_region_column = (column_id == "#1") or (event.keysym == 'Return' and column_id == "#1")
        is_double_click = event.type == '5' # Double-click event type
        if is_region_column and (is_double_click or event.keysym == 'Return'):
            self._launch_multi_edit_dialog(region_name)
            return
        if not column_id or column_id == "#1": return
        col_index = int(column_id.replace('#', ''))
        col_name = self.price_tree.coldata[col_index]['text'] 
        x, y, width, height = self.price_tree.view.bbox(row_iid, column_id)
        value = self.price_df.at[region_name, col_name]
        entry = b.Entry(self.price_tree.view)
        entry.place(x=x, y=y, width=width, height=height)
        entry.insert(0, f"{value:.2f}")
        entry.focus()
        def save_edit(evt):
            new_value = entry.get()
            try:
                float_value = float(new_value)
                if float_value < 0:
                    raise ValueError("Price must be non-negative.")
                self.price_df.at[region_name, col_name] = float_value
                self.log(f"Updated {region_name} -> {col_name} to ${float_value:.2f}")
                self._populate_price_table()
                self.changes_saved.set(False)
            except ValueError as e:
                self.log(f"ERROR: Invalid input for price: '{new_value}'. {e}")
                messagebox.showerror("Validation Error", f"Price must be a valid non-negative number.", parent=self.root)
            finally:
                entry.destroy()
        entry.bind("<Return>", save_edit)
        entry.bind("<FocusOut>", lambda e: entry.destroy())
    def _launch_multi_edit_dialog(self, region_name):
        """Launches the Toplevel dialog for editing all prices in a region."""
        def update_callback(region):
            self.log(f"Updated all prices for region '{region}' via dialog.")
            self._populate_price_table()
            self.changes_saved.set(False)
        PriceEditDialog(self.root, region_name, self.price_df, update_callback)
    def _add_pack_size(self):
        new_size = simpledialog.askstring("Add Pack Size", "Enter new pack size name:", parent=self.root)
        if new_size and new_size.strip():
            new_size = new_size.strip().replace(' ', '_')
            if new_size in self.price_df.columns:
                messagebox.showwarning("Duplicate", f"Size '{new_size}' already exists.", parent=self.root)
                return
            self.price_df[new_size] = 0.0
            self._populate_price_table()
            self.log(f"Added new size: '{new_size}'")
            self.changes_saved.set(False)
        elif new_size is not None:
            messagebox.showwarning("Invalid Name", "Pack size name cannot be empty.", parent=self.root)
    def _remove_pack_size(self):
        col_to_remove = simpledialog.askstring("Remove Pack Size", "Enter the exact name of the size to remove:", parent=self.root)
        if col_to_remove and col_to_remove.strip():
            col_to_remove = col_to_remove.strip().replace(' ', '_')
            if col_to_remove in self.price_df.columns:
                if messagebox.askyesno("Confirm", f"Are you sure you want to remove the '{col_to_remove}' size column?", parent=self.root):
                    self.price_df.drop(columns=[col_to_remove], inplace=True)
                    self._populate_price_table()
                    self.log(f"Removed size: '{col_to_remove}'")
                    self.changes_saved.set(False)
            else:
                messagebox.showerror("Not Found", f"Size '{col_to_remove}' not found.", parent=self.root)
    def _reset_price_data(self):
        """Resets the price DataFrame to its initial state."""
        if messagebox.askyesno("Confirm Reset", "Are you sure you want to reset ALL price data to $0.00? This cannot be undone.", parent=self.root, icon='warning'):
            self._initialize_price_data()
            self._populate_price_table()
            self.log("⚠️ Price data reset to default (all $0.00).")
            self.changes_saved.set(False)
    def _confirm_and_save_prices(self):
        """Marks prices as saved and enables the Generate button."""
        if self.price_df is None or self.price_df.empty:
            messagebox.showwarning("Empty Data", "The price matrix is empty. Please load postcodes and add pack sizes.", parent=self.root)
            self.log("ERROR: Cannot save. Price matrix is empty.")
            return
        self.changes_saved.set(True)
        self.log("✅ Price Matrix Confirmed and Saved for Generation.")
        self._check_save_state()
    def _check_save_state(self):
        """Updates the state of the save and generate buttons."""
        if self.changes_saved.get():
            self.save_button.config(state=DISABLED, bootstyle="success")
            self.generate_button.config(state=NORMAL, bootstyle="success")
        else:
            self.save_button.config(state=NORMAL, bootstyle="warning")
            self.generate_button.config(state=DISABLED)
        if not hasattr(self, '_save_trace_id'):
            self._save_trace_id = self.changes_saved.trace_add('write', lambda *args: self._check_save_state())
    def upload_sku_file(self):
        file_path = filedialog.askopenfilename(title="Select Excel file with 'SKU Input' sheet", filetypes=[("Excel files", "*.xlsx *.xls")])
        if not file_path: 
            return
        if self.price_df is None or self.price_df.empty:
            self.log("ERROR: Load postcode data first to define regions/pack sizes.")
            messagebox.showerror("Pre-requisite Error", "Please load the postcode file first to define regions.", parent=self.root)
            return
        try:
            df_sku = pd.read_excel(file_path, 'SKU Input').fillna('')
            sku_columns_map = {col.lower().replace('_', '').replace(' ', ''): col for col in df_sku.columns}
            self.processed_sku_data = {}
            for price_col in self.price_df.columns:
                normalized_price_col = price_col.lower().replace('_', '').replace(' ', '')
                if normalized_price_col in sku_columns_map:
                    original_sku_col = sku_columns_map[normalized_price_col]
                    skus = [str(sku) for sku in df_sku[original_sku_col] if sku not in ('', None)]
                    if skus:
                        self.processed_sku_data[price_col.lower()] = skus
            if not self.processed_sku_data:
                 self.log("WARNING: No SKUs found that match current pack-size columns.")
                 messagebox.showwarning("Data Mismatch", "No SKU columns matched the price matrix pack sizes. Check column names.", parent=self.root)
            self.log(f"✅ SKU data loaded successfully. Found {len(self.processed_sku_data)} matching columns.")
            self._update_stats()
        except FileNotFoundError:
            self.log("ERROR: SKU file not found or sheet name incorrect ('SKU Input' expected).")
            messagebox.showerror("File Error", "SKU file not found or sheet 'SKU Input' is missing.", parent=self.root)
        except Exception as e:
            self.log(f"ERROR reading SKU file: {e}")
            logging.error(f"SKU upload error: {e}", exc_info=True)
            self.processed_sku_data = None
            messagebox.showerror("Processing Error", f"An error occurred while reading the SKU file: {e}", parent=self.root)
    def generate_output_files(self):
        """Generates the final CSV and JSON output files."""
        if not self.changes_saved.get():
            self.log("ERROR: Price Matrix changes not confirmed and saved."); return
        if not self.processed_sku_data:
            self.log("ERROR: No SKU data loaded."); return
        if not self.generate_csv.get() and not self.generate_json.get():
            self.log("ERROR: Please select at least one output format."); return
        self.log("--- Starting file generation... ---")
        self.progress_bar.start(10) # Start indeterminate progress
        try:
            prices = self.price_df.T.to_dict()
            for region, data in prices.items(): 
                prices[region] = {k.lower(): v for k,v in data.items()}
            output_table = []
            for i, (region, postcodes) in enumerate(self.region_postcodes.items()):
                self.progress_bar['value'] = (i / len(self.region_postcodes)) * 100
                self.root.update()
                region_prices = prices.get(region)
                if not region_prices: continue
                for pack_size_key, price in region_prices.items():
                    skus = self.processed_sku_data.get(pack_size_key, [])
                    if not skus: continue
                    price_str = f"{price:.2f}"
                    for sku in skus:
                        for postcode in postcodes:
                            output_table.append({"postCode": postcode, "productCode": sku, "price": price_str})
            self.progress_bar.stop()
            self.progress_bar['value'] = 0
            if not output_table: 
                self.log("Warning: No data to output. Check SKU/price/postcode inputs."); return
            self.output_path.mkdir(parents=True, exist_ok=True)
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            df_output = pd.DataFrame(output_table)
            if self.generate_csv.get():
                csv_file_path = self.output_path / f"output_{timestamp}.csv"
                df_output.to_csv(csv_file_path, index=False)
                self.log(f"✅ CSV file saved: {csv_file_path}")
            if self.generate_json.get():
                json_file_path = self.output_path / f"output_{timestamp}.json"
                json_output = [
                    {
                        "postCode": r['postCode'], 
                        "productCode": r['productCode'], 
                        "price": r['price'], 
                        "id": f"{r['productCode']}{r['postCode']}", 
                        "message": ""
                    } 
                    for _, r in df_output.iterrows()
                ]
                with open(json_file_path, 'w') as f: 
                    json.dump(json_output, f, indent=4)
                self.log(f"✅ JSON file saved: {json_file_path}")
            self.log(f"--- Generation Complete! Total records: {len(output_table)} ---")
        except Exception as e:
            self.progress_bar.stop()
            self.progress_bar['value'] = 0
            self.log(f"FATAL ERROR during generation: {e}")
            logging.error(f"Generation error: {e}", exc_info=True)
            messagebox.showerror("Generation Error", f"A fatal error occurred during file generation: {e}", parent=self.root)
    def log(self, message):
        """Thread-safe logging to the UI and console."""
        self.log_text.config(state=NORMAL)
        self.log_text.insert(END, f"[{datetime.now().strftime('%H:%M:%S')}] {message}\n")
        self.log_text.see(END); 
        self.log_text.config(state=DISABLED)
        self.root.update_idletasks() 
    def load_postcodes(self):
        """Loads region-to-postcode mapping from JSON file, creating defaults if necessary."""
        self.base_path.mkdir(parents=True, exist_ok=True)
        if not self.postcodes_file.exists():
            default_postcodes = {
                region: [] for region in ["Sydney Metro", "Melbourne Metro", "Brisbane Metro", "Adelaide Metro", 
                                        "Perth Metro", "NSW Country", "VIC Country", "QLD Country", "SA Country", 
                                        "WA Country", "TAS All", "NT All", "ACT All"]
            }
            try:
                with open(self.postcodes_file, 'w') as f: json.dump(default_postcodes, f, indent=4)
                self.log("Created default postcode file. Please edit it with your data.")
            except IOError as e:
                self.log(f"ERROR: Could not create default postcode file: {e}")
                self.region_postcodes = {}
                return
        try:
            with open(self.postcodes_file, 'r') as f: 
                self.region_postcodes = json.load(f)
            self._initialize_price_data()
            self._populate_price_table()
            state_values = self._get_unique_states()
            slicer = self.price_tree.master.winfo_children()[0].winfo_children()[-1].winfo_children()[-1]
            slicer['values'] = state_values
            self.log(f"✅ Postcode mapping loaded. Found {len(self.region_postcodes)} regions.")
            self.changes_saved.set(False)
        except (json.JSONDecodeError, IOError) as e:
            self.log(f"FATAL ERROR loading postcodes: {e}"); 
            logging.error(f"Postcode file load error: {e}")
            messagebox.showerror("Data Error", f"Failed to load or parse postcode file: {e}", parent=self.root)
            self.region_postcodes = {}
    def edit_postcodes(self):
        """Opens the postcode JSON file in the default editor."""
        self.log(f"Opening postcode file: {self.postcodes_file}. Reload after saving changes.")
        try:
            if os.name == 'nt': os.startfile(self.postcodes_file)
            elif sys.platform == 'darwin': os.system(f'open "{self.postcodes_file}"')
            else: os.system(f'xdg-open "{self.postcodes_file}"')
        except Exception as e:
            self.log(f"ERROR opening file: {e}")
    def set_output_directory(self):
        """Allows the user to select a custom output directory."""
        path = filedialog.askdirectory(initialdir=self.output_path, parent=self.root)
        if path: 
            self.output_path = Path(path)
            self.log(f"Output directory set to: {self.output_path}")
    def _get_state_from_region(self, region_name):
        """Infers state (simplified for Australian regions)."""
        name = region_name.lower()
        if 'nsw' in name or 'sydney' in name or 'newcastle' in name or 'wollongong' in name: return 'NSW'
        if 'vic' in name or 'melbourne' in name or 'geelong' in name: return 'VIC'
        if 'qld' in name or 'brisbane' in name or 'gold coast' in name: return 'QLD'
        if 'sa' in name or 'adelaide' in name: return 'SA'
        if 'wa' in name or 'perth' in name: return 'WA'
        if 'tas' in name: return 'TAS'
        if 'nt' in name: return 'NT'
        if 'act' in name or 'canberra' in name: return 'ACT'
        return 'Other'
    def _get_unique_states(self):
        """Generates a list of unique states for the filter combobox."""
        regions = self.region_postcodes.keys()
        states = set(self._get_state_from_region(r) for r in regions)
        return ["All States"] + sorted(list(states))
    def _update_stats(self):
        """Recalculates and updates the statistics labels."""
        num_regions = len(self.price_df.index) if self.price_df is not None else 0
        num_sizes = len(self.price_df.columns) if self.price_df is not None else 0
        num_skus = sum(len(v) for v in self.processed_sku_data.values()) if self.processed_sku_data else 0
        positive_prices = self.price_df[self.price_df > 0] if self.price_df is not None else pd.DataFrame()
        avg_price = positive_prices.stack().mean() if not positive_prices.empty else 0.0
        self.stats_regions_label.config(text=f"Regions: {num_regions}")
        self.stats_sizes_label.config(text=f"Pack Sizes: {num_sizes}")
        self.stats_skus_label.config(text=f"SKUs Loaded: {num_skus}")
        self.stats_avg_price_label.config(text=f"Avg. Price: ${avg_price:.2f}")
    def _change_theme(self, event):
        """Applies the selected ttkbootstrap theme."""
        theme = self.theme_selector.get()
        self.root.style.theme_use(theme)
        self.log(f"Theme changed to '{theme}'")
    def _save_configuration(self):
        """Saves current theme, output path, and price matrix to a JSON file."""
        file_path = filedialog.asksaveasfilename(defaultextension=".json", filetypes=[("JSON Config files", "*.json")], parent=self.root, title="Save Application Configuration")
        if not file_path: return
        config_data = {
            "theme": self.root.style.theme_names(),
            "output_path": str(self.output_path),
            "price_data": self.price_df.to_dict('split') if self.price_df is not None else None
        }
        try:
            with open(file_path, 'w') as f:
                json.dump(config_data, f, indent=4)
            self.log(f"✅ Configuration saved to {Path(file_path).name}")
            self.changes_saved.set(True)
        except Exception as e:
            self.log(f"ERROR saving configuration: {e}")
            logging.error(f"Config save error: {e}")
    def _load_configuration(self):
        """Loads theme, output path, and price matrix from a JSON file."""
        file_path = filedialog.askopenfilename(filetypes=[("JSON Config files", "*.json")], parent=self.root, title="Load Application Configuration")
        if not file_path: return
        try:
            with open(file_path, 'r') as f:
                config_data = json.load(f)
            theme_name = config_data.get("theme")
            if theme_name and theme_name in self.root.style.theme_names():
                self.theme_selector.set(theme_name)
                self._change_theme(None)
            self.output_path = Path(config_data.get("output_path", self.base_path / "Output"))
            price_data = config_data.get('price_data')
            if price_data:
                self.price_df = pd.DataFrame(
                    price_data['data'],
                    index=price_data['index'],
                    columns=price_data['columns'],
                    dtype=float
                )
                self.price_df.index.name = "Region"
                self._populate_price_table()
                self._update_stats()
            self.log(f"✅ Configuration loaded from {Path(file_path).name}")
            self.changes_saved.set(True)
        except Exception as e:
            self.log(f"ERROR loading configuration: {e}")
            logging.error(f"Config load error: {e}")
            messagebox.showerror("Load Error", f"Failed to load configuration file: {e}", parent=self.root)
    def _on_closing(self):
        """Check for unsaved changes before exiting."""
        if not self.changes_saved.get():
            if not messagebox.askyesno("Exit Confirmation", "Unsaved price changes detected. Exit anyway?", icon='warning', parent=self.root):
                return
        self.root.destroy()
if __name__ == "__main__":
    try:
        root = b.Window(themename="litera")
        app = FreightApp(root)
        root.mainloop()
    except Exception as e:
        logging.critical(f"Unhandled fatal error: {e}", exc_info=True)
        try:
            messagebox.showerror("Fatal Error", f"The application encountered a fatal error and must close. See logs for details.\nError: {e}")
        except:
            print(f"FATAL ERROR: {e}")
